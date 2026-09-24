using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Commands;
using ErpOnecAgent.Contracts.Erp;
using ErpOnecAgent.Contracts.OneC;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Domain.Common;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

public sealed class CommandIntakeTests : IAsyncLifetime
{
    private readonly SqliteTestDatabase _database = new();
    private SqliteAgentStore _store = null!;

    public async Task InitializeAsync()
    {
        var factory = _database.CreateFactory(Path.Combine("data", "agent.db"));
        _store = new(factory, new SqliteMigrator(factory));
        await _store.InitializeAsync(CancellationToken.None);
    }

    public async Task DisposeAsync()
    {
        await _database.DisposeAsync();
    }

    [Fact]
    public async Task Failing_ACK_after_invalid_hash_admission_leaves_no_executable_row_and_zero_onec_calls()
    {
        var onec = new CountingOnecClient();
        var erp = new FakeErpClient { OnAcknowledgeReceived = () => throw new HttpRequestException("ACK failed") };
        var intake = new CommandIntakeService(erp, _store);
        var leaseCommand = MakeLeaseCommand(payloadJson: "{\"amount\":10}", payloadHash: "wrong-hash");

        var result = await intake.IntakeAsync(Guid.NewGuid(), leaseCommand, ["create_customer_order"], 1024, DateTimeOffset.UtcNow, CancellationToken.None);

        Assert.Equal(StoreCommandOutcome.Rejected, result.Outcome);
        Assert.False(result.Validation.IsValid);
        Assert.False(result.Acknowledged);
        Assert.NotNull(result.AckError);
        await AssertNoExecutableInvalidCommandAsync(onec);
    }

    [Fact]
    public async Task Delayed_ACK_does_not_expose_executable_invalid_row_before_ack_completes()
    {
        var onec = new CountingOnecClient();
        var ackEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseAck = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var erp = new FakeErpClient
        {
            OnAcknowledgeReceived = async () =>
            {
                ackEntered.SetResult();
                await releaseAck.Task;
            }
        };
        var intake = new CommandIntakeService(erp, _store);
        var leaseCommand = MakeLeaseCommand(payloadJson: "{\"amount\":10}", payloadHash: "wrong-hash");

        var intakeTask = intake.IntakeAsync(Guid.NewGuid(), leaseCommand, ["create_customer_order"], 1024, DateTimeOffset.UtcNow, CancellationToken.None);
        await ackEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await AssertNoExecutableInvalidCommandAsync(onec);

        releaseAck.SetResult();
        var result = await intakeTask;
        Assert.Equal(StoreCommandOutcome.Rejected, result.Outcome);
        Assert.True(result.Acknowledged);
        await AssertNoExecutableInvalidCommandAsync(onec);
    }

    [Fact]
    public async Task Failing_ACK_after_unknown_payload_version_admission_leaves_no_executable_row()
    {
        var onec = new CountingOnecClient();
        var erp = new FakeErpClient { OnAcknowledgeReceived = () => throw new HttpRequestException("ACK failed") };
        var intake = new CommandIntakeService(erp, _store);
        var leaseCommand = MakeLeaseCommand(payloadJson: "{\"amount\":10}", payloadHash: null, payloadVersion: 2, computeHash: true);

        var result = await intake.IntakeAsync(Guid.NewGuid(), leaseCommand, ["create_customer_order"], 1024, DateTimeOffset.UtcNow, CancellationToken.None);

        Assert.Equal(StoreCommandOutcome.Rejected, result.Outcome);
        Assert.Equal("UNSUPPORTED_PAYLOAD_VERSION", result.Validation.ErrorCode);
        await AssertNoExecutableInvalidCommandAsync(onec);
    }

    [Fact]
    public async Task Failing_ACK_after_null_payload_admission_leaves_no_executable_row()
    {
        var onec = new CountingOnecClient();
        var erp = new FakeErpClient { OnAcknowledgeReceived = () => throw new HttpRequestException("ACK failed") };
        var intake = new CommandIntakeService(erp, _store);
        var leaseCommand = MakeLeaseCommand(payloadJson: "null", payloadHash: "any", payloadVersion: 1);

        var result = await intake.IntakeAsync(Guid.NewGuid(), leaseCommand, ["create_customer_order"], 1024, DateTimeOffset.UtcNow, CancellationToken.None);

        Assert.Equal(StoreCommandOutcome.Rejected, result.Outcome);
        Assert.Equal("INVALID_PAYLOAD", result.Validation.ErrorCode);
        await AssertNoExecutableInvalidCommandAsync(onec);
    }

    [Fact]
    public async Task Valid_command_admission_still_queues_before_ack()
    {
        var onec = new CountingOnecClient();
        var erp = new FakeErpClient();
        var intake = new CommandIntakeService(erp, _store);
        var leaseCommand = MakeLeaseCommand(payloadJson: "{\"amount\":10}", payloadHash: null, computeHash: true);

        var result = await intake.IntakeAsync(Guid.NewGuid(), leaseCommand, ["create_customer_order"], 1024, DateTimeOffset.UtcNow, CancellationToken.None);

        Assert.Equal(StoreCommandOutcome.Stored, result.Outcome);
        Assert.True(result.Acknowledged);
        Assert.Null(result.AckError);
        Assert.Single(erp.Acks);
        var ready = await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddMinutes(1), CancellationToken.None);
        Assert.Single(ready);
        Assert.Equal(CommandStatus.Queued, ready[0].Status);
        Assert.Equal(0, onec.ExecuteCalls);
    }

    private async Task AssertNoExecutableInvalidCommandAsync(CountingOnecClient onec)
    {
        var ready = await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddMinutes(1), CancellationToken.None);
        Assert.Empty(ready);
        foreach (var stored in ready) await onec.ExecuteAsync(stored.Envelope, CancellationToken.None);
        Assert.Equal(0, onec.ExecuteCalls);
        Assert.Single(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
    }

    private static JsonElement MakeLeaseCommand(string payloadJson, string? payloadHash, int payloadVersion = 1, bool computeHash = false)
    {
        using var document = JsonDocument.Parse(payloadJson);
        var payload = document.RootElement.Clone();
        var hash = computeHash ? PayloadHasher.Compute(payload) : payloadHash;
        var json = JsonSerializer.Serialize(new
        {
            commandId = Guid.NewGuid(),
            commandType = "create_customer_order",
            payloadVersion,
            priority = 100,
            orderingKey = "order:42",
            correlationId = (Guid?)null,
            createdAtUtc = DateTimeOffset.UtcNow,
            notBeforeUtc = (DateTimeOffset?)null,
            expiresAtUtc = DateTimeOffset.UtcNow.AddHours(1),
            requestedBy = (object?)null,
            payloadHash = hash,
            payload
        });
        return JsonDocument.Parse(json).RootElement.Clone();
    }

    private sealed class CountingOnecClient : IOnecCommandClient
    {
        public int ExecuteCalls { get; private set; }

        public Task<OnecExecutionResult> ExecuteAsync(CommandEnvelope command, CancellationToken cancellationToken)
        {
            ExecuteCalls++;
            return Task.FromResult(new OnecExecutionResult(OnecExecutionKind.Succeeded, null, 200, null, null));
        }

        public Task<OnecExecutionResult> GetStatusAsync(Guid commandId, CancellationToken cancellationToken) =>
            Task.FromResult(new OnecExecutionResult(OnecExecutionKind.NotFound, null, 404, null, null));
    }

    private sealed class FakeErpClient : IErpClient
    {
        public Func<Task>? OnAcknowledgeReceived { get; set; }
        public List<(Guid CommandId, CommandReceivedRequest Request)> Acks { get; } = [];

        public Task AcknowledgeReceivedAsync(Guid commandId, CommandReceivedRequest request, CancellationToken cancellationToken)
        {
            Acks.Add((commandId, request));
            return OnAcknowledgeReceived?.Invoke() ?? Task.CompletedTask;
        }

        public Task<LeaseResponse> LeaseCommandAsync(LeaseRequest request, CancellationToken cancellationToken) =>
            Task.FromResult(new LeaseResponse(false, null, null, null));

        public Task<SessionStartResponse> StartSessionAsync(SessionStartRequest request, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task AcknowledgeResultAsync(Guid commandId, string resultJson, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<BatchAcknowledgement> UploadBatchAsync(EtlBatch batch, Stream content, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task CompleteEtlRunAsync(Guid runId, object summary, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task SendHeartbeatAsync(HeartbeatRequest request, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<RemoteConfigurationResponse?> GetConfigurationAsync(long currentVersion, CancellationToken cancellationToken) =>
            throw new NotSupportedException();
    }
}
