using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Contracts.OneC;
using ErpOnecAgent.Domain.Agent;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Domain.Common;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using ErpOnecAgent.Service.Diagnostics;
using ErpOnecAgent.Service.Runtime;
using ErpOnecAgent.Service.Workers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// A07b B2/B7 runtime reproductions of the missing resolution/admission split (baseline RED):
/// (1) a durable already-sent business command must stay status-resolvable while
///     <c>CanExecuteCommands</c> is false — today the single loop gate skips resolution entirely;
/// (2) a mode restriction taking effect between the ready fetch and the pass claim must deny the
///     fresh POST — today the gate is evaluated once per loop iteration and the claimed pass POSTs
///     unconditionally. Both tests must FAIL on the unchanged baseline and are expected to pass only
///     after the bounded split slice lands.
/// </summary>
public sealed class A07CommandBoundaryRedTests : IAsyncLifetime
{
    private static readonly TimeSpan BoundedWait = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan StopTimeout = TimeSpan.FromSeconds(5);
    private readonly SqliteTestDatabase _database = new();
    private SqliteConnectionFactory _factory = null!;
    private SqliteAgentStore _store = null!;

    public async Task InitializeAsync()
    {
        _factory = _database.CreateFactory(Path.Combine("data", "a07-boundaries.db"));
        _store = new(_factory, new SqliteMigrator(_factory));
        await _store.InitializeAsync(CancellationToken.None);
    }

    public async Task DisposeAsync() => await _database.DisposeAsync();

    [Fact]
    public async Task PauseCommands_still_resolves_already_sent_business_command_via_lookup()
    {
        // B2 reproduction: ready + PauseCommands. A business command with durable POST evidence in
        // unknown_result is due; the resolve path needs only GetStatusAsync and must still run once
        // ready. Baseline: the single CanExecuteCommands gate skips the whole loop, so no lookup ever
        // happens (RED). No POST may occur in either case.
        var state = ReadyState(AgentMode.PauseCommands);
        var onec = new GatedOnec { StatusKind = OnecExecutionKind.Succeeded };
        var command = MakeBusinessCommand();
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        var postAttempt = await _store.ClaimPostAttemptAsync(command.CommandId, CancellationToken.None);
        Assert.NotNull(postAttempt);
        Assert.True(await _store.MarkUnknownResultAsync(command.CommandId, postAttempt.AttemptId, "UNKNOWN_RESULT", "1C execution result is unknown.", DateTimeOffset.UtcNow.AddSeconds(-5), CancellationToken.None));

        var worker = CreateWorker(_store, state, onec);
        try
        {
            await worker.StartAsync(CancellationToken.None);
            var lookupObserved = await Task.WhenAny(onec.LookupObserved.Task, Task.Delay(BoundedWait)) == onec.LookupObserved.Task;

            Assert.True(lookupObserved, "Sent-evidence business command must be status-looked-up while CanExecuteCommands is false (lookup is allowed once ready; only POST admission is denied).");
            Assert.Equal(0, onec.ExecuteCalls);
            Assert.Equal(1, onec.StatusCalls);
        }
        finally
        {
            using var stop = new CancellationTokenSource(StopTimeout);
            await worker.StopAsync(stop.Token);
        }
    }

    [Fact]
    public async Task Mode_restriction_between_fetch_and_claim_suppresses_fresh_post()
    {
        // B7 reproduction: the worker fetches a fresh due row under Normal, then remote mode flips to
        // PauseCommands while the pass is parked at its execution claim. Baseline evaluates
        // CanExecuteCommands once per loop iteration, so the claimed pass still issues the POST
        // (RED). Required: the in-pass admission decision is re-evaluated before the POST claim path
        // and the fresh POST is suppressed.
        var state = ReadyState(AgentMode.Normal);
        var onec = new GatedOnec();
        var gate = new ClaimGate();
        var store = ClaimGatedStoreProxy.Create(_store, gate);
        var command = MakeBusinessCommand();
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);

        var worker = CreateWorker(store, state, onec);
        try
        {
            await worker.StartAsync(CancellationToken.None);
            var claimEntered = await Task.WhenAny(gate.ClaimEntered.Task, Task.Delay(BoundedWait)) == gate.ClaimEntered.Task;
            Assert.True(claimEntered, "Worker pass did not reach the execution claim under Normal mode; fixture broken.");

            state.SetRemoteMode(AgentMode.PauseCommands);
            gate.ReleaseClaim.TrySetResult(true);

            // Deterministic pass barrier: PassCompleted fires only after the pass's real
            // ReleaseCommandExecutionClaimAsync has completed, so the zero-POST assertion cannot
            // false-pass on a stalled pass.
            var passDone = await Task.WhenAny(gate.PassCompleted.Task, Task.Delay(BoundedWait)) == gate.PassCompleted.Task;
            Assert.True(passDone, "Worker pass did not complete within the bounded window.");
            Assert.False(onec.PostObserved.Task.IsCompleted, "Fresh POST was issued after CanExecuteCommands became false; the admission decision must be re-evaluated inside the pass before the POST claim path.");
            Assert.Equal(0, onec.ExecuteCalls);
        }
        finally
        {
            gate.ReleaseClaim.TrySetResult(true);
            using var stop = new CancellationTokenSource(StopTimeout);
            await worker.StopAsync(stop.Token);
        }
    }

    private static AgentRuntimeState ReadyState(AgentMode mode)
    {
        var state = new AgentRuntimeState();
        state.SetRemoteMode(mode);
        state.CompleteBootstrap();
        return state;
    }

    private static CommandExecutionWorker CreateWorker(IAgentStore store, AgentRuntimeState state, GatedOnec onec)
    {
        var agent = Options.Create(new AgentOptions { AgentId = $"a07-boundaries-{Guid.NewGuid():N}", SiteId = "a07-site", DataDirectory = Path.GetTempPath() });
        var diagnostics = new DiagnosticsCollector(
            store,
            agent,
            Options.Create(new ErpOptions { RequireClientCertificate = false }),
            Options.Create(new OnecOptions()));
        return new CommandExecutionWorker(
            store,
            onec,
            new FakeOnecHealthClient(),
            new DynamicConfigurationState(Options.Create(new CommandOptions()), Options.Create(new EtlOptions())),
            state,
            new LocalEtlPauseController(store, state),
            diagnostics,
            Options.Create(new CommandOptions { MaxConcurrency = 4, MaxOperationalAttempts = 12, SupportedTypes = ["synthetic"] }),
            NullLogger<CommandExecutionWorker>.Instance);
    }

    private static CommandEnvelope MakeBusinessCommand()
    {
        using var document = JsonDocument.Parse("{\"amount\":10}");
        var payload = document.RootElement.Clone();
        return new(Guid.NewGuid(), "create_customer_order", 1, 100, $"order:{Guid.NewGuid():N}", null, DateTimeOffset.UtcNow, null, null, null, PayloadHasher.Compute(payload), payload);
    }

    private sealed class GatedOnec : IOnecCommandClient
    {
        private int _executeCalls;
        private int _statusCalls;

        public OnecExecutionKind StatusKind { get; init; } = OnecExecutionKind.Succeeded;
        public TaskCompletionSource<bool> LookupObserved { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<bool> PostObserved { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int ExecuteCalls => Volatile.Read(ref _executeCalls);
        public int StatusCalls => Volatile.Read(ref _statusCalls);

        public Task<OnecExecutionResult> ExecuteAsync(CommandEnvelope command, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _executeCalls);
            PostObserved.TrySetResult(true);
            return Task.FromResult(new OnecExecutionResult(OnecExecutionKind.Succeeded, new(command.CommandId, "succeeded", JsonSerializer.SerializeToElement(new { @ref = "synthetic-ref" }), null, [], 1), 200, null, null));
        }

        public Task<OnecExecutionResult> GetStatusAsync(Guid commandId, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _statusCalls);
            LookupObserved.TrySetResult(true);
            var result = StatusKind is OnecExecutionKind.Succeeded
                ? new OnecExecutionResult(OnecExecutionKind.Succeeded, new(commandId, "succeeded", JsonSerializer.SerializeToElement(new { @ref = "synthetic-ref" }), null, [], 1), 200, null, null)
                : new OnecExecutionResult(StatusKind, null, 202, null, null);
            return Task.FromResult(result);
        }
    }

    private sealed class FakeOnecHealthClient : IOnecHealthClient
    {
        public Task<OnecHealthResponse?> CheckAsync(CancellationToken cancellationToken) => Task.FromResult<OnecHealthResponse?>(null);
    }

    public sealed class ClaimGate
    {
        public TaskCompletionSource<bool> ClaimEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<bool> ReleaseClaim { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<bool> PassCompleted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    /// <summary>
    /// IAgentStore pass-through that parks the FIRST TryAcquireCommandExecutionClaimAsync call on a
    /// test-controlled gate, so a mode change can be injected deterministically between the ready
    /// fetch and the claim/POST path without sleeps. All other members delegate unchanged.
    /// </summary>
    public class ClaimGatedStoreProxy : DispatchProxy
    {
        private sealed record ProxyTarget(IAgentStore Inner, ClaimGate Gate);

        private static readonly ConditionalWeakTable<DispatchProxy, ProxyTarget> Targets = new();
        private int _gatedOnce;

        public static IAgentStore Create(IAgentStore inner, ClaimGate gate)
        {
            var proxy = DispatchProxy.Create<IAgentStore, ClaimGatedStoreProxy>();
            Targets.Add((DispatchProxy)(object)proxy, new(inner, gate));
            return proxy;
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (!Targets.TryGetValue(this, out var target)) throw new InvalidOperationException("Proxy state is missing.");
            if (targetMethod?.Name == nameof(IAgentStore.TryAcquireCommandExecutionClaimAsync)
                && Interlocked.CompareExchange(ref _gatedOnce, 1, 0) == 0)
            {
                return GatedClaimAsync(target.Inner, target.Gate, args);
            }
            if (targetMethod?.Name == nameof(IAgentStore.ReleaseCommandExecutionClaimAsync))
            {
                return ReleaseAndSignalAsync(target, args);
            }
            return targetMethod!.Invoke(target.Inner, args);
        }

        private static async Task ReleaseAndSignalAsync(ProxyTarget target, object?[]? args)
        {
            await target.Inner.ReleaseCommandExecutionClaimAsync((Guid)args![0]!, (string)args[1]!, (CancellationToken)args[2]!).ConfigureAwait(false);
            target.Gate.PassCompleted.TrySetResult(true);
        }

        private static async Task<ExecutionClaim?> GatedClaimAsync(IAgentStore inner, ClaimGate gate, object?[]? args)
        {
            gate.ClaimEntered.TrySetResult(true);
            await gate.ReleaseClaim.Task.ConfigureAwait(false);
            return await inner.TryAcquireCommandExecutionClaimAsync(
                (Guid)args![0]!, (string)args[1]!, (DateTimeOffset)args[2]!, (DateTimeOffset)args[3]!, (CancellationToken)args[4]!).ConfigureAwait(false);
        }
    }
}
