using System.Text.Json;
using ErpOnecAgent.Application.Commands;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Domain.Common;
using Xunit;

namespace ErpOnecAgent.UnitTests;

public sealed class CommandTests
{
    [Fact]
    public void Canonical_hash_is_independent_of_object_property_order()
    {
        using var first = JsonDocument.Parse("{\"b\":2,\"a\":1}");
        using var second = JsonDocument.Parse("{\"a\":1,\"b\":2}");
        Assert.Equal(PayloadHasher.Compute(first.RootElement), PayloadHasher.Compute(second.RootElement));
    }

    [Fact]
    public void State_machine_rejects_reexecution_after_local_success()
    {
        Assert.False(CommandStateMachine.CanTransition(CommandStatus.SucceededLocal, CommandStatus.Executing));
        Assert.Throws<InvalidOperationException>(() => CommandStateMachine.EnsureTransition(CommandStatus.Completed, CommandStatus.Queued));
    }

    [Fact]
    public void Validator_fails_closed_on_hash_mismatch()
    {
        using var payload = JsonDocument.Parse("{\"orderId\":42}");
        var command = new CommandEnvelope(Guid.NewGuid(), "create_customer_order", 1, 100, "order:42", null, DateTimeOffset.UtcNow, null, null, null, "wrong", payload.RootElement.Clone());
        var result = CommandValidator.Validate(command, ["create_customer_order"], 1024);
        Assert.False(result.IsValid); Assert.Equal("PAYLOAD_HASH_MISMATCH", result.ErrorCode);
    }

    [Fact]
    public void Retry_delay_is_bounded_and_nonzero()
    {
        var delay = CommandPolicy.RetryDelay(10_000, new Random(1));
        Assert.InRange(delay, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5) + TimeSpan.FromSeconds(1));
    }

    [Fact]
    public void Validator_rejects_unknown_positive_payload_version()
    {
        using var payload = JsonDocument.Parse("{\"orderId\":42}");
        var command = new CommandEnvelope(Guid.NewGuid(), "create_customer_order", 2, 100, "order:42", null, DateTimeOffset.UtcNow, null, null, null, PayloadHasher.Compute(payload.RootElement), payload.RootElement.Clone());
        var result = CommandValidator.Validate(command, ["create_customer_order"], 1024);
        Assert.False(result.IsValid);
        Assert.Equal("UNSUPPORTED_PAYLOAD_VERSION", result.ErrorCode);
    }

    [Fact]
    public void Validator_accepts_supported_payload_version_v1()
    {
        using var payload = JsonDocument.Parse("{\"orderId\":42}");
        var hash = PayloadHasher.Compute(payload.RootElement);
        var command = new CommandEnvelope(Guid.NewGuid(), "create_customer_order", 1, 100, "order:42", null, DateTimeOffset.UtcNow, null, null, null, hash, payload.RootElement.Clone());
        var result = CommandValidator.Validate(command, ["create_customer_order"], 1024);
        Assert.True(result.IsValid);
    }

    [Fact]
    public void Validator_rejects_missing_payload_without_throwing()
    {
        var command = new CommandEnvelope(Guid.NewGuid(), "create_customer_order", 1, 100, null, null, DateTimeOffset.UtcNow, null, null, null, "any-hash", default);
        var result = CommandValidator.Validate(command, ["create_customer_order"], 1024);
        Assert.False(result.IsValid);
        Assert.Equal("INVALID_PAYLOAD", result.ErrorCode);
    }

    [Fact]
    public void Validator_rejects_null_payload_without_throwing()
    {
        using var payload = JsonDocument.Parse("null");
        var command = new CommandEnvelope(Guid.NewGuid(), "create_customer_order", 1, 100, null, null, DateTimeOffset.UtcNow, null, null, null, PayloadHasher.Compute(payload.RootElement), payload.RootElement.Clone());
        var result = CommandValidator.Validate(command, ["create_customer_order"], 1024);
        Assert.False(result.IsValid);
        Assert.Equal("INVALID_PAYLOAD", result.ErrorCode);
    }

    [Fact]
    public void Validator_rejects_null_hash_without_throwing()
    {
        using var payload = JsonDocument.Parse("{\"orderId\":42}");
        var command = new CommandEnvelope(Guid.NewGuid(), "create_customer_order", 1, 100, null, null, DateTimeOffset.UtcNow, null, null, null, null!, payload.RootElement.Clone());
        var result = CommandValidator.Validate(command, ["create_customer_order"], 1024);
        Assert.False(result.IsValid);
        Assert.Equal("PAYLOAD_HASH_MISMATCH", result.ErrorCode);
    }

    [Fact]
    public void Validator_rejects_empty_command_id_without_throwing()
    {
        using var payload = JsonDocument.Parse("{}");
        var command = new CommandEnvelope(Guid.Empty, "create_customer_order", 1, 100, null, null, DateTimeOffset.UtcNow, null, null, null, PayloadHasher.Compute(payload.RootElement), payload.RootElement.Clone());
        var result = CommandValidator.Validate(command, ["create_customer_order"], 1024);
        Assert.False(result.IsValid);
        Assert.Equal("INVALID_COMMAND_ID", result.ErrorCode);
    }
}
