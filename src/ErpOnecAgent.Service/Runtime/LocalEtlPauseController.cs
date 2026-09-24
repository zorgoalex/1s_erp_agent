using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;

namespace ErpOnecAgent.Service.Runtime;

public sealed class LocalEtlPauseController : IDisposable
{
    public const string StateKey = "local_etl_paused";

    private readonly IAgentStore _store;
    private readonly AgentRuntimeState _state;
    private readonly SemaphoreSlim _gate = new(1, 1);

    public LocalEtlPauseController(IAgentStore store, AgentRuntimeState state)
    {
        _store = store;
        _state = state;
    }

    public async Task<bool> RestoreAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var value = await _store.GetStateAsync(StateKey, cancellationToken).ConfigureAwait(false);
            var paused = Parse(value);
            _state.RestoreLocalEtlPause(paused);
            return paused;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task SetAsync(bool paused, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!_state.IsReady) throw new InvalidOperationException("Mode state has not completed bootstrap.");
            await _store.SetStateAsync(StateKey, Serialize(paused), CancellationToken.None).ConfigureAwait(false);
            _state.SetLocalEtlPause(paused);
        }
        finally
        {
            _gate.Release();
        }
    }

    public void Dispose() => _gate.Dispose();

    private static string Serialize(bool paused) => $"{{\"paused\":{(paused ? "true" : "false")}}}";

    private static bool Parse(string? value)
    {
        if (value is null) return false;
        try
        {
            using var document = JsonDocument.Parse(value);
            if (document.RootElement.ValueKind is JsonValueKind.True or JsonValueKind.False)
                return document.RootElement.GetBoolean();
            if (document.RootElement.ValueKind != JsonValueKind.Object)
                throw new InvalidDataException("Local ETL pause state must be a JSON boolean or object.");
            var properties = document.RootElement.EnumerateObject().ToArray();
            if (properties.Length != 1
                || !string.Equals(properties[0].Name, "paused", StringComparison.Ordinal)
                || properties[0].Value.ValueKind is not (JsonValueKind.True or JsonValueKind.False))
                throw new InvalidDataException("Local ETL pause state is invalid.");
            return properties[0].Value.GetBoolean();
        }
        catch (JsonException ex)
        {
            throw new InvalidDataException("Local ETL pause state is not valid JSON.", ex);
        }
    }
}
