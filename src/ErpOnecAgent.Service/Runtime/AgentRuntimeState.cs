using ErpOnecAgent.Domain.Agent;

namespace ErpOnecAgent.Service.Runtime;

public sealed record AgentModeSnapshot(
    bool IsReady,
    bool LocalEtlPaused,
    AgentMode RemoteMode,
    bool HandshakeMaintenance,
    AgentMode EffectiveMode)
{
    public bool CanLeaseCommands => IsReady
        && !HandshakeMaintenance
        && RemoteMode is not (AgentMode.PauseCommands or AgentMode.Drain or AgentMode.Maintenance or AgentMode.Disabled);

    public bool CanExecuteCommands => IsReady
        && !HandshakeMaintenance
        && RemoteMode is not (AgentMode.PauseCommands or AgentMode.Maintenance or AgentMode.Disabled);

    /// <summary>
    /// A07b resolution/admission split: status lookup of already-sent business commands is
    /// resolution work, not new-work admission, so it is permitted whenever the agent is ready —
    /// under every command restriction (<c>PauseCommands</c>/<c>Maintenance</c>/<c>Disabled</c>/
    /// handshake maintenance). This does NOT permit any POST to 1C or any administrative side
    /// effect; admission for new work stays governed by <see cref="CanExecuteCommands"/>.
    /// </summary>
    public bool CanResolveCommandResults => IsReady;

    public bool CanExtract => IsReady
        && !LocalEtlPaused
        && !HandshakeMaintenance
        && RemoteMode is not (AgentMode.PauseEtl or AgentMode.Maintenance or AgentMode.Disabled);

    public bool CanDeliverResults => IsReady;
    public bool CanUploadBatches => IsReady;
    public bool CanCompleteEtlRuns => IsReady;
}

public sealed class AgentRuntimeState
{
    private readonly object _modeGate = new();
    private long _startedAtTicks = DateTimeOffset.UtcNow.UtcTicks;
    private bool _localEtlPaused;
    private AgentMode _remoteMode = AgentMode.Normal;
    private bool _handshakeMaintenance;
    private bool _ready;

    public AgentMode Mode => Snapshot.EffectiveMode;
    public AgentMode EffectiveMode => Snapshot.EffectiveMode;
    public AgentModeSnapshot Snapshot
    {
        get
        {
            lock (_modeGate)
            {
                return CreateSnapshot();
            }
        }
    }

    public bool IsReady
    {
        get
        {
            lock (_modeGate)
            {
                return _ready;
            }
        }
    }

    public DateTimeOffset StartedAtUtc => new(Interlocked.Read(ref _startedAtTicks), TimeSpan.Zero);
    public DateTimeOffset? LastErpSuccessAtUtc { get; set; }
    public DateTimeOffset? LastOnecSuccessAtUtc { get; set; }
    public DateTimeOffset? LastEtlSuccessAtUtc { get; set; }
    public Guid? CurrentEtlRunId { get; set; }
    public bool OnecODataAvailable { get; set; }
    public bool OnecCommandApiAvailable { get; set; }
    public string? LastOnecError { get; set; }

    public void RestoreLocalEtlPause(bool paused)
    {
        lock (_modeGate)
        {
            _localEtlPaused = paused;
        }
    }

    public void SetLocalEtlPause(bool paused)
    {
        lock (_modeGate)
        {
            _localEtlPaused = paused;
        }
    }

    public void SetRemoteMode(AgentMode mode)
    {
        ValidateRemoteMode(mode);
        lock (_modeGate)
        {
            _remoteMode = mode;
        }
    }

    public void SetHandshakeMaintenance(bool maintenance)
    {
        lock (_modeGate)
        {
            _handshakeMaintenance = maintenance;
        }
    }

    public void CompleteBootstrap()
    {
        lock (_modeGate)
        {
            _ready = true;
        }
    }

    public static AgentMode ParseRemoteMode(string? value)
    {
        if (string.IsNullOrWhiteSpace(value)
            || !Enum.TryParse<AgentMode>(value, ignoreCase: true, out var mode)
            || !Enum.IsDefined(mode)
            || !Enum.GetNames<AgentMode>().Any(name => string.Equals(name, value, StringComparison.OrdinalIgnoreCase)))
        {
            throw new InvalidDataException($"Unknown remote agent mode '{value}'.");
        }

        return mode;
    }

    private AgentModeSnapshot CreateSnapshot()
    {
        var effectiveMode = ComposeEffectiveMode();
        return new(_ready, _localEtlPaused, _remoteMode, _handshakeMaintenance, effectiveMode);
    }

    private AgentMode ComposeEffectiveMode()
    {
        if (!_ready || !Enum.IsDefined(_remoteMode)) return AgentMode.Disabled;
        if (_remoteMode == AgentMode.Disabled) return AgentMode.Disabled;
        if (_handshakeMaintenance || _remoteMode == AgentMode.Maintenance) return AgentMode.Maintenance;
        if (_remoteMode == AgentMode.Drain) return AgentMode.Drain;
        if (_remoteMode == AgentMode.PauseCommands) return AgentMode.PauseCommands;
        if (_localEtlPaused || _remoteMode == AgentMode.PauseEtl) return AgentMode.PauseEtl;
        return AgentMode.Normal;
    }

    private static void ValidateRemoteMode(AgentMode mode)
    {
        if (!Enum.IsDefined(mode)) throw new InvalidDataException($"Unknown remote agent mode value '{mode}'.");
    }
}
