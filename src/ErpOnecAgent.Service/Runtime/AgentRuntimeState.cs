using ErpOnecAgent.Domain.Agent;

namespace ErpOnecAgent.Service.Runtime;

/// <param name="CompatibilityRejected">
/// A07b B6: latched only by an EXPLICIT ERP compatibility rejection (accepted:false or a parsed
/// minimumAgentVersion above the running binary). It denies new-work admission
/// (<see cref="CanLeaseCommands"/>/<see cref="CanExecuteCommands"/>) and extraction
/// (<see cref="CanExtract"/>) but never gates resolution of already-sent work, result delivery, or
/// batch upload/run completion — those stay governed by readiness alone. Cleared only by a later
/// accepted version-compatible handshake; unknown before the first handshake keeps the baseline
/// admitted-work policy. In-memory only: the latch is intentionally not durable across restart.
/// </param>
public sealed record AgentModeSnapshot(
    bool IsReady,
    bool LocalEtlPaused,
    AgentMode RemoteMode,
    bool HandshakeMaintenance,
    bool CompatibilityRejected,
    AgentMode EffectiveMode)
{
    public bool CanLeaseCommands => IsReady
        && !HandshakeMaintenance
        && !CompatibilityRejected
        && RemoteMode is not (AgentMode.PauseCommands or AgentMode.Drain or AgentMode.Maintenance or AgentMode.Disabled);

    public bool CanExecuteCommands => IsReady
        && !HandshakeMaintenance
        && !CompatibilityRejected
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
        && !CompatibilityRejected
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
    private bool _compatibilityRejected;
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

    /// <summary>
    /// A07b B6: publishes one accepted, version-compatible handshake response coherently — clears
    /// the compatibility rejection and applies THIS response's maintenance flag under the same
    /// lock, so no snapshot ever shows a transient lifting of all restrictions. The local ETL
    /// pause and remote mode inputs are never touched.
    /// </summary>
    public void ApplyCompatibleHandshake(bool maintenanceMode)
    {
        lock (_modeGate)
        {
            _compatibilityRejected = false;
            _handshakeMaintenance = maintenanceMode;
        }
    }

    /// <summary>
    /// A07b B6: latches an explicit ERP compatibility rejection (accepted:false or a parsed
    /// minimumAgentVersion above the running binary). Other mode inputs are untouched; the latch
    /// is cleared only by <see cref="ApplyCompatibleHandshake"/>.
    /// </summary>
    public void LatchCompatibilityRejection()
    {
        lock (_modeGate)
        {
            _compatibilityRejected = true;
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
        return new(_ready, _localEtlPaused, _remoteMode, _handshakeMaintenance, _compatibilityRejected, effectiveMode);
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
