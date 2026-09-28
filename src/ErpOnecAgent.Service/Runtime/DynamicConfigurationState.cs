using System.Collections.ObjectModel;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Domain.Agent;
using ErpOnecAgent.Domain.Etl;
using Microsoft.Extensions.Options;

namespace ErpOnecAgent.Service.Runtime;

public sealed record DynamicConfigurationSnapshot(
    long Version,
    IReadOnlyList<string> CommandTypes,
    IReadOnlyList<EtlEntityDefinition> Entities,
    int IntervalMinutes,
    AgentMode Mode,
    string? SourceGeneration = null);

/// <summary>E4: the last remote configuration the agent refused, reported in heartbeat.</summary>
public sealed record ConfigurationRejection(long Version, string Reason);

public sealed class DynamicConfigurationState
{
    private readonly object _gate = new();
    private ConfigurationRejection? _rejection;
    private long _version;
    private IReadOnlyList<string> _commandTypes;
    // Administrative (agent-executed) types are allowed by the LOCAL allowlist only
    // (Commands:SupportedTypes), independent of ERP; ERP's configuration decides business types.
    // User decision 2026-09-28, agreed with ERP (agent-bridge to-onec/0018, variant 1).
    private readonly string[] _localAdministrativeTypes;
    private IReadOnlyList<EtlEntityDefinition> _entities;
    private int _intervalMinutes;
    private AgentMode _mode = AgentMode.Normal;
    private string? _sourceGeneration;

    public DynamicConfigurationState(IOptions<CommandOptions> commands, IOptions<EtlOptions> etl)
    {
        _commandTypes = Freeze(commands.Value.SupportedTypes);
        _localAdministrativeTypes = commands.Value.SupportedTypes
            .Where(ErpOnecAgent.Application.Commands.AdministrativeCommandRouting.IsAdministrativeCommandType)
            .Distinct(StringComparer.Ordinal).ToArray();
        _entities = FreezeEntities(etl.Value.Entities);
        _intervalMinutes = etl.Value.IntervalMinutes;
    }

    public DynamicConfigurationSnapshot Snapshot
    {
        get
        {
            lock (_gate)
            {
                return new(_version, _commandTypes, _entities, _intervalMinutes, _mode, _sourceGeneration);
            }
        }
    }

    public long Version => Snapshot.Version;
    public IReadOnlyList<string> CommandTypes => Snapshot.CommandTypes;
    public IReadOnlyList<EtlEntityDefinition> Entities => Snapshot.Entities;
    public int IntervalMinutes => Snapshot.IntervalMinutes;
    public AgentMode Mode => Snapshot.Mode;
    public string? SourceGeneration => Snapshot.SourceGeneration;

    /// <summary>E4: the last refused version newer than the active one; null once a version at least as new is active.</summary>
    public ConfigurationRejection? Rejection
    {
        get
        {
            lock (_gate) return _rejection is { } rejection && rejection.Version > _version ? rejection : null;
        }
    }

    public void RecordRejection(long version, string reason)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reason);
        lock (_gate) _rejection = new(version, reason);
    }

    public DynamicConfigurationSnapshot Prepare(long version, RemoteAgentConfiguration configuration)
    {
        if (version <= 0) throw new InvalidDataException("Remote config version must be positive.");
        lock (_gate)
        {
            if (version < _version) throw new InvalidDataException("Remote configuration rollback is not allowed without an explicit administrative workflow.");
        }
        if (configuration is null) throw new InvalidDataException("Remote configuration is null.");
        if (configuration.CommandTypes is null || configuration.EtlEntities is null) throw new InvalidDataException("Remote configuration lists cannot be null.");
        if (configuration.EtlIntervalMinutes <= 0) throw new InvalidDataException("Remote ETL interval must be positive.");
        if (configuration.SourceGeneration is not null && !ErpOnecAgent.Application.Etl.SourceGenerationToken.IsValid(configuration.SourceGeneration))
            throw new InvalidDataException("Remote sourceGeneration must be 1..128 printable ASCII characters without spaces.");
        if (configuration.CommandTypes.Any(string.IsNullOrWhiteSpace) || configuration.CommandTypes.Distinct(StringComparer.Ordinal).Count() != configuration.CommandTypes.Count) throw new InvalidDataException("Remote command type allowlist is invalid.");
        if (configuration.EtlEntities.Any(static value => value is null)) throw new InvalidDataException("Remote ETL entities cannot contain null.");
        if (configuration.EtlEntities.Select(static value => value.EntityCode).Distinct(StringComparer.Ordinal).Count() != configuration.EtlEntities.Count) throw new InvalidDataException("Remote ETL entity codes must be unique.");
        foreach (var entity in configuration.EtlEntities)
        {
            var keys = entity.EffectiveKeyFields();
            if (string.IsNullOrWhiteSpace(entity.EntityCode) || string.IsNullOrWhiteSpace(entity.ODataPath) || string.IsNullOrWhiteSpace(entity.KeyField)
                || entity.Select is null || keys.Any(string.IsNullOrWhiteSpace) || keys.Distinct(StringComparer.Ordinal).Count() != keys.Count || !keys.Contains(entity.KeyField, StringComparer.Ordinal)
                || entity.PageSize is < 1 or > 10_000 || entity.Select.Count == 0 || entity.ODataVersion is < 3 or > 4
                || (entity.UpdatedAtField is not null && entity.UpdatedAtEdmType is not ("Edm.DateTime" or "Edm.DateTimeOffset"))
                || !EtlEntityFilterPolicy.IsAcceptable(entity.Filter))
                throw new InvalidDataException($"Remote ETL entity '{entity.EntityCode}' is invalid.");
        }

        return new(
            version,
            Freeze(EffectiveCommandTypes(configuration.CommandTypes)),
            FreezeEntities(configuration.EtlEntities),
            configuration.EtlIntervalMinutes,
            AgentRuntimeState.ParseRemoteMode(configuration.Mode),
            configuration.SourceGeneration);
    }

    public void Publish(DynamicConfigurationSnapshot snapshot, AgentRuntimeState runtime)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        if (runtime is null) throw new InvalidDataException("Runtime state is null.");
        if (snapshot.Version <= 0) throw new InvalidDataException("Remote config version must be positive.");
        if (!Enum.IsDefined(snapshot.Mode)) throw new InvalidDataException("Remote configuration mode is invalid.");
        lock (_gate)
        {
            if (snapshot.Version < _version) throw new InvalidDataException("Remote configuration rollback is not allowed without an explicit administrative workflow.");
            _version = snapshot.Version;
            _commandTypes = snapshot.CommandTypes;
            _entities = snapshot.Entities;
            _intervalMinutes = snapshot.IntervalMinutes;
            _mode = snapshot.Mode;
            _sourceGeneration = snapshot.SourceGeneration;
            runtime.SetRemoteMode(snapshot.Mode);
        }
    }

    public void Apply(long version, RemoteAgentConfiguration configuration, AgentRuntimeState runtime)
    {
        if (runtime is null) throw new InvalidDataException("Runtime state is null.");
        Publish(Prepare(version, configuration), runtime);
    }

    private static ReadOnlyCollection<string> Freeze(IEnumerable<string> values) => Array.AsReadOnly(values.ToArray());

    /// <summary>ERP's business types plus the locally allowed administrative types; an administrative type listed by ERP but not allowed locally stays refused.</summary>
    internal IEnumerable<string> EffectiveCommandTypes(IEnumerable<string> remoteTypes) =>
        remoteTypes.Where(static type => !ErpOnecAgent.Application.Commands.AdministrativeCommandRouting.IsAdministrativeCommandType(type))
            .Concat(_localAdministrativeTypes)
            .Distinct(StringComparer.Ordinal);

    private static ReadOnlyCollection<EtlEntityDefinition> FreezeEntities(IEnumerable<EtlEntityDefinition> entities) =>
        Array.AsReadOnly(entities.Select(CloneEntity).ToArray());

    private static EtlEntityDefinition CloneEntity(EtlEntityDefinition entity) => entity with
    {
        Select = entity.Select is null ? null! : Array.AsReadOnly(entity.Select.ToArray()),
        KeyFields = entity.KeyFields is null ? null : Array.AsReadOnly(entity.KeyFields.ToArray())
    };
}

/// <summary>
/// A static entity filter is an OData $filter expression from ERP's configuration. It is sent
/// URL-escaped, so it cannot add query options; the bounds keep it a plain expression.
/// </summary>
public static class EtlEntityFilterPolicy
{
    public const int MaxLength = 512;

    public static bool IsAcceptable(string? filter) =>
        filter is null
        || (filter.Length is > 0 and <= MaxLength && !string.IsNullOrWhiteSpace(filter) && filter.All(static c => !char.IsControl(c)));
}
