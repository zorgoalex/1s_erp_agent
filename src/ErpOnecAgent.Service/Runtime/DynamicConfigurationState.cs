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
    AgentMode Mode);

public sealed class DynamicConfigurationState
{
    private readonly object _gate = new();
    private long _version;
    private IReadOnlyList<string> _commandTypes;
    private IReadOnlyList<EtlEntityDefinition> _entities;
    private int _intervalMinutes;
    private AgentMode _mode = AgentMode.Normal;

    public DynamicConfigurationState(IOptions<CommandOptions> commands, IOptions<EtlOptions> etl)
    {
        _commandTypes = Freeze(commands.Value.SupportedTypes);
        _entities = FreezeEntities(etl.Value.Entities);
        _intervalMinutes = etl.Value.IntervalMinutes;
    }

    public DynamicConfigurationSnapshot Snapshot
    {
        get
        {
            lock (_gate)
            {
                return new(_version, _commandTypes, _entities, _intervalMinutes, _mode);
            }
        }
    }

    public long Version => Snapshot.Version;
    public IReadOnlyList<string> CommandTypes => Snapshot.CommandTypes;
    public IReadOnlyList<EtlEntityDefinition> Entities => Snapshot.Entities;
    public int IntervalMinutes => Snapshot.IntervalMinutes;
    public AgentMode Mode => Snapshot.Mode;

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
        if (configuration.CommandTypes.Any(string.IsNullOrWhiteSpace) || configuration.CommandTypes.Distinct(StringComparer.Ordinal).Count() != configuration.CommandTypes.Count) throw new InvalidDataException("Remote command type allowlist is invalid.");
        if (configuration.EtlEntities.Any(static value => value is null)) throw new InvalidDataException("Remote ETL entities cannot contain null.");
        if (configuration.EtlEntities.Select(static value => value.EntityCode).Distinct(StringComparer.Ordinal).Count() != configuration.EtlEntities.Count) throw new InvalidDataException("Remote ETL entity codes must be unique.");
        foreach (var entity in configuration.EtlEntities)
        {
            var keys = entity.EffectiveKeyFields();
            if (string.IsNullOrWhiteSpace(entity.EntityCode) || string.IsNullOrWhiteSpace(entity.ODataPath) || string.IsNullOrWhiteSpace(entity.KeyField)
                || entity.Select is null || keys.Any(string.IsNullOrWhiteSpace) || keys.Distinct(StringComparer.Ordinal).Count() != keys.Count || !keys.Contains(entity.KeyField, StringComparer.Ordinal)
                || entity.PageSize is < 1 or > 10_000 || entity.Select.Count == 0 || entity.ODataVersion is < 3 or > 4
                || (entity.UpdatedAtField is not null && entity.UpdatedAtEdmType is not ("Edm.DateTime" or "Edm.DateTimeOffset")))
                throw new InvalidDataException($"Remote ETL entity '{entity.EntityCode}' is invalid.");
        }

        return new(
            version,
            Freeze(configuration.CommandTypes),
            FreezeEntities(configuration.EtlEntities),
            configuration.EtlIntervalMinutes,
            AgentRuntimeState.ParseRemoteMode(configuration.Mode));
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
            runtime.SetRemoteMode(snapshot.Mode);
        }
    }

    public void Apply(long version, RemoteAgentConfiguration configuration, AgentRuntimeState runtime)
    {
        if (runtime is null) throw new InvalidDataException("Runtime state is null.");
        Publish(Prepare(version, configuration), runtime);
    }

    private static ReadOnlyCollection<string> Freeze(IEnumerable<string> values) => Array.AsReadOnly(values.ToArray());

    private static ReadOnlyCollection<EtlEntityDefinition> FreezeEntities(IEnumerable<EtlEntityDefinition> entities) =>
        Array.AsReadOnly(entities.Select(CloneEntity).ToArray());

    private static EtlEntityDefinition CloneEntity(EtlEntityDefinition entity) => entity with
    {
        Select = entity.Select is null ? null! : Array.AsReadOnly(entity.Select.ToArray()),
        KeyFields = entity.KeyFields is null ? null : Array.AsReadOnly(entity.KeyFields.ToArray())
    };
}
