using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Domain.Etl;

namespace ErpOnecAgent.Service.Runtime;

/// <summary>
/// Reads Etl:Entities from appsettings. The configuration binder builds the positional
/// <see cref="EtlEntityDefinition"/> record through its constructor and SILENTLY DROPS an array
/// element whose non-default parameter is missing or null — e.g. every catalog with
/// <c>"updatedAtField": null</c>, the documented shape. Binding into this mutable settings class
/// keeps every element; an incomplete one then fails the startup validation instead of vanishing.
/// </summary>
public static class EtlEntityConfiguration
{
    public static IReadOnlyList<EtlEntityDefinition> Read(IConfiguration configuration)
    {
        var section = configuration.GetSection(EtlOptions.SectionName).GetSection(nameof(EtlOptions.Entities));
        var settings = section.Get<List<EtlEntitySettings>>() ?? [];
        return settings.Select(static entity => entity.ToDefinition()).ToArray();
    }

    public sealed class EtlEntitySettings
    {
        public string? EntityCode { get; set; }
        public string? ODataPath { get; set; }
        public string? KeyField { get; set; }
        public string? UpdatedAtField { get; set; }
        public string? DeletedField { get; set; }
        public List<string>? Select { get; set; }
        public string? SyncMode { get; set; }
        public int? PageSize { get; set; }
        public int? OverlapMinutes { get; set; }
        public int? SchemaVersion { get; set; }
        public int? ODataVersion { get; set; }
        public bool? Enabled { get; set; }
        public List<string>? KeyFields { get; set; }
        public string? UpdatedAtEdmType { get; set; }

        // Missing required values become empty ones, which the startup validation rejects.
        public EtlEntityDefinition ToDefinition() => new(
            EntityCode ?? string.Empty,
            ODataPath ?? string.Empty,
            KeyField ?? string.Empty,
            string.IsNullOrEmpty(UpdatedAtField) ? null : UpdatedAtField,
            string.IsNullOrEmpty(DeletedField) ? null : DeletedField,
            Select ?? [],
            SyncMode ?? "incremental",
            PageSize ?? new EtlOptions().DefaultPageSize,
            OverlapMinutes ?? new EtlOptions().OverlapMinutes,
            SchemaVersion ?? 1,
            ODataVersion ?? 3,
            Enabled ?? true,
            KeyFields,
            UpdatedAtEdmType ?? "Edm.DateTimeOffset");
    }
}
