using System.Text.Json;
using ErpOnecAgent.Application.Configuration;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// ERP describes entities with "oDataPath" (spec §6), the OpenAPI writes "odataPath"; the agent
/// reads remote configuration case-insensitively (agent-bridge to-onec/0025), so both bind.
/// </summary>
public sealed class RemoteEntityNamingTests
{
    // The same options ConfigurationWorker and BootstrapService use for the remote configuration.
    private static readonly JsonSerializerOptions RemoteOptions = new(JsonSerializerDefaults.Web) { PropertyNameCaseInsensitive = true };

    [Theory]
    [InlineData("oDataPath", "oDataVersion")]
    [InlineData("odataPath", "odataVersion")]
    [InlineData("ODataPath", "ODataVersion")]
    public void Both_spellings_of_the_odata_fields_bind(string pathName, string versionName)
    {
        var json = $$"""
            {"mode":"Normal","commandTypes":[],"etlIntervalMinutes":60,"sourceGeneration":"0b9f3c2e-4f7a-4d33-9a58-0f6f2c1d9e10",
             "etlEntities":[{"entityCode":"items","{{pathName}}":"Catalog_Номенклатура","keyField":"Ref_Key","updatedAtField":null,
               "deletedField":"DeletionMark","select":["Ref_Key"],"syncMode":"manual_only","pageSize":500,"overlapMinutes":0,"{{versionName}}":3}]}
            """;

        var configuration = JsonSerializer.Deserialize<RemoteAgentConfiguration>(json, RemoteOptions)!;

        var entity = Assert.Single(configuration.EtlEntities);
        Assert.Equal("Catalog_Номенклатура", entity.ODataPath);
        Assert.Equal(3, entity.ODataVersion);
        Assert.Null(entity.UpdatedAtField);
        Assert.Equal("0b9f3c2e-4f7a-4d33-9a58-0f6f2c1d9e10", configuration.SourceGeneration);
    }

    [Theory]
    [InlineData("Тип eq 'Телефон'", true)]
    [InlineData(null, true)]
    [InlineData("", false)]
    [InlineData("   ", false)]
    [InlineData("Тип eq 'Телефон'\u0000", false)]
    public void A_remote_entity_filter_is_accepted_only_as_a_plain_bounded_expression(string? filter, bool accepted)
    {
        var configuration = new ErpOnecAgent.Service.Runtime.DynamicConfigurationState(
            Microsoft.Extensions.Options.Options.Create(new CommandOptions()), Microsoft.Extensions.Options.Options.Create(new EtlOptions()));
        var entity = new ErpOnecAgent.Domain.Etl.EtlEntityDefinition("counterparty_phones", "Catalog_Контрагенты_КонтактнаяИнформация", "Ref_Key", null, null,
            ["Ref_Key", "LineNumber"], "manual_only", 200, 0, KeyFields: ["Ref_Key", "LineNumber"], Filter: filter);
        var remote = new RemoteAgentConfiguration { Mode = "Normal", CommandTypes = [], EtlEntities = [entity], EtlIntervalMinutes = 60 };

        if (accepted) Assert.Equal(filter, configuration.Prepare(2, remote).Entities.Single().Filter);
        else Assert.Throws<InvalidDataException>(() => configuration.Prepare(2, remote));
        Assert.False(ErpOnecAgent.Service.Runtime.EtlEntityFilterPolicy.IsAcceptable(new string('x', ErpOnecAgent.Service.Runtime.EtlEntityFilterPolicy.MaxLength + 1)));
    }
}
