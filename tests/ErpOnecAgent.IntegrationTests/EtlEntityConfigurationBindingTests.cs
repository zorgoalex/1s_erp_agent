using System.Text;
using ErpOnecAgent.Application.Configuration;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// appsettings.json binding of Etl:Entities — the positional EtlEntityDefinition record is built
/// through its constructor, so every documented shape (including updatedAtField: null) must bind.
/// </summary>
public sealed class EtlEntityConfigurationBindingTests
{
    [Fact]
    public void A_catalog_without_a_date_field_binds_with_a_null_updated_at_field()
    {
        var options = Bind("""
            { "Etl": { "Entities": [ {
              "entityCode": "counterparties", "odataPath": "Catalog_Контрагенты", "keyField": "Ref_Key",
              "updatedAtField": null, "deletedField": "DeletionMark", "select": [ "Ref_Key", "Description" ],
              "syncMode": "manual_only", "pageSize": 500, "overlapMinutes": 10, "schemaVersion": 1, "oDataVersion": 3, "enabled": true } ] } }
            """);

        var entity = Assert.Single(options.Entities);
        Assert.Equal("counterparties", entity.EntityCode);
        Assert.Null(entity.UpdatedAtField);
        Assert.Equal(["Ref_Key", "Description"], entity.Select);
        Assert.True(entity.Enabled);
    }

    [Fact]
    public void A_catalog_without_the_updated_at_key_binds_too()
    {
        var options = Bind("""
            { "Etl": { "Entities": [ {
              "entityCode": "items", "odataPath": "Catalog_Номенклатура", "keyField": "Ref_Key",
              "deletedField": "DeletionMark", "select": [ "Ref_Key" ], "syncMode": "manual_only", "pageSize": 200, "overlapMinutes": 0 } ] } }
            """);

        var entity = Assert.Single(options.Entities);
        Assert.Null(entity.UpdatedAtField);
    }

    [Fact]
    public void A_dated_entity_binds_its_field_and_edm_type()
    {
        var options = Bind("""
            { "Etl": { "Entities": [ {
              "entityCode": "orders", "odataPath": "Document_Orders", "keyField": "Ref_Key", "updatedAtField": "Date",
              "updatedAtEdmType": "Edm.DateTime", "deletedField": null, "select": [ "Ref_Key", "Date" ],
              "syncMode": "incremental", "pageSize": 500, "overlapMinutes": 10 } ] } }
            """);

        var entity = Assert.Single(options.Entities);
        Assert.Equal("Date", entity.UpdatedAtField);
        Assert.Equal("Edm.DateTime", entity.UpdatedAtEdmType);
        Assert.Null(entity.DeletedField);
    }

    [Fact]
    public void An_incomplete_entity_is_kept_and_fails_validation_instead_of_vanishing()
    {
        var options = Bind("""{ "Etl": { "Entities": [ { "entityCode": "broken", "select": [ "Ref_Key" ] } ] } }""");

        var entity = Assert.Single(options.Entities);
        Assert.Equal(string.Empty, entity.ODataPath);
        Assert.Equal(string.Empty, entity.KeyField);
    }

    [Fact]
    public void The_shipped_appsettings_and_the_e2e_shape_bind_every_entity()
    {
        var configuration = new ConfigurationBuilder().AddJsonStream(new MemoryStream(Encoding.UTF8.GetBytes("""
            { "Etl": { "Entities": [
              { "EntityCode": "items", "ODataPath": "Catalog_Номенклатура", "KeyField": "Ref_Key", "UpdatedAtField": null, "DeletedField": "DeletionMark",
                "Select": [ "Ref_Key", "DataVersion" ], "SyncMode": "manual_only", "PageSize": 200, "OverlapMinutes": 0, "SchemaVersion": 1, "ODataVersion": 3, "Enabled": true },
              { "EntityCode": "counterparties", "ODataPath": "Catalog_Контрагенты", "KeyField": "Ref_Key", "UpdatedAtField": null, "DeletedField": "DeletionMark",
                "Select": [ "Ref_Key" ], "SyncMode": "manual_only", "PageSize": 200, "OverlapMinutes": 0 } ] } }
            """))).Build();

        var entities = ErpOnecAgent.Service.Runtime.EtlEntityConfiguration.Read(configuration);

        Assert.Equal(["items", "counterparties"], entities.Select(static e => e.EntityCode).ToArray());
        Assert.All(entities, static e => Assert.Null(e.UpdatedAtField));
        Assert.Equal("manual_only", entities[0].SyncMode);
    }

    private static EtlOptions Bind(string json)
    {
        var configuration = new ConfigurationBuilder().AddJsonStream(new MemoryStream(Encoding.UTF8.GetBytes(json))).Build();
        return new EtlOptions { Entities = ErpOnecAgent.Service.Runtime.EtlEntityConfiguration.Read(configuration) };
    }
}
