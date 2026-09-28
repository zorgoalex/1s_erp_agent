using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Service.Runtime;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// User decision 2026-09-28 (agent-bridge to-onec/0018, variant 1): administrative types are
/// allowed by the local Commands:SupportedTypes only; ERP's configuration decides business types.
/// </summary>
public sealed class AdministrativeCommandTypesTests
{
    [Fact]
    public void An_empty_erp_list_still_allows_the_locally_allowed_administrative_types()
    {
        var configuration = Configuration("start_full_sync", "reload_entity", "create_customer_order");

        Publish(configuration);

        Assert.Equal(["reload_entity", "start_full_sync"], configuration.CommandTypes.Order(StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public void Erp_business_types_are_added_and_its_administrative_entries_do_not_widen_the_local_list()
    {
        var configuration = Configuration("start_full_sync");

        Publish(configuration, "integration_probe", "collect_diagnostics", "start_full_sync");

        Assert.Equal(["integration_probe", "start_full_sync"], configuration.CommandTypes.Order(StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public void A_local_list_without_administrative_types_leaves_only_erp_business_types()
    {
        var configuration = Configuration("create_customer_order");

        Publish(configuration, "integration_probe");

        Assert.Equal(["integration_probe"], configuration.CommandTypes.ToArray());
    }

    [Fact]
    public void Before_any_erp_configuration_the_local_list_applies_as_is()
    {
        var configuration = Configuration("start_full_sync", "create_customer_order");

        Assert.Equal(["start_full_sync", "create_customer_order"], configuration.CommandTypes.ToArray());
    }

    private static DynamicConfigurationState Configuration(params string[] localTypes) =>
        new(Options.Create(new CommandOptions { SupportedTypes = localTypes }), Options.Create(new EtlOptions()));

    private static void Publish(DynamicConfigurationState configuration, params string[] remoteTypes)
    {
        var state = new AgentRuntimeState();
        configuration.Apply(5, new RemoteAgentConfiguration { Mode = "Normal", CommandTypes = remoteTypes, EtlEntities = [], EtlIntervalMinutes = 60 }, state);
    }
}
