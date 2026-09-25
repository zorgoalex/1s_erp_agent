using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using ErpOnecAgent.Service.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// Stage 6: operator CLI for R1 resolution and D1 domain reset. The command refuses while the
/// service (the single-instance lock) is running, requires the workers-stopped attestation, and
/// reports store refusals with a non-zero exit code.
/// </summary>
public sealed class EtlMaintenanceCliTests : IAsyncLifetime
{
    private readonly SqliteTestDatabase _database = new();
    private readonly string _agentId = "cli-test-" + Guid.NewGuid().ToString("N");
    private SqliteConnectionFactory _factory = null!;
    private SqliteAgentStore _store = null!;
    private ServiceProvider _services = null!;

    public async Task InitializeAsync()
    {
        _factory = _database.CreateFactory(Path.Combine("data", "agent.db"));
        _store = new SqliteAgentStore(_factory, new SqliteMigrator(_factory));
        await _store.InitializeAsync(CancellationToken.None);
        _services = new ServiceCollection().AddSingleton(Options.Create(new AgentOptions { AgentId = _agentId })).BuildServiceProvider();
    }

    public async Task DisposeAsync()
    {
        await _services.DisposeAsync();
        await _database.DisposeAsync();
    }

    [Fact]
    public async Task Resolve_refuses_without_the_workers_stopped_attestation()
    {
        await Assert.ThrowsAnyAsync<ArgumentException>(() => CliRunner.RunEtlMaintenanceAsync(_services, _store,
            ["--etl-resolve-run", Guid.NewGuid().ToString("D"), "--decision", "retry", "--operator", "ops", "--verification", "checked ERP"], CancellationToken.None));
    }

    [Fact]
    public async Task Resolve_of_an_unknown_run_exits_with_refusal()
    {
        var exit = await CliRunner.RunEtlMaintenanceAsync(_services, _store,
            ["--etl-resolve-run", Guid.NewGuid().ToString("D"), "--decision", "retry", "--operator", "ops", "--verification", "checked ERP", "--workers-stopped"], CancellationToken.None);

        Assert.Equal(2, exit);
    }

    [Fact]
    public async Task Commands_refuse_while_the_service_holds_the_instance_lock()
    {
        using var running = new SingleInstanceLock();
        running.Acquire(_agentId);

        await Assert.ThrowsAsync<InvalidOperationException>(() => CliRunner.RunEtlMaintenanceAsync(_services, _store,
            ["--etl-reset-domain", "clients", "--generation", "1", "--operator", "ops", "--reason", "epoch rotated"], CancellationToken.None));
    }

    [Fact]
    public async Task Reset_domain_archives_the_row_and_reports_success()
    {
        await ExecuteSqlAsync("INSERT INTO watermarks(entity_name,committed_cursor_json,extracting_cursor_json,last_run_id,updated_at_utc,generation,domain_fingerprint) VALUES('clients','{\"u\":1}',NULL,NULL,'2026-09-26T00:00:00.0000000+00:00',3,NULL);");

        var exit = await CliRunner.RunEtlMaintenanceAsync(_services, _store,
            ["--etl-reset-domain", "clients", "--generation", "3", "--operator", "ops", "--reason", "legacy row without fingerprint"], CancellationToken.None);

        Assert.Equal(0, exit);
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM watermarks WHERE entity_name='clients'"));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM watermark_domain_resets WHERE entity_name='clients' AND operator_id='ops'"));
    }

    [Fact]
    public async Task Reset_with_a_stale_generation_exits_with_refusal_and_keeps_the_row()
    {
        await ExecuteSqlAsync("INSERT INTO watermarks(entity_name,committed_cursor_json,extracting_cursor_json,last_run_id,updated_at_utc,generation,domain_fingerprint) VALUES('clients','{\"u\":1}',NULL,NULL,'2026-09-26T00:00:00.0000000+00:00',3,NULL);");

        var exit = await CliRunner.RunEtlMaintenanceAsync(_services, _store,
            ["--etl-reset-domain", "clients", "--generation", "2", "--operator", "ops", "--reason", "stale"], CancellationToken.None);

        Assert.Equal(2, exit);
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM watermarks WHERE entity_name='clients'"));
    }

    [Theory]
    [InlineData("--etl-reset-domain|clients|--generation|1|--reason|x")]
    [InlineData("--etl-reset-domain|clients|--operator|ops|--reason|x")]
    [InlineData("--etl-resolve-run|not-a-guid|--decision|retry|--operator|ops|--verification|v|--workers-stopped")]
    [InlineData("--etl-resolve-run|8f8f8f8f-8f8f-4f8f-8f8f-8f8f8f8f8f8f|--decision|maybe|--operator|ops|--verification|v|--workers-stopped")]
    public async Task Missing_or_invalid_arguments_are_rejected(string joined)
    {
        var args = joined.Split('|');
        await Assert.ThrowsAnyAsync<ArgumentException>(() => CliRunner.RunEtlMaintenanceAsync(_services, _store, args, CancellationToken.None));
    }

    [Fact]
    public async Task Resolve_and_reset_together_are_rejected()
    {
        await Assert.ThrowsAnyAsync<ArgumentException>(() => CliRunner.RunEtlMaintenanceAsync(_services, _store,
            ["--etl-resolve-run", Guid.NewGuid().ToString("D"), "--etl-reset-domain", "clients", "--generation", "1", "--decision", "retry", "--operator", "ops", "--verification", "v", "--workers-stopped", "--reason", "x"], CancellationToken.None));
    }

    [Theory]
    [InlineData("--integrity-check")]
    [InlineData("--etl-reset-domain|clients|--generation|1|--operator|ops|--reason|x")]
    public async Task Cli_commands_do_not_recreate_a_lost_database(string joined)
    {
        var root = Path.Combine(Path.GetTempPath(), "ErpOnecAgentTests", "cli-lostdb-" + Guid.NewGuid().ToString("N"));
        var databasePath = Path.Combine(root, "data", "agent.db");
        DatabasePresenceGuard.MarkInitialized(databasePath);
        try
        {
            var factory = new SqliteConnectionFactory(databasePath);
            await using var services = new ServiceCollection()
                .AddSingleton(Options.Create(new AgentOptions { AgentId = _agentId, DataDirectory = root }))
                .AddSingleton(factory)
                .AddSingleton<ErpOnecAgent.Application.Abstractions.IAgentStore>(new SqliteAgentStore(factory, new SqliteMigrator(factory)))
                .BuildServiceProvider();

            var exit = await CliRunner.RunAsync(services, joined.Split('|'), CancellationToken.None);

            Assert.Equal(4, exit);
            Assert.False(File.Exists(databasePath));
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, recursive: true);
        }
    }

    private async Task<long> ScalarAsync(string sql)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(await command.ExecuteScalarAsync(CancellationToken.None), System.Globalization.CultureInfo.InvariantCulture);
    }

    private async Task ExecuteSqlAsync(string sql)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }
}
