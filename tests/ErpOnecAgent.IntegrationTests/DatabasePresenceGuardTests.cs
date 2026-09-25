using ErpOnecAgent.Service.Runtime;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

public sealed class DatabasePresenceGuardTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "ErpOnecAgentTests", "dbguard-" + Guid.NewGuid().ToString("N"));

    private string DatabasePath => Path.Combine(_root, "data", "agent.db");
    private string SpoolPath => Path.Combine(_root, "spool");

    public void Dispose()
    {
        if (Directory.Exists(_root)) Directory.Delete(_root, recursive: true);
    }

    [Fact]
    public void A_fresh_install_without_any_state_may_create_the_database() =>
        Assert.Null(DatabasePresenceGuard.CheckBeforeStart(DatabasePath, SpoolPath));

    [Fact]
    public void An_existing_database_is_fine()
    {
        Directory.CreateDirectory(Path.Combine(_root, "data"));
        File.WriteAllText(Path.Combine(_root, "data", "agent.db"), "x");
        DatabasePresenceGuard.MarkInitialized(DatabasePath);

        Assert.Null(DatabasePresenceGuard.CheckBeforeStart(DatabasePath, SpoolPath));
    }

    [Fact]
    public void A_missing_database_after_initialization_refuses_start()
    {
        DatabasePresenceGuard.MarkInitialized(DatabasePath);

        var reason = DatabasePresenceGuard.CheckBeforeStart(DatabasePath, SpoolPath);

        Assert.NotNull(reason);
        Assert.Contains("initialization marker", reason, StringComparison.Ordinal);
    }

    [Fact]
    public void Backups_or_spool_files_without_a_database_refuse_start()
    {
        Directory.CreateDirectory(Path.Combine(_root, "data", "backups"));
        File.WriteAllText(Path.Combine(_root, "data", "backups", "agent-20260926-000000.db"), "x");
        Directory.CreateDirectory(Path.Combine(_root, "spool", "ready"));
        File.WriteAllText(Path.Combine(_root, "spool", "ready", "b.ndjson.gz"), "x");

        var reason = DatabasePresenceGuard.CheckBeforeStart(DatabasePath, SpoolPath);

        Assert.Contains("maintenance backups", reason, StringComparison.Ordinal);
        Assert.Contains("spool files", reason, StringComparison.Ordinal);
    }

    [Fact]
    public void Marking_is_idempotent_and_keeps_the_first_timestamp()
    {
        DatabasePresenceGuard.MarkInitialized(DatabasePath);
        var first = File.ReadAllText(Path.Combine(_root, "data", DatabasePresenceGuard.MarkerFileName));
        DatabasePresenceGuard.MarkInitialized(DatabasePath);

        Assert.Equal(first, File.ReadAllText(Path.Combine(_root, "data", DatabasePresenceGuard.MarkerFileName)));
    }
}
