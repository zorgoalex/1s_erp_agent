using System.Reflection;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using ErpOnecAgent.Service.Runtime;
using ErpOnecAgent.Service.Workers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// Stage 6 backup: a backup is taken only from a database that passes the integrity check,
/// is verified before it counts, and rotation never removes a good backup in favour of an
/// unverified one.
/// </summary>
public sealed class MaintenanceBackupTests : IAsyncLifetime
{
    private readonly SqliteTestDatabase _database = new();
    private readonly string _root = Path.Combine(Path.GetTempPath(), "ErpOnecAgentTests", "backup-" + Guid.NewGuid().ToString("N"));
    private SqliteAgentStore _store = null!;

    private string BackupDirectory => Path.Combine(_root, "data", "backups");

    public async Task InitializeAsync()
    {
        var factory = _database.CreateFactory(Path.Combine("data", "agent.db"));
        _store = new SqliteAgentStore(factory, new SqliteMigrator(factory));
        await _store.InitializeAsync(CancellationToken.None);
    }

    public async Task DisposeAsync()
    {
        await _database.DisposeAsync();
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        if (Directory.Exists(_root)) Directory.Delete(_root, recursive: true);
    }

    [Fact]
    public async Task A_healthy_database_gets_a_verified_backup()
    {
        var worker = Worker(_store);
        var now = new DateTimeOffset(2026, 9, 26, 3, 0, 0, TimeSpan.Zero);

        Assert.Equal(MaintenanceWorker.BackupOutcome.Created, await worker.RunBackupAsync(now, CancellationToken.None));

        var backup = Assert.Single(Directory.GetFiles(BackupDirectory, "agent-*.db"));
        Assert.EndsWith("agent-20260926-030000.db", backup, StringComparison.Ordinal);
        Assert.Equal("ok", await SqliteBackupVerifier.VerifyAsync(backup, CancellationToken.None));
        // One self-contained file: no .tmp, no -wal/-shm sidecars left by the verification.
        Assert.Equal(["agent-20260926-030000.db"], Directory.GetFiles(BackupDirectory).Select(static path => Path.GetFileName(path)).ToArray());
    }

    [Fact]
    public async Task Rotation_keeps_the_newest_by_name_not_by_file_time()
    {
        var worker = Worker(_store, retention: 2);
        var start = new DateTimeOffset(2026, 9, 20, 0, 0, 0, TimeSpan.Zero);
        for (var day = 0; day < 4; day++)
            await worker.RunBackupAsync(start.AddDays(day), CancellationToken.None);
        // Make the OLDEST file look newest on the file system (e.g. restored by a copy tool).
        File.SetCreationTimeUtc(Path.Combine(BackupDirectory, "agent-20260922-000000.db"), DateTime.UtcNow.AddYears(1));

        await worker.RunBackupAsync(start.AddDays(4), CancellationToken.None);

        var names = Directory.GetFiles(BackupDirectory, "agent-*.db").Select(static path => Path.GetFileName(path)).Order(StringComparer.Ordinal).ToArray();
        Assert.Equal(["agent-20260923-000000.db", "agent-20260924-000000.db"], names);
    }

    [Fact]
    public async Task A_corrupt_live_database_is_not_backed_up_and_old_backups_are_kept()
    {
        var good = Worker(_store, retention: 1);
        await good.RunBackupAsync(new DateTimeOffset(2026, 9, 25, 0, 0, 0, TimeSpan.Zero), CancellationToken.None);
        var worker = Worker(StoreProxy.Create(_store, integrity: "*** in database main ***\nPage 7 is never used"), retention: 1);

        var outcome = await worker.RunBackupAsync(new DateTimeOffset(2026, 9, 26, 0, 0, 0, TimeSpan.Zero), CancellationToken.None);

        Assert.Equal(MaintenanceWorker.BackupOutcome.LiveDatabaseCorrupt, outcome);
        Assert.Equal(["agent-20260925-000000.db"], Directory.GetFiles(BackupDirectory, "agent-*.db").Select(static path => Path.GetFileName(path)).ToArray());
    }

    [Fact]
    public async Task An_invalid_backup_is_discarded_and_never_displaces_a_good_one()
    {
        var good = Worker(_store, retention: 1);
        await good.RunBackupAsync(new DateTimeOffset(2026, 9, 25, 0, 0, 0, TimeSpan.Zero), CancellationToken.None);
        var worker = Worker(StoreProxy.Create(_store, corruptBackup: true), retention: 1);

        var outcome = await worker.RunBackupAsync(new DateTimeOffset(2026, 9, 26, 0, 0, 0, TimeSpan.Zero), CancellationToken.None);

        Assert.Equal(MaintenanceWorker.BackupOutcome.BackupInvalid, outcome);
        Assert.Equal(["agent-20260925-000000.db"], Directory.GetFiles(BackupDirectory, "agent-*.db").Select(static path => Path.GetFileName(path)).ToArray());
        Assert.Equal(["agent-20260925-000000.db"], Directory.GetFiles(BackupDirectory).Select(static path => Path.GetFileName(path)).ToArray());
    }

    [Fact]
    public async Task Stale_temporary_copies_are_swept_and_foreign_names_are_not_rotated()
    {
        Directory.CreateDirectory(BackupDirectory);
        await File.WriteAllTextAsync(Path.Combine(BackupDirectory, "agent-20260901-000000.db.tmp"), "partial");
        await File.WriteAllTextAsync(Path.Combine(BackupDirectory, "agent-20260901-000000.db.tmp-wal"), "x");
        await File.WriteAllTextAsync(Path.Combine(BackupDirectory, "agent-manual.db"), "operator copy");
        var worker = Worker(_store, retention: 1);

        await worker.RunBackupAsync(new DateTimeOffset(2026, 9, 26, 0, 0, 0, TimeSpan.Zero), CancellationToken.None);
        await worker.RunBackupAsync(new DateTimeOffset(2026, 9, 27, 0, 0, 0, TimeSpan.Zero), CancellationToken.None);

        Assert.Equal(["agent-20260927-000000.db", "agent-manual.db"],
            Directory.GetFiles(BackupDirectory).Select(static path => Path.GetFileName(path)).Order(StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public async Task A_corrupt_live_database_skips_the_destructive_cleanup()
    {
        var calls = new List<string>();
        var worker = Worker(StoreProxy.Create(_store, integrity: "*** corrupt ***", calls: calls));

        await worker.RunCycleAsync(DateTimeOffset.UtcNow, CancellationToken.None);

        Assert.DoesNotContain(nameof(IAgentStore.GetAcknowledgedBatchesAsync), calls);
        Assert.DoesNotContain(nameof(IAgentStore.CleanupAsync), calls);
        Assert.DoesNotContain(nameof(IAgentStore.BackupAsync), calls);
    }

    [Fact]
    public async Task A_healthy_database_runs_the_cleanup()
    {
        var calls = new List<string>();
        var worker = Worker(StoreProxy.Create(_store, calls: calls));

        await worker.RunCycleAsync(DateTimeOffset.UtcNow, CancellationToken.None);

        Assert.Contains(nameof(IAgentStore.CleanupAsync), calls);
        Assert.Contains(nameof(IAgentStore.RunMaintenanceAsync), calls);
    }

    [Fact]
    public async Task The_verifier_rejects_a_file_that_is_not_a_database()
    {
        Directory.CreateDirectory(_root);
        var path = Path.Combine(_root, "garbage.db");
        await File.WriteAllTextAsync(path, "this is not sqlite");

        Assert.NotEqual("ok", await SqliteBackupVerifier.VerifyAsync(path, CancellationToken.None));
    }

    private MaintenanceWorker Worker(IAgentStore store, int retention = 7) => new(
        store,
        new NullSpool(),
        new AgentRuntimeState(),
        Options.Create(new AgentOptions { AgentId = "backup-test", DataDirectory = _root }),
        Options.Create(new StorageOptions { BackupRetentionCount = retention }),
        NullLogger<MaintenanceWorker>.Instance);

    public class StoreProxy : DispatchProxy
    {
        private IAgentStore _inner = null!;
        private string? _integrity;
        private bool _corruptBackup;
        private List<string>? _calls;

        public static IAgentStore Create(IAgentStore inner, string? integrity = null, bool corruptBackup = false, List<string>? calls = null)
        {
            var proxy = DispatchProxy.Create<IAgentStore, StoreProxy>();
            var state = (StoreProxy)(object)proxy;
            state._inner = inner;
            state._integrity = integrity;
            state._corruptBackup = corruptBackup;
            state._calls = calls;
            return proxy;
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod is not null) _calls?.Add(targetMethod.Name);
            if (targetMethod?.Name == nameof(IAgentStore.IntegrityCheckAsync) && _integrity is not null)
                return Task.FromResult(_integrity);
            if (targetMethod?.Name == nameof(IAgentStore.BackupAsync) && _corruptBackup)
            {
                var destination = (string)args![0]!;
                Directory.CreateDirectory(Path.GetDirectoryName(destination)!);
                File.WriteAllBytes(destination, [.. "SQLite format 3\0"u8, .. new byte[4096]]);
                return Task.CompletedTask;
            }
            return targetMethod!.Invoke(_inner, args);
        }
    }

    private sealed class NullSpool : ISpoolStore
    {
        public Task<ErpOnecAgent.Domain.Etl.EtlBatch> WriteBatchAsync(Guid runId, ErpOnecAgent.Domain.Etl.EtlEntityDefinition entity, IReadOnlyList<System.Text.Json.JsonElement> rows, ErpOnecAgent.Domain.Etl.EtlCursor? watermarkFrom, ErpOnecAgent.Domain.Etl.EtlCursor? watermarkTo, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<Stream> OpenReadAsync(ErpOnecAgent.Domain.Etl.EtlBatch batch, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<long> GetSizeAsync(CancellationToken cancellationToken) => Task.FromResult(0L);
        public Task QuarantineTemporaryFilesAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task DeleteAcknowledgedAsync(ErpOnecAgent.Domain.Etl.EtlBatch batch, CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
