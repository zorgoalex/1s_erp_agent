namespace ErpOnecAgent.Service.Runtime;

/// <summary>
/// Stage 6: the service must never silently start on a fresh empty SQLite database when agent
/// state existed before — that would lose accepted commands, results, ETL evidence and
/// watermarks without a trace. A marker is written next to the database after its first
/// successful initialization. If the database file is missing while the marker, a
/// maintenance backup or spool files exist, startup refuses. Creating a new database is then
/// an explicit operator decision: restore a backup, or run <c>--migrate</c>.
/// </summary>
public static class DatabasePresenceGuard
{
    public const string MarkerFileName = "agent.db.initialized";

    /// <summary>Returns null when starting is safe, otherwise the reason to refuse. The marker and
    /// backups are looked up next to the actual database file; spool files in the spool root.</summary>
    public static string? CheckBeforeStart(string databasePath, string spoolDirectory)
    {
        if (File.Exists(databasePath)) return null;
        var dataDir = Path.GetDirectoryName(Path.GetFullPath(databasePath)) ?? throw new InvalidOperationException("Database path has no parent directory.");
        var evidence = new List<string>();
        if (File.Exists(Path.Combine(dataDir, MarkerFileName))) evidence.Add("initialization marker");
        var backups = Path.Combine(dataDir, "backups");
        if (Directory.Exists(backups) && Directory.EnumerateFiles(backups, "agent-*.db").Any()) evidence.Add("maintenance backups");
        if (Directory.Exists(spoolDirectory) && Directory.EnumerateFiles(spoolDirectory, "*", SearchOption.AllDirectories).Any()) evidence.Add("spool files");
        return evidence.Count == 0
            ? null
            : $"SQLite database '{databasePath}' is missing although agent state exists ({string.Join(", ", evidence)}). Restore it from backup (docs/operations.md) or create a new database explicitly with --migrate.";
    }

    public static void MarkInitialized(string databasePath)
    {
        var dataDir = Path.GetDirectoryName(Path.GetFullPath(databasePath)) ?? throw new InvalidOperationException("Database path has no parent directory.");
        Directory.CreateDirectory(dataDir);
        var marker = Path.Combine(dataDir, MarkerFileName);
        if (!File.Exists(marker)) File.WriteAllText(marker, DateTimeOffset.UtcNow.ToString("O", System.Globalization.CultureInfo.InvariantCulture));
    }
}
