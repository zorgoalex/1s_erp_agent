namespace ErpOnecAgent.Infrastructure.Persistence.Sqlite;

/// <summary>
/// Pinned compatibility catalog for the already-published migrations 001-007.
/// Each entry is keyed by the exact migration version and file name and lists
/// every approved SHA-256 of the migration text: the canonical published
/// (git-blob, LF) checksum plus the historical CRLF checkout variants observed
/// for that exact SQL. No semantic normalization is performed — only these
/// literal hashes are admitted. Versions not listed here keep the strict
/// contract: the stored ledger checksum must equal the raw hash of the
/// embedded resource.
/// </summary>
internal static class MigrationChecksumCatalog
{
    private sealed record Entry(string Name, string PublishedChecksum, string[] ApprovedChecksums);

    private static readonly Dictionary<int, Entry> Entries = new()
    {
        [1] = new("001_initial.sql",
            "6BB8EC130CA7FDD303C08BEF28C117452613D768BA9A995D627F9D4E31F7959A",
            ["6BB8EC130CA7FDD303C08BEF28C117452613D768BA9A995D627F9D4E31F7959A"]),
        [2] = new("002_retry_budgets.sql",
            "95DA7777F172B465E11A8E7439F8A2D5FC7694B775A369D23A55A09610192F04",
            ["95DA7777F172B465E11A8E7439F8A2D5FC7694B775A369D23A55A09610192F04",
             "EB4D22E6A0B747B89FC09B8B9B2857C3AD893F1F15B9010BA38CDF6447CA3590"]),
        [3] = new("003_ordering_claims.sql",
            "C9F6EF47483F8ABEFE6266AE9044D1FF4F20F00EA09B81B882EF1BC8E238981B",
            ["C9F6EF47483F8ABEFE6266AE9044D1FF4F20F00EA09B81B882EF1BC8E238981B",
             "0BF8E5122B6D094C6E65E0781A72B967FCE361130BEBE63F9D9CA47F3AAC44CB"]),
        [4] = new("004_command_payload_conflicts.sql",
            "E75CAF3EF711BE74A91E15BE8FC467330B83C8F943C7C1626DDB38F91C23A6AE",
            ["E75CAF3EF711BE74A91E15BE8FC467330B83C8F943C7C1626DDB38F91C23A6AE"]),
        [5] = new("005_durable_etl_jobs.sql",
            "2D30F71101B88E26721EED688668C804FDF344E567363C5D7961055D41212657",
            ["2D30F71101B88E26721EED688668C804FDF344E567363C5D7961055D41212657",
             "38BB53A4732A258A5B528BB1B2FEEB6E6E8A5ACB4743813F954BB09117911A34"]),
        [6] = new("006_etl_finalize.sql",
            "D1B20CE29BC87A9F962BD9842E5C2D613704540326D5F10AA2796CC85EBA6FB3",
            ["D1B20CE29BC87A9F962BD9842E5C2D613704540326D5F10AA2796CC85EBA6FB3",
             "1634CFEB915BB7530F911104267575AF158F3446D828297D775FAA8693D3166A"]),
        [7] = new("007_etl_ownership.sql",
            "8F4DBACBEE7C66603AD296C53182E8E108F680B7DA37FF5612345204D58B8658",
            ["8F4DBACBEE7C66603AD296C53182E8E108F680B7DA37FF5612345204D58B8658"]),
        [8] = new("008_etl_send_attempts.sql",
            "2984DC879A62064E03F76CA1F2369DEF62DDBD21C2231F382F5583EFB1C2F373",
            ["2984DC879A62064E03F76CA1F2369DEF62DDBD21C2231F382F5583EFB1C2F373"]),
        [9] = new("009_etl_scheduled_runs.sql",
            "C9A69671C4DE9471C03E073E6796D4F9C5C3BB18F03583FF3AE1B30964A9ED07",
            ["C9A69671C4DE9471C03E073E6796D4F9C5C3BB18F03583FF3AE1B30964A9ED07"]),
        [10] = new("010_etl_run_resolutions.sql",
            "AED834B63AC9051690F9DEFDD253A0BD003B026416BDF93934C536AC464DFE20",
            ["AED834B63AC9051690F9DEFDD253A0BD003B026416BDF93934C536AC464DFE20"]),
        [11] = new("011_watermark_domain_resets.sql",
            "6ABF179B5A13EE6928B72116099BCC9402584BE82062D107DE10B28B77BE2BEA",
            ["6ABF179B5A13EE6928B72116099BCC9402584BE82062D107DE10B28B77BE2BEA"]),
    };

    /// <summary>
    /// Returns the checksum to record in the ledger for a newly applied
    /// migration: the canonical published checksum for cataloged versions, the
    /// raw resource checksum otherwise. For cataloged versions the embedded
    /// resource must hash to an approved variant of the exact published file;
    /// any other bytes fail closed.
    /// </summary>
    internal static string ResolveRecordedChecksum(int version, string fileName, string resourceChecksum)
    {
        if (!Entries.TryGetValue(version, out var entry))
        {
            return resourceChecksum;
        }
        if (!string.Equals(entry.Name, fileName, StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"Published migration resource {fileName} does not match cataloged name {entry.Name} for version {version}.");
        }
        if (!IsApproved(entry, resourceChecksum))
        {
            throw new InvalidOperationException($"Embedded migration resource {fileName} does not match any approved checksum for published migration version {version}.");
        }
        return entry.PublishedChecksum;
    }

    /// <summary>
    /// Whether a checksum already stored in the ledger is acceptable for the
    /// given migration: for cataloged versions the resource must itself be an
    /// approved variant of the exact published file and the stored checksum
    /// must be an approved variant; for unlisted versions the stored checksum
    /// must equal the raw resource checksum.
    /// </summary>
    internal static bool IsAcceptedStoredChecksum(int version, string fileName, string resourceChecksum, string storedChecksum)
    {
        if (!Entries.TryGetValue(version, out var entry))
        {
            return string.Equals(resourceChecksum, storedChecksum, StringComparison.Ordinal);
        }
        return string.Equals(entry.Name, fileName, StringComparison.Ordinal)
            && IsApproved(entry, resourceChecksum)
            && IsApproved(entry, storedChecksum);
    }

    private static bool IsApproved(Entry entry, string checksum)
    {
        foreach (var approved in entry.ApprovedChecksums)
        {
            if (string.Equals(approved, checksum, StringComparison.Ordinal)) return true;
        }
        return false;
    }
}
