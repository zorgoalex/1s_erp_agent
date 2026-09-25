namespace ErpOnecAgent.Application.Etl;

// A08: ETL must never consume the disk reserve that SQLite, command results and the
// outbox rely on. The reserve (StorageOptions.MinimumReservedBytesForCommands) is checked
// against the ACTUAL free space of the data volume — not only against the spool quota —
// before extraction starts, before each batch file is created, and while it is written.

/// <summary>Free space of the volume that holds a path.</summary>
public interface IDiskSpaceProbe
{
    long GetAvailableFreeBytes(string path);
}

/// <summary>Raised when ETL spool writing would cut into the command disk reserve, or the disk is full.</summary>
public sealed class EtlDiskReserveException(string message, Exception? inner = null) : IOException(message, inner);

/// <summary>C1: raised when an ETL batch would exceed the configured spool quota (StorageOptions.MaxSpoolBytes).</summary>
public sealed class EtlSpoolLimitException(string message) : IOException(message);

public static class EtlDiskAdmission
{
    /// <summary>Headroom required on top of the reserve before a new batch file is started.</summary>
    public const long DefaultBatchHeadroomBytes = 64L * 1024 * 1024;

    /// <summary>True when <paramref name="availableFreeBytes"/> leaves the reserve intact after <paramref name="projectedWriteBytes"/>.</summary>
    public static bool Allows(long availableFreeBytes, long reservedBytes, long projectedWriteBytes) =>
        availableFreeBytes - Math.Max(0, projectedWriteBytes) > reservedBytes;

    /// <summary>Uncompressed bytes written between in-write checks; a single larger row is checked on its own.</summary>
    public const long CheckIntervalBytes = 4L * 1024 * 1024;

    /// <summary>
    /// Windows: ERROR_DISK_FULL (112), ERROR_HANDLE_DISK_FULL (39), ERROR_DISK_QUOTA_EXCEEDED
    /// (1295) as Win32 HRESULTs (facility 7). Elsewhere: ENOSPC (28) and EDQUOT (122).
    /// </summary>
    public static bool IsDiskFull(IOException exception) => IsDiskFull(exception.HResult, OperatingSystem.IsWindows());

    public static bool IsDiskFull(int hresult, bool windows) => windows
        ? ((hresult & unchecked((int)0xFFFF0000)) == unchecked((int)0x80070000) && (hresult & 0xFFFF) is 112 or 39 or 1295)
        : hresult is 28 or 122;
}
