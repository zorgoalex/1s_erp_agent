using ErpOnecAgent.Application.Etl;

namespace ErpOnecAgent.Infrastructure.Spool;

/// <summary>
/// A08: free space available to the caller on the volume holding <c>path</c> (the path need
/// not exist yet). On Windows it uses GetDiskFreeSpaceEx on the nearest existing directory,
/// which also covers UNC shares and folder mount points; elsewhere DriveInfo.
/// </summary>
public sealed class DriveInfoDiskSpaceProbe : IDiskSpaceProbe
{
    public long GetAvailableFreeBytes(string path)
    {
        var full = Path.GetFullPath(path);
        var existing = full;
        while (!Directory.Exists(existing))
        {
            existing = Path.GetDirectoryName(existing) ?? throw new InvalidOperationException($"Cannot resolve the volume of '{path}'.");
        }
        if (OperatingSystem.IsWindows())
        {
            var directory = existing.EndsWith(Path.DirectorySeparatorChar) ? existing : existing + Path.DirectorySeparatorChar;
            if (!GetDiskFreeSpaceEx(directory, out var available, out _, out _))
                throw new IOException($"GetDiskFreeSpaceEx failed for '{directory}'.", System.Runtime.InteropServices.Marshal.GetHRForLastWin32Error());
            return available > long.MaxValue ? long.MaxValue : (long)available;
        }
        var root = Path.GetPathRoot(existing);
        if (string.IsNullOrEmpty(root)) throw new InvalidOperationException($"Cannot resolve the volume of '{path}'.");
        return new DriveInfo(root).AvailableFreeSpace;
    }

    [System.Runtime.InteropServices.DllImport("kernel32.dll", EntryPoint = "GetDiskFreeSpaceExW", SetLastError = true, CharSet = System.Runtime.InteropServices.CharSet.Unicode)]
    [return: System.Runtime.InteropServices.MarshalAs(System.Runtime.InteropServices.UnmanagedType.Bool)]
    private static extern bool GetDiskFreeSpaceEx(string directoryName, out ulong freeBytesAvailableToCaller, out ulong totalNumberOfBytes, out ulong totalNumberOfFreeBytes);
}
