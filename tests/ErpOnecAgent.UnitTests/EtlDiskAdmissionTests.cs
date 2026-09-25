using System.Text.Json;
using ErpOnecAgent.Application.Etl;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.Spool;
using Xunit;

namespace ErpOnecAgent.UnitTests;

/// <summary>
/// A08: spool writing never consumes the command disk reserve. The probe is faked; the
/// spool writes real files into a temporary directory.
/// </summary>
public sealed class EtlDiskAdmissionTests : IDisposable
{
    private const long Reserve = 1_000_000;
    private readonly string _root = Path.Combine(Path.GetTempPath(), "ErpOnecAgentTests", "a08-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_root)) Directory.Delete(_root, recursive: true);
    }

    [Theory]
    [InlineData(2_000_000, 500_000, true)]
    [InlineData(1_500_000, 500_000, false)]
    [InlineData(1_000_001, 0, true)]
    [InlineData(1_000_000, 0, false)]
    [InlineData(900_000, -5, false)]
    public void Allows_keeps_the_reserve_strictly_intact(long free, long projected, bool expected) =>
        Assert.Equal(expected, EtlDiskAdmission.Allows(free, Reserve, projected));

    [Theory]
    [InlineData(unchecked((int)0x80070070), true, true)]   // ERROR_DISK_FULL
    [InlineData(unchecked((int)0x80070027), true, true)]   // ERROR_HANDLE_DISK_FULL
    [InlineData(unchecked((int)0x8007050F), true, true)]   // ERROR_DISK_QUOTA_EXCEEDED
    [InlineData(unchecked((int)0x80070005), true, false)]  // access denied
    [InlineData(112, true, false)]                         // bare 112 is not a Win32 HRESULT
    [InlineData(28, false, true)]                          // ENOSPC
    [InlineData(122, false, true)]                         // EDQUOT
    [InlineData(39, false, false)]                         // ENOTEMPTY on Linux is not disk full
    [InlineData(112, false, false)]                        // EHOSTDOWN on Linux is not disk full
    public void Disk_full_codes_are_recognized_per_platform(int hresult, bool windows, bool expected) =>
        Assert.Equal(expected, EtlDiskAdmission.IsDiskFull(hresult, windows));

    [Fact]
    public async Task A_single_large_row_is_checked_against_its_own_size_before_it_is_written()
    {
        // Enough for the preflight headroom, but not for one 5 MB row on top of the reserve.
        var probe = new FakeProbe(Reserve + EtlDiskAdmission.DefaultBatchHeadroomBytes + 1, Reserve + 1_000_000);
        var spool = new FileSpoolStore(_root, diskProbe: probe, reservedBytesForCommands: Reserve);
        var huge = new[] { JsonSerializer.SerializeToElement(new { Ref_Key = "big", UpdatedAt = "2026-09-26T00:00:00Z", DeletionMark = false, Blob = new string('x', 5 * 1024 * 1024) }) };

        await Assert.ThrowsAsync<EtlDiskReserveException>(() => spool.WriteBatchAsync(Guid.NewGuid(), Entity(), huge, null, null, CancellationToken.None));

        Assert.Empty(Directory.EnumerateFiles(_root, "*", SearchOption.AllDirectories));
        Assert.Equal(2, probe.Calls);
    }

    [Fact]
    public async Task Payload_volume_triggers_a_check_even_below_the_row_interval()
    {
        // 60 rows of ~100 KB = ~6 MB < 256 rows, but crosses the 4 MB byte interval once.
        var roomy = Reserve + EtlDiskAdmission.DefaultBatchHeadroomBytes * 2;
        var probe = new FakeProbe(roomy, Reserve - 1);
        var spool = new FileSpoolStore(_root, diskProbe: probe, reservedBytesForCommands: Reserve);
        var rows = Enumerable.Range(0, 60)
            .Select(i => JsonSerializer.SerializeToElement(new { Ref_Key = $"k{i}", UpdatedAt = "2026-09-26T00:00:00Z", DeletionMark = false, Blob = new string('y', 100 * 1024) }))
            .ToArray();

        await Assert.ThrowsAsync<EtlDiskReserveException>(() => spool.WriteBatchAsync(Guid.NewGuid(), Entity(), rows, null, null, CancellationToken.None));

        Assert.Empty(Directory.EnumerateFiles(_root, "*", SearchOption.AllDirectories));
        Assert.Equal(2, probe.Calls);
    }

    [Fact]
    public async Task A_failing_probe_fails_closed_without_writing()
    {
        var spool = new FileSpoolStore(_root, diskProbe: new ThrowingProbe(), reservedBytesForCommands: Reserve);

        var ex = await Assert.ThrowsAsync<EtlDiskReserveException>(() => spool.WriteBatchAsync(Guid.NewGuid(), Entity(), Rows(3), null, null, CancellationToken.None));

        Assert.IsType<IOException>(ex.InnerException);
        Assert.Empty(Directory.EnumerateFiles(_root, "*", SearchOption.AllDirectories));
    }

    [Fact]
    public async Task Batch_is_refused_before_any_file_when_headroom_would_cut_the_reserve()
    {
        var probe = new FakeProbe(Reserve + 10);
        var spool = new FileSpoolStore(_root, maxBatchCompressedBytes: 1_000, diskProbe: probe, reservedBytesForCommands: Reserve);

        await Assert.ThrowsAsync<EtlDiskReserveException>(() => spool.WriteBatchAsync(Guid.NewGuid(), Entity(), Rows(10), null, null, CancellationToken.None));

        Assert.Empty(Directory.EnumerateFiles(_root, "*", SearchOption.AllDirectories));
        Assert.Equal(1, probe.Calls);
    }

    [Fact]
    public async Task Writing_stops_and_leaves_no_file_when_free_space_drops_mid_batch()
    {
        // Plenty of space for the preflight, then the volume fills up while rows are written.
        var roomy = Reserve + EtlDiskAdmission.DefaultBatchHeadroomBytes * 2;
        var probe = new FakeProbe(roomy, roomy, Reserve - 1);
        var spool = new FileSpoolStore(_root, diskProbe: probe, reservedBytesForCommands: Reserve);

        await Assert.ThrowsAsync<EtlDiskReserveException>(() =>
            spool.WriteBatchAsync(Guid.NewGuid(), Entity(), Rows(FileSpoolStore.DiskCheckRowInterval * 3), null, null, CancellationToken.None));

        Assert.Empty(Directory.EnumerateFiles(_root, "*", SearchOption.AllDirectories));
        Assert.Equal(3, probe.Calls);
    }

    [Fact]
    public async Task Batch_is_written_when_the_reserve_stays_intact()
    {
        var probe = new FakeProbe(long.MaxValue / 2);
        var spool = new FileSpoolStore(_root, diskProbe: probe, reservedBytesForCommands: Reserve);

        var batch = await spool.WriteBatchAsync(Guid.NewGuid(), Entity(), Rows(FileSpoolStore.DiskCheckRowInterval * 2 + 1), null, null, CancellationToken.None);

        Assert.True(File.Exists(batch.FilePath));
        Assert.Equal(FileSpoolStore.DiskCheckRowInterval * 2 + 1, batch.RowCount);
        Assert.Equal(3, probe.Calls); // preflight + two in-write checks
    }

    [Fact]
    public async Task Spool_without_a_probe_keeps_its_previous_behaviour()
    {
        var spool = new FileSpoolStore(_root);

        var batch = await spool.WriteBatchAsync(Guid.NewGuid(), Entity(), Rows(3), null, null, CancellationToken.None);

        Assert.True(File.Exists(batch.FilePath));
    }

    [Fact]
    public void Negative_reserve_is_rejected() =>
        Assert.Throws<ArgumentOutOfRangeException>(() => new FileSpoolStore(_root, reservedBytesForCommands: -1));

    [Fact]
    public void Drive_probe_reports_free_space_for_a_path_that_does_not_exist_yet() =>
        Assert.True(new DriveInfoDiskSpaceProbe().GetAvailableFreeBytes(Path.Combine(_root, "not", "yet")) > 0);

    private static EtlEntityDefinition Entity() =>
        new("clients", "Catalog_Clients", "Ref_Key", "UpdatedAt", "DeletionMark", ["Ref_Key", "UpdatedAt", "DeletionMark"], "incremental", 500, 10);

    private static JsonElement[] Rows(int count) =>
        Enumerable.Range(0, count)
            .Select(i => JsonSerializer.SerializeToElement(new { Ref_Key = $"k{i}", UpdatedAt = "2026-09-26T00:00:00Z", DeletionMark = false }))
            .ToArray();

    private sealed class ThrowingProbe : IDiskSpaceProbe
    {
        public long GetAvailableFreeBytes(string path) => throw new IOException("The device is not ready.");
    }

    private sealed class FakeProbe(params long[] answers) : IDiskSpaceProbe
    {
        public int Calls { get; private set; }

        public long GetAvailableFreeBytes(string path)
        {
            var answer = answers[Math.Min(Calls, answers.Length - 1)];
            Calls++;
            return answer;
        }
    }
}
