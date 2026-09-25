using System.IO.Compression;
using System.Security.Cryptography;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Etl;
using ErpOnecAgent.Domain.Etl;

namespace ErpOnecAgent.Infrastructure.Spool;

// A08: with a disk probe, a batch is started only while the data volume keeps the command
// reserve plus batch headroom free, and writing stops as soon as the reserve would be cut.
// A real disk-full IOException is surfaced as EtlDiskReserveException. In every case the
// temporary file is removed and no ready file or batch is produced.
public sealed class FileSpoolStore(string spoolRoot, long maxBatchCompressedBytes = long.MaxValue, long maxSpoolBytes = long.MaxValue,
    IDiskSpaceProbe? diskProbe = null, long reservedBytesForCommands = 0) : ISpoolStore
{
    internal const int DiskCheckRowInterval = 256;
    private readonly long _reservedBytes = reservedBytesForCommands >= 0 ? reservedBytesForCommands : throw new ArgumentOutOfRangeException(nameof(reservedBytesForCommands));
    private readonly string _root = Path.GetFullPath(spoolRoot);
    private readonly long _maxBatchCompressedBytes = maxBatchCompressedBytes > 0 ? maxBatchCompressedBytes : throw new ArgumentOutOfRangeException(nameof(maxBatchCompressedBytes));
    private readonly long _maxSpoolBytes = maxSpoolBytes > 0 ? maxSpoolBytes : throw new ArgumentOutOfRangeException(nameof(maxSpoolBytes));

    public async Task<EtlBatch> WriteBatchAsync(Guid runId, EtlEntityDefinition entity, IReadOnlyList<JsonElement> rows, EtlCursor? watermarkFrom, EtlCursor? watermarkTo, CancellationToken cancellationToken)
    {
        var batchId = Guid.NewGuid();
        var creating = EnsureSubdirectory("creating");
        var ready = EnsureSubdirectory("ready");
        var fileName = $"{Sanitize(entity.EntityCode)}-{runId:D}-{batchId:D}.ndjson.gz";
        var temporaryPath = Path.Combine(creating, fileName + ".tmp");
        var readyPath = Path.Combine(ready, fileName);
        var existingSpoolBytes = await GetSizeAsync(cancellationToken).ConfigureAwait(false);
        if (existingSpoolBytes >= _maxSpoolBytes) throw new IOException($"ETL spool limit reached: {existingSpoolBytes} bytes of {_maxSpoolBytes} bytes.");
        EnsureDiskReserve(Math.Min(_maxBatchCompressedBytes, EtlDiskAdmission.DefaultBatchHeadroomBytes), "before the batch file is created");
        long uncompressed = 0;
        try
        {
            await using (var file = new FileStream(temporaryPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 65_536, FileOptions.Asynchronous | FileOptions.WriteThrough))
            await using (var gzip = new GZipStream(file, CompressionLevel.SmallestSize, leaveOpen: false))
            {
                var written = 0;
                long sinceCheck = 0;
                foreach (var row in rows)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var bytes = JsonSerializer.SerializeToUtf8Bytes(BuildEnvelope(row, entity));
                    // The reserve is re-checked every DiskCheckRowInterval rows AND every
                    // CheckIntervalBytes of payload, and a single row larger than the interval is
                    // checked against its own size before it is written (compressed <= raw).
                    written++;
                    if (bytes.Length >= EtlDiskAdmission.CheckIntervalBytes) EnsureDiskReserve(bytes.Length, "before a large row is written");
                    else if (written % DiskCheckRowInterval == 0 || sinceCheck + bytes.Length >= EtlDiskAdmission.CheckIntervalBytes)
                    {
                        EnsureDiskReserve(EtlDiskAdmission.CheckIntervalBytes, "while writing the batch file");
                        sinceCheck = 0;
                    }
                    sinceCheck += bytes.Length + 1;
                    if (file.Length > _maxBatchCompressedBytes) throw new IOException($"Compressed ETL batch exceeds configured limit of {_maxBatchCompressedBytes} bytes.");
                    await gzip.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
                    await gzip.WriteAsync("\n"u8.ToArray(), cancellationToken).ConfigureAwait(false);
                    uncompressed += bytes.Length + 1;
                }
            }
            var temporaryLength = new FileInfo(temporaryPath).Length;
            if (temporaryLength > _maxBatchCompressedBytes) throw new IOException($"Compressed ETL batch exceeds configured limit of {_maxBatchCompressedBytes} bytes.");
            if (existingSpoolBytes + temporaryLength > _maxSpoolBytes) throw new IOException($"ETL batch would exceed configured spool limit of {_maxSpoolBytes} bytes.");
            File.Move(temporaryPath, readyPath, overwrite: false);
            await using var verify = new FileStream(readyPath, FileMode.Open, FileAccess.Read, FileShare.Read, 65_536, FileOptions.Asynchronous | FileOptions.SequentialScan);
            var compressedLength = verify.Length;
            var hash = Convert.ToBase64String(await SHA256.HashDataAsync(verify, cancellationToken).ConfigureAwait(false));
            return new EtlBatch(batchId, runId, entity.EntityCode, entity.SchemaVersion, readyPath, EtlBatchStatus.Ready, rows.Count, watermarkFrom, watermarkTo, hash, compressedLength, uncompressed, 0, DateTimeOffset.UtcNow);
        }
        catch (IOException ex) when (ex is not EtlDiskReserveException && EtlDiskAdmission.IsDiskFull(ex))
        {
            TryDelete(temporaryPath);
            throw new EtlDiskReserveException("The disk is full; ETL spool writing stopped.", ex);
        }
        catch
        {
            // A failed cleanup never replaces the original error; the leftover .tmp is
            // quarantined by QuarantineTemporaryFilesAsync at the next start.
            TryDelete(temporaryPath);
            throw;
        }
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path)) File.Delete(path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            // Swallowed deliberately: see the caller.
        }
    }

    private void EnsureDiskReserve(long projectedWriteBytes, string stage)
    {
        if (diskProbe is null) return;
        long free;
        try
        {
            free = diskProbe.GetAvailableFreeBytes(_root);
        }
        catch (Exception ex) when (ex is IOException or ArgumentException or UnauthorizedAccessException or InvalidOperationException)
        {
            // An unknown free-space state fails closed: nothing is written.
            throw new EtlDiskReserveException($"ETL spool writing refused {stage}: free disk space could not be determined.", ex);
        }
        if (!EtlDiskAdmission.Allows(free, _reservedBytes, projectedWriteBytes))
            throw new EtlDiskReserveException($"ETL spool writing refused {stage}: {free} bytes free would not keep the {_reservedBytes}-byte command reserve.");
    }

    public Task<Stream> OpenReadAsync(EtlBatch batch, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Stream stream = new FileStream(EnsureInsideRoot(batch.FilePath), FileMode.Open, FileAccess.Read, FileShare.Read, 65_536, FileOptions.Asynchronous | FileOptions.SequentialScan);
        return Task.FromResult(stream);
    }

    public Task<long> GetSizeAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!Directory.Exists(_root)) return Task.FromResult(0L);
        return Task.FromResult(Directory.EnumerateFiles(_root, "*", SearchOption.AllDirectories).Sum(static path => new FileInfo(path).Length));
    }

    public Task QuarantineTemporaryFilesAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var creating = EnsureSubdirectory("creating"); var quarantine = EnsureSubdirectory("quarantine");
        foreach (var path in Directory.EnumerateFiles(creating, "*.tmp", SearchOption.TopDirectoryOnly))
        {
            var destination = Path.Combine(quarantine, Path.GetFileName(path) + "." + DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(System.Globalization.CultureInfo.InvariantCulture));
            File.Move(path, destination, overwrite: false);
        }
        return Task.CompletedTask;
    }

    public Task DeleteAcknowledgedAsync(EtlBatch batch, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var path = EnsureInsideRoot(batch.FilePath);
        if (File.Exists(path)) File.Delete(path);
        return Task.CompletedTask;
    }

    private static object BuildEnvelope(JsonElement row, EtlEntityDefinition entity)
    {
        var sourceId = entity.SourceIdFrom(row);
        JsonElement? updated = null;
        if (entity.UpdatedAtField is not null && row.TryGetProperty(entity.UpdatedAtField, out var updatedValue)) updated = updatedValue.Clone();
        var deleted = entity.DeletedField is not null && row.TryGetProperty(entity.DeletedField, out var deletedValue) && deletedValue.ValueKind == JsonValueKind.True;
        return new { sourceId, sourceUpdatedAt = updated, deleted, data = row.Clone() };
    }

    private string EnsureSubdirectory(string name) { var directory = Path.Combine(_root, name); Directory.CreateDirectory(directory); return directory; }
    private string EnsureInsideRoot(string path)
    {
        var full = Path.GetFullPath(path); var root = _root.TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar;
        if (!full.StartsWith(root, StringComparison.OrdinalIgnoreCase)) throw new InvalidOperationException("Spool path escapes configured root.");
        return full;
    }

    public static string Sanitize(string value)
    {
        var invalid = Path.GetInvalidFileNameChars().ToHashSet();
        var result = new string(value.Select(c => invalid.Contains(c) || c is '/' or '\\' ? '_' : c).ToArray()).Trim();
        return string.IsNullOrWhiteSpace(result) ? "entity" : result;
    }
}
