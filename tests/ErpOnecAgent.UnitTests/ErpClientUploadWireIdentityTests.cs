using System.Net;
using System.Text;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.ErpApi;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.UnitTests;

/// <summary>
/// O2 wire-identity proof (design §11): every send of the same durable batch emits
/// byte-identical payload identity on the wire — the batch-scoped dedup surface
/// (Idempotency-Key=batch_id, X-Content-SHA256, X-Batch-Id, X-Run-Id, X-Entity,
/// X-Schema-Version, X-Row-Count, Content-Type, Content-Encoding, and the exact body
/// bytes) is stable across attempts, so a bounded proven-unsent re-send after
/// 'precheck_failed' presents the SAME remote identity. Per-request transport
/// correlation headers (X-Request-Id/X-Correlation-Id) are fresh per call and are
/// deliberately excluded — they are not payload identity. ErpClient is untouched.
/// </summary>
public sealed class ErpClientUploadWireIdentityTests
{
    [Fact]
    public async Task Resend_of_the_same_batch_emits_identical_dedup_wire_identity()
    {
        var captured = new List<CapturedRequest>();
        var handler = new CapturingHandler(captured);
        using var http = new HttpClient(handler) { BaseAddress = new Uri("https://erp.test/") };
        var client = new ErpClient(http, Options.Create(new AgentOptions { AgentId = "agent-1", SiteId = "site-1" }));
        var batch = new EtlBatch(
            Guid.Parse("11111111-1111-1111-1111-111111111111"),
            Guid.Parse("22222222-2222-2222-2222-222222222222"),
            "clients", 1, "spool/b.gz", EtlBatchStatus.Ready, 5,
            null, null, "sha256-batch-bytes", 100, 400, 0, DateTimeOffset.UtcNow);
        var payload = Encoding.UTF8.GetBytes("gzipped-batch-body");

        // Two admissions of the SAME batch (the O2 retry shape under a fresh attempt
        // id — the wire carries only the durable batch identity, never the attempt).
        await client.UploadBatchAsync(batch, new MemoryStream(payload), CancellationToken.None);
        await client.UploadBatchAsync(batch, new MemoryStream(payload), CancellationToken.None);

        Assert.Equal(2, captured.Count);
        var first = captured[0];
        var second = captured[1];
        foreach (var header in new[] { "Idempotency-Key", "X-Content-SHA256", "X-Batch-Id", "X-Run-Id", "X-Entity", "X-Schema-Version", "X-Row-Count" })
        {
            Assert.Equal(first.Headers[header], second.Headers[header]);
        }
        Assert.Equal(batch.BatchId.ToString("D"), first.Headers["Idempotency-Key"]);
        Assert.Equal(first.ContentType, second.ContentType);
        Assert.Equal(first.ContentEncoding, second.ContentEncoding);
        Assert.Equal(first.Body, second.Body);
        Assert.Equal(first.Path, second.Path);
    }

    private sealed record CapturedRequest(
        string Path,
        IReadOnlyDictionary<string, string> Headers,
        string ContentType,
        string ContentEncoding,
        byte[] Body);

    private sealed class CapturingHandler(List<CapturedRequest> captured) : HttpMessageHandler
    {
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var body = request.Content is null ? [] : await request.Content.ReadAsByteArrayAsync(cancellationToken);
            captured.Add(new CapturedRequest(
                request.RequestUri!.AbsolutePath,
                request.Headers.ToDictionary(h => h.Key, h => string.Join(",", h.Value), StringComparer.OrdinalIgnoreCase),
                request.Content?.Headers.ContentType?.ToString() ?? "",
                string.Join(",", request.Content?.Headers.ContentEncoding ?? []),
                body));
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(
                    """{"batchId":"11111111-1111-1111-1111-111111111111","status":"acknowledged","rowsAccepted":5,"checksumValid":true,"acknowledgedAtUtc":"2026-09-25T00:00:00Z"}""",
                    Encoding.UTF8, "application/json")
            };
        }
    }
}
