using System.Collections.Concurrent;
using System.IO.Compression;
using System.Security.Cryptography;
using System.Text.Json;
using ErpOnecAgent.Contracts.Erp;
using ErpOnecAgent.Contracts.OneC;

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls(builder.Configuration["urls"] ?? "http://127.0.0.1:5099");
var app = builder.Build();
var commands = new ConcurrentQueue<JsonElement>();
var received = new ConcurrentDictionary<Guid, string>();
var results = new ConcurrentDictionary<Guid, JsonElement>();
var onecJournal = new ConcurrentDictionary<Guid, (string Hash, OnecCommandResponse Result)>();
var odata = new ConcurrentDictionary<string, IReadOnlyList<JsonElement>>(StringComparer.Ordinal);
var mockCommandTypes = new[] { "create_customer_order" };
var mockVersions = new[] { 1 };

app.MapPost("/mock/commands", (JsonElement command) => { commands.Enqueue(command.Clone()); return Results.Accepted(); });
app.MapGet("/mock/results/{id:guid}", (Guid id) => results.TryGetValue(id, out var result) ? Results.Json(result) : Results.NotFound());
app.MapPost("/mock/odata/{entity}", (string entity, JsonElement rows) =>
{
    if (rows.ValueKind != JsonValueKind.Array) return Results.BadRequest();
    odata[entity] = rows.EnumerateArray().Select(static row => row.Clone()).ToArray(); return Results.Accepted();
});

var erp = "/api/integration/1c-agents/v1";
app.MapPost(erp + "/session/start", (SessionStartRequest request) => Results.Json(new SessionStartResponse(Guid.NewGuid(), DateTimeOffset.UtcNow, true, "1.0.0", 1, false)));
app.MapPost(erp + "/commands/lease", (LeaseRequest request) =>
    commands.TryDequeue(out var command) ? Results.Json(new LeaseResponse(true, Guid.NewGuid(), DateTimeOffset.UtcNow.AddMinutes(1), command)) : Results.Json(new LeaseResponse(false, null, null, null)));
app.MapPost(erp + "/commands/{id:guid}/received", (Guid id, CommandReceivedRequest request) => { received[id] = request.PayloadHash; return Results.NoContent(); });
app.MapPut(erp + "/commands/{id:guid}/result", (Guid id, JsonElement result) => { results[id] = result.Clone(); return Results.NoContent(); });
app.MapPost(erp + "/heartbeat", (HeartbeatRequest request) => Results.NoContent());
app.MapGet(erp + "/configuration", () => Results.StatusCode(StatusCodes.Status304NotModified));
app.MapPost(erp + "/etl/batches", async (HttpRequest request, CancellationToken cancellationToken) =>
{
    await using var buffer = new MemoryStream(); await request.Body.CopyToAsync(buffer, cancellationToken); var compressed = buffer.ToArray();
    var expected = request.Headers["X-Content-SHA256"].ToString(); var valid = string.Equals(expected, Convert.ToBase64String(SHA256.HashData(compressed)), StringComparison.Ordinal);
    buffer.Position = 0; var rows = 0;
    await using (var gzip = new GZipStream(buffer, CompressionMode.Decompress, leaveOpen: true)) using (var reader = new StreamReader(gzip)) while (await reader.ReadLineAsync(cancellationToken) is not null) rows++;
    var batchId = Guid.Parse(request.Headers["X-Batch-Id"].ToString());
    return Results.Json(new BatchAcknowledgement(batchId, "acknowledged", rows, valid, DateTimeOffset.UtcNow));
});
app.MapPost(erp + "/etl/runs/{id:guid}/complete", (Guid id) => Results.NoContent());

var onec = "/erp-integration/v1";
app.MapGet(onec + "/health", () => Results.Json(new OnecHealthResponse("ok", "1C", "mock", "mock", DateTimeOffset.UtcNow)));
app.MapGet(onec + "/capabilities", () => Results.Json(new { commandTypes = mockCommandTypes, versions = mockVersions }));
app.MapPost(onec + "/commands/execute", (ExecuteCommandRequest request) =>
{
    if (onecJournal.TryGetValue(request.CommandId, out var existing)) return string.Equals(existing.Hash, request.PayloadHash, StringComparison.Ordinal) ? Results.Json(existing.Result, statusCode: 200) : Results.Conflict(new { error = new { code = "COMMAND_PAYLOAD_CONFLICT", message = "Hash differs", retryable = false } });
    var document = JsonSerializer.SerializeToElement(new { type = "CustomerOrder", @ref = Guid.NewGuid().ToString("D"), number = Random.Shared.Next(1, 999999).ToString("D8", System.Globalization.CultureInfo.InvariantCulture), date = DateTimeOffset.UtcNow, posted = false });
    var response = new OnecCommandResponse(request.CommandId, "succeeded", document, null, [], 1); onecJournal[request.CommandId] = (request.PayloadHash, response);
    return Results.Json(response, statusCode: 201);
});
app.MapGet(onec + "/commands/{id:guid}", (Guid id) => onecJournal.TryGetValue(id, out var entry) ? Results.Json(entry.Result) : Results.NotFound());
app.MapGet("/odata/{entity}", (string entity) => Results.Json(new { value = odata.TryGetValue(entity, out var rows) ? rows : [] }));

app.Run();

public partial class Program;
