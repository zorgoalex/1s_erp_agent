using System.Net;

namespace ErpOnecAgent.Application.Abstractions;

/// <summary>
/// A non-2xx ERP API response with its machine-readable <c>ApiError.code</c> (when the body
/// carried a well-formed one) and <c>Retry-After</c>. The body itself is never kept or logged.
/// Derives from <see cref="HttpRequestException"/>, so existing handling of ERP failures is
/// unchanged; callers that understand a specific agreed code can act on it.
/// </summary>
public sealed class ErpApiException(HttpStatusCode statusCode, string? apiErrorCode, TimeSpan? retryAfter)
    : HttpRequestException(
        $"ERP API returned {(int)statusCode}{(apiErrorCode is null ? string.Empty : " " + apiErrorCode)} (body withheld).",
        null,
        statusCode)
{
    /// <summary>Agreed ERP error codes (agent-bridge, spec 1.0-draft).</summary>
    public const string ResultConflict = "RESULT_CONFLICT";
    public const string BatchPayloadInvalid = "BATCH_PAYLOAD_INVALID";
    public const string BatchNotStoredRetryable = "BATCH_NOT_STORED_RETRYABLE";
    public const string SourceIdentityMismatch = "SOURCE_IDENTITY_MISMATCH";
    public const string RunGenerationClosed = "RUN_GENERATION_CLOSED";

    public string? ApiErrorCode { get; } = apiErrorCode;
    public TimeSpan? RetryAfter { get; } = retryAfter;

    public bool Is(HttpStatusCode status, string code) =>
        StatusCode == status && string.Equals(ApiErrorCode, code, StringComparison.Ordinal);
}
