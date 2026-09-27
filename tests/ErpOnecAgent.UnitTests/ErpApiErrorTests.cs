using System.Net;
using System.Text;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Infrastructure.ErpApi;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.UnitTests;

/// <summary>
/// E2: a non-2xx ERP answer surfaces the agreed ApiError.code and Retry-After, reading at most
/// 4 KiB of the body and never putting the body into the exception.
/// </summary>
public sealed class ErpApiErrorTests
{
    [Fact]
    public async Task The_error_code_and_retry_after_are_extracted()
    {
        var client = Client(HttpStatusCode.ServiceUnavailable, "{\"code\":\"BATCH_NOT_STORED_RETRYABLE\",\"message\":\"secret ERP detail\",\"retryable\":true}", retryAfterSeconds: 30);

        var ex = await Assert.ThrowsAsync<ErpApiException>(() => client.AcknowledgeResultAsync(Guid.NewGuid(), "{}", CancellationToken.None));

        Assert.Equal(HttpStatusCode.ServiceUnavailable, ex.StatusCode);
        Assert.Equal("BATCH_NOT_STORED_RETRYABLE", ex.ApiErrorCode);
        Assert.Equal(TimeSpan.FromSeconds(30), ex.RetryAfter);
        Assert.True(ex.Is(HttpStatusCode.ServiceUnavailable, ErpApiException.BatchNotStoredRetryable));
        Assert.DoesNotContain("secret", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("not json")]
    [InlineData("{\"code\":\"lower_case\"}")]
    [InlineData("{\"code\":\"HAS SPACE\"}")]
    [InlineData("{\"code\":42}")]
    [InlineData("[\"RESULT_CONFLICT\"]")]
    [InlineData("")]
    public async Task A_malformed_or_missing_code_is_null_and_still_an_http_failure(string body)
    {
        var client = Client(HttpStatusCode.Conflict, body);

        var ex = await Assert.ThrowsAsync<ErpApiException>(() => client.AcknowledgeResultAsync(Guid.NewGuid(), "{}", CancellationToken.None));

        Assert.Null(ex.ApiErrorCode);
        Assert.IsAssignableFrom<HttpRequestException>(ex);
        Assert.False(ex.Is(HttpStatusCode.Conflict, ErpApiException.ResultConflict));
    }

    [Fact]
    public async Task A_code_at_the_start_of_a_long_body_is_read_from_the_prefix()
    {
        var body = "{\"code\":\"RESULT_CONFLICT\",\"message\":\"" + new string('x', 5000) + "\"}";
        var client = Client(HttpStatusCode.Conflict, body);

        var ex = await Assert.ThrowsAsync<ErpApiException>(() => client.AcknowledgeResultAsync(Guid.NewGuid(), "{}", CancellationToken.None));

        Assert.Equal("RESULT_CONFLICT", ex.ApiErrorCode);
    }

    [Theory]
    [InlineData("{\"details\":{\"code\":\"NESTED\"},\"code\":\"TOP_LEVEL\"}", "TOP_LEVEL")]
    [InlineData("{\"list\":[1,2,{\"code\":\"X\"}],\"code\":\"AFTER_ARRAY\"}", "AFTER_ARRAY")]
    [InlineData("{\"message\":\"no code here\"}", null)]
    public void The_code_is_the_first_top_level_string_property(string json, string? expected) =>
        Assert.Equal(expected, ErpClient.ExtractCode(System.Text.Encoding.UTF8.GetBytes(json), isFinalBlock: true));

    [Fact]
    public async Task A_code_beyond_the_first_4_kib_is_not_read()
    {
        var body = "{\"pad\":\"" + new string('x', 5000) + "\",\"code\":\"RESULT_CONFLICT\"}";
        var client = Client(HttpStatusCode.Conflict, body);

        var ex = await Assert.ThrowsAsync<ErpApiException>(() => client.AcknowledgeResultAsync(Guid.NewGuid(), "{}", CancellationToken.None));

        Assert.Null(ex.ApiErrorCode);
    }

    [Fact]
    public async Task The_same_code_under_a_different_status_does_not_match()
    {
        var client = Client(HttpStatusCode.InternalServerError, "{\"code\":\"RESULT_CONFLICT\"}");

        var ex = await Assert.ThrowsAsync<ErpApiException>(() => client.AcknowledgeResultAsync(Guid.NewGuid(), "{}", CancellationToken.None));

        Assert.False(ex.Is(HttpStatusCode.Conflict, ErpApiException.ResultConflict));
    }

    private static ErpClient Client(HttpStatusCode status, string body, int? retryAfterSeconds = null)
    {
        var http = new HttpClient(new StaticHandler(status, body, retryAfterSeconds)) { BaseAddress = new Uri("https://erp.test/api/integration/1c-agents/v1/") };
        return new ErpClient(http, Options.Create(new AgentOptions { AgentId = "agent-1", SiteId = "site-1" }));
    }

    private sealed class StaticHandler(HttpStatusCode status, string body, int? retryAfterSeconds) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var response = new HttpResponseMessage(status) { Content = new StringContent(body, Encoding.UTF8, "application/json") };
            if (retryAfterSeconds is { } seconds) response.Headers.RetryAfter = new System.Net.Http.Headers.RetryConditionHeaderValue(TimeSpan.FromSeconds(seconds));
            return Task.FromResult(response);
        }
    }
}
