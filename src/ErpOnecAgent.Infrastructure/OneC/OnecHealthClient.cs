using System.Net.Http.Json;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Contracts.OneC;

namespace ErpOnecAgent.Infrastructure.OneC;

public sealed class OnecHealthClient(HttpClient httpClient, OnecAuthentication authentication) : IOnecHealthClient
{
    public async Task<OnecHealthResponse?> CheckAsync(CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, "health");
        await authentication.ApplyAsync(request, cancellationToken).ConfigureAwait(false);
        using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode) return null;
        return await response.Content.ReadFromJsonAsync<OnecHealthResponse>(new JsonSerializerOptions(JsonSerializerDefaults.Web), cancellationToken).ConfigureAwait(false);
    }
}

