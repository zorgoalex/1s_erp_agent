using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using Microsoft.Extensions.Options;

namespace ErpOnecAgent.Infrastructure.OneC;

public sealed class OnecAuthentication(ISecretStore secrets, IOptions<OnecOptions> options)
{
    public async Task ApplyAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var secret = await secrets.ReadAsync(options.Value.CredentialSecretName, cancellationToken).ConfigureAwait(false);
        if (string.IsNullOrWhiteSpace(secret)) throw new InvalidOperationException($"1C credential secret '{options.Value.CredentialSecretName}' is not configured.");
        string user; string password;
        if (secret.TrimStart().StartsWith('{'))
        {
            using var document = JsonDocument.Parse(secret);
            user = document.RootElement.GetProperty("username").GetString() ?? string.Empty;
            password = document.RootElement.GetProperty("password").GetString() ?? string.Empty;
        }
        else
        {
            var separator = secret.IndexOf(':', StringComparison.Ordinal);
            if (separator <= 0) throw new InvalidDataException("1C secret must be JSON with username/password or username:password.");
            user = secret[..separator]; password = secret[(separator + 1)..];
        }
        request.Headers.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes(user + ":" + password)));
    }
}

