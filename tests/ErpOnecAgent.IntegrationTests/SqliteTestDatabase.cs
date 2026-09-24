using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Data.Sqlite;

namespace ErpOnecAgent.IntegrationTests;

internal sealed class SqliteTestDatabase
{
    private readonly List<SqliteConnectionFactory> _factories = [];

    public string Root { get; } = Path.Combine(Path.GetTempPath(), "ErpOnecAgentTests", Guid.NewGuid().ToString("N"));

    public SqliteConnectionFactory CreateFactory(string relativePath)
    {
        var factory = new SqliteConnectionFactory(Path.Combine(Root, relativePath));
        _factories.Add(factory);
        return factory;
    }

    public static async Task ClearPoolAsync(SqliteConnectionFactory factory)
    {
        await using var connection = await factory.OpenAsync(CancellationToken.None);
        SqliteConnection.ClearPool(connection);
    }

    public async Task DisposeAsync()
    {
        foreach (var factory in _factories)
        {
            await ClearPoolAsync(factory);
        }

        if (Directory.Exists(Root)) Directory.Delete(Root, recursive: true);
    }
}
