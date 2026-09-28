using System.Net;
using System.Text;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.OneC;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.UnitTests;

/// <summary>stock_balances: the register Balance() virtual table with explicit dimensions as the entity path.</summary>
public sealed class BalanceEntityPathTests
{
    private const string StockPath = "AccumulationRegister_ЗапасыНаСкладах/Balance(Dimensions='Организация,Номенклатура,Характеристика,Партия,СтруктурнаяЕдиница,Ячейка')";

    private static readonly EtlEntityDefinition Stock = new("stock_balances", StockPath, "Номенклатура_Key", null, null,
        ["Организация_Key", "Номенклатура_Key", "Характеристика_Key", "Партия_Key", "СтруктурнаяЕдиница_Key", "Ячейка_Key", "КоличествоBalance"],
        "manual_only", 500, 0,
        KeyFields: ["Организация_Key", "Номенклатура_Key", "Характеристика_Key", "Партия_Key", "СтруктурнаяЕдиница_Key", "Ячейка_Key"]);

    [Theory]
    [InlineData("Catalog_Номенклатура", true)]
    [InlineData(StockPath, true)]
    [InlineData("AccumulationRegister_ЗапасыНаСкладах/Balance(Dimensions='Номенклатура')", true)]
    [InlineData("AccumulationRegister_ЗапасыНаСкладах/Balance()", false)]
    [InlineData("AccumulationRegister_ЗапасыНаСкладах/Balance(Period=datetime'2026-01-01')", false)]
    [InlineData("AccumulationRegister_ЗапасыНаСкладах/Turnovers(Dimensions='Номенклатура')", false)]
    [InlineData("AccumulationRegister_ЗапасыНаСкладах/Balance(Dimensions='Номенклатура')?$top=1", false)]
    [InlineData("AccumulationRegister_ЗапасыНаСкладах/Balance(Dimensions='Номенклатура'),Catalog_X", false)]
    [InlineData("Catalog_X/../Catalog_Y", false)]
    [InlineData("Catalog X", false)]
    public void Only_plain_sets_and_the_dimensioned_balance_are_safe_paths(string path, bool safe) =>
        Assert.Equal(safe, OnecODataClient.IsSafeODataPath(path));

    [Fact]
    public async Task The_balance_is_read_counted_and_key_passed_with_its_dimensions()
    {
        var handler = new Handler();
        var client = new OnecODataClient(new HttpClient(handler) { BaseAddress = new Uri("http://127.0.0.1/test/odata/standard.odata/") },
            new OnecAuthentication(new StaticSecretStore(), Options.Create(new OnecOptions { CredentialSecretName = "test" })));

        await foreach (var _ in client.ReadEntityAsync(Stock, null, new EtlCursor(DateTimeOffset.UtcNow, null), full: true, CancellationToken.None)) { }
        await client.CountAsync(Stock, CancellationToken.None);
        await foreach (var _ in client.ReadKeysAsync(Stock, CancellationToken.None)) { }

        var paths = handler.Requests.Select(static r => Uri.UnescapeDataString(r.RequestUri!.AbsolutePath)).ToArray();
        Assert.Equal(3, paths.Length);
        Assert.Equal("/test/odata/standard.odata/" + StockPath, paths[0]);
        Assert.Equal("/test/odata/standard.odata/" + StockPath + "/$count", paths[1]);
        Assert.Equal("/test/odata/standard.odata/" + StockPath, paths[2]);
        Assert.DoesNotContain("$filter", Uri.UnescapeDataString(handler.Requests[0].RequestUri!.Query), StringComparison.Ordinal);
    }

    [Fact]
    public async Task An_unsafe_path_is_refused_before_any_request()
    {
        var handler = new Handler();
        var client = new OnecODataClient(new HttpClient(handler) { BaseAddress = new Uri("http://127.0.0.1/test/odata/standard.odata/") },
            new OnecAuthentication(new StaticSecretStore(), Options.Create(new OnecOptions { CredentialSecretName = "test" })));

        await Assert.ThrowsAsync<InvalidDataException>(() => client.CountAsync(Stock with { ODataPath = "AccumulationRegister_ЗапасыНаСкладах/Balance()" }, CancellationToken.None));
        Assert.Empty(handler.Requests);
    }

    private sealed class Handler : HttpMessageHandler
    {
        public List<HttpRequestMessage> Requests { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Requests.Add(request);
            var body = request.RequestUri!.AbsolutePath.EndsWith("/%24count", StringComparison.Ordinal) || request.RequestUri.AbsolutePath.EndsWith("/$count", StringComparison.Ordinal) ? "0" : "{\"value\":[]}";
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(body, Encoding.UTF8, "application/json") });
        }
    }

    private sealed class StaticSecretStore : ISecretStore
    {
        public Task<string?> ReadAsync(string name, CancellationToken cancellationToken) => Task.FromResult<string?>("user:password");
        public Task SaveAsync(string name, string value, CancellationToken cancellationToken) => throw new NotSupportedException();
    }
}
