using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Infrastructure.ErpApi;
using ErpOnecAgent.Service.Workers.Etl;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>The completion claim must outlast one call on the ETL transfer channel, or it could expire mid-call and be re-sent.</summary>
public sealed class EtlCompletionClaimHoldTests
{
    [Fact]
    public void The_completion_claim_hold_outlasts_one_transfer_call()
    {
        Assert.Equal(TimeSpan.FromMinutes(5), EtlCompletionWorker.ClaimHoldFor(null));
        var longest = new ErpOptions { TransferTimeoutSeconds = 3600 };
        Assert.True(EtlCompletionWorker.ClaimHoldFor(longest) > ErpClientRegistration.TransferHttpClientTimeout(longest));
        Assert.True(EtlCompletionWorker.ClaimHoldFor(new ErpOptions()) > ErpClientRegistration.TransferHttpClientTimeout(new ErpOptions()));
    }
}
