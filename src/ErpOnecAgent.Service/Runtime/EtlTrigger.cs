using System.Threading.Channels;

namespace ErpOnecAgent.Service.Runtime;

public sealed record EtlTriggerRequest(string Mode, IReadOnlyList<string>? Entities);

public sealed class EtlTrigger
{
    private readonly Channel<EtlTriggerRequest> _channel = Channel.CreateBounded<EtlTriggerRequest>(new BoundedChannelOptions(16)
    {
        FullMode = BoundedChannelFullMode.DropWrite,
        SingleReader = true,
        SingleWriter = false
    });

    public bool TryWrite(EtlTriggerRequest request) => _channel.Writer.TryWrite(request);
    public ValueTask<EtlTriggerRequest> ReadAsync(CancellationToken cancellationToken) => _channel.Reader.ReadAsync(cancellationToken);
}

