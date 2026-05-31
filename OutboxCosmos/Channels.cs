using System.Threading.Channels;

namespace OutboxCosmos;

public interface IOutboxChannel
{
    ChannelReader<OutboxDispatchRequest> Reader { get; }
    ChannelWriter<OutboxDispatchRequest> Writer { get; }        
}

public sealed class OutboxChannel : IOutboxChannel
{
    private readonly Channel<OutboxDispatchRequest> _channel;

    public OutboxChannel()
    {
        var options = new BoundedChannelOptions(capacity:500)
        {
            SingleReader = false,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait,
            AllowSynchronousContinuations = false
        };

        _channel = Channel.CreateBounded<OutboxDispatchRequest>(options);
    }

    public ChannelReader<OutboxDispatchRequest> Reader => _channel.Reader;

    public ChannelWriter<OutboxDispatchRequest> Writer => _channel.Writer;
}
