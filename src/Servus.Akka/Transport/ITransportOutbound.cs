namespace Servus.Akka.Transport;

public interface ITransportOutbound;

public sealed record ConnectTransport(TransportOptions Options) : ITransportOutbound;

public sealed record DisconnectTransport(DisconnectReason Reason) : ITransportOutbound;

public sealed record OpenStream(StreamTarget StreamId, StreamDirection Direction) : ITransportOutbound;

public sealed record CloseStream(StreamTarget StreamId) : ITransportOutbound;

public sealed record CompleteWrites(StreamTarget StreamId) : ITransportOutbound;

public sealed record ResetStream(StreamTarget StreamId, long ErrorCode = 0) : ITransportOutbound;

public sealed class TransportData : ITransportOutbound, ITransportInbound
{
    private static readonly ObjectPool<TransportData> Pool = new(256);

    public TransportBuffer Buffer { get; private set; } = null!;

    public static TransportData Rent(TransportBuffer buffer)
    {
        if (!Pool.TryRent(out var item))
        {
            item = new TransportData();
        }

        item.Buffer = buffer;
        return item;
    }

    public void Return()
    {
        Buffer = null!;
        Pool.Return(this);
    }
}

public sealed class MultiplexedData : ITransportOutbound, ITransportInbound
{
    private static readonly ObjectPool<MultiplexedData> Pool = new(256);

    public TransportBuffer Buffer { get; private set; } = null!;
    public StreamTarget StreamId { get; private set; }

    public static MultiplexedData Rent(TransportBuffer buffer, StreamTarget streamId)
    {
        if (!Pool.TryRent(out var item))
        {
            item = new MultiplexedData();
        }

        item.Buffer = buffer;
        item.StreamId = streamId;
        return item;
    }

    public void Return()
    {
        Buffer = null!;
        StreamId = default;
        Pool.Return(this);
    }
}
