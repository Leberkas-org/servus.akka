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
    private static readonly System.Collections.Concurrent.ConcurrentStack<TransportData> Pool = new();

    public TransportBuffer Buffer { get; private set; } = null!;

    public static TransportData Rent(TransportBuffer buffer)
    {
        if (!Pool.TryPop(out var item))
        {
            item = new TransportData();
        }

        item.Buffer = buffer;
        return item;
    }

    public void Return()
    {
        Buffer = null!;
        if (Pool.Count < 256)
        {
            Pool.Push(this);
        }
    }
}

public sealed record MultiplexedData(TransportBuffer Buffer, StreamTarget StreamId) : ITransportOutbound, ITransportInbound;
