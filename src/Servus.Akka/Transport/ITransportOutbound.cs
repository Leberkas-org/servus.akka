namespace Servus.Akka.Transport;

public interface ITransportOutbound;

public sealed record ConnectTransport(TransportOptions Options) : ITransportOutbound;

public sealed record DisconnectTransport(DisconnectReason Reason) : ITransportOutbound;

public sealed record OpenStream(StreamTarget StreamId, StreamDirection Direction) : ITransportOutbound;

public sealed record CloseStream(StreamTarget StreamId) : ITransportOutbound;

public sealed record CompleteWrites(StreamTarget StreamId) : ITransportOutbound;

public sealed record ResetStream(StreamTarget StreamId, long ErrorCode = 0) : ITransportOutbound;

public sealed record TransportData(TransportBuffer Buffer) : ITransportOutbound, ITransportInbound;

public sealed record MultiplexedData(TransportBuffer Buffer, StreamTarget StreamId) : ITransportOutbound, ITransportInbound;
