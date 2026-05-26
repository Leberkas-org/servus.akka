namespace Servus.Akka.Transport;

public abstract record ListenerOptions
{
    public required string Host { get; init; }
    public required ushort Port { get; init; }
    public int Backlog { get; init; } = int.MaxValue;
    public int? SocketSendBufferSize { get; init; }
    public int? SocketReceiveBufferSize { get; init; }
}
