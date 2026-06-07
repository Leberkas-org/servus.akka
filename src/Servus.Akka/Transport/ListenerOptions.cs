namespace Servus.Akka.Transport;

public abstract record ListenerOptions
{
    public required string Host { get; init; }
    public required ushort Port { get; init; }
    public int Backlog { get; init; } = 512;
    public int? SocketSendBufferSize { get; init; }
    public int? SocketReceiveBufferSize { get; init; }
    public int ReceiveBufferHint { get; init; } = 64 * 1024;
    public bool WaitForData { get; init; } = true;
    public long InputPauseThreshold { get; init; } = 1024 * 1024;
    public long InputResumeThreshold { get; init; } = 512 * 1024;
    public long OutputPauseThreshold { get; init; } = 64 * 1024;
    public long OutputResumeThreshold { get; init; } = 32 * 1024;
    public int MinimumSegmentSize { get; init; } = 16 * 1024;
}
