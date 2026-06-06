namespace Servus.Akka.Transport.Tcp;

internal sealed class SocketPipeConnectionOptions
{
    public long InputPauseWriterThreshold { get; init; } = 1024 * 1024;
    public long InputResumeWriterThreshold { get; init; } = 512 * 1024;
    public long OutputPauseWriterThreshold { get; init; } = 1024 * 1024;
    public long OutputResumeWriterThreshold { get; init; } = 512 * 1024;
    public int MinimumSegmentSize { get; init; } = 4 * 1024;
    public int ReceiveBufferHint { get; init; } = 4 * 1024;
    public bool WaitForData { get; init; } = true;
}
