namespace Servus.Akka.Transport;

internal sealed class SocketPipeConnectionOptions
{
    public long InputPauseWriterThreshold { get; init; } = 1024 * 1024;
    public long InputResumeWriterThreshold { get; init; } = 512 * 1024;
    public long OutputPauseWriterThreshold { get; init; } = 1024 * 1024;
    public long OutputResumeWriterThreshold { get; init; } = 512 * 1024;
    public int MinimumSegmentSize { get; init; } = 16 * 1024;
    public int ReceiveBufferHint { get; init; } = 64 * 1024;
    public bool WaitForData { get; init; }

    internal static SocketPipeConnectionOptions FromTransport(TransportOptions transport) => new()
    {
        ReceiveBufferHint = transport.ReceiveBufferHint,
        WaitForData = transport.WaitForData,
        InputPauseWriterThreshold = transport.InputPauseThreshold,
        InputResumeWriterThreshold = transport.InputResumeThreshold,
        OutputPauseWriterThreshold = transport.OutputPauseThreshold,
        OutputResumeWriterThreshold = transport.OutputResumeThreshold,
        MinimumSegmentSize = transport.MinimumSegmentSize,
    };

    internal static SocketPipeConnectionOptions FromListener(ListenerOptions listener) => new()
    {
        ReceiveBufferHint = listener.ReceiveBufferHint,
        WaitForData = listener.WaitForData,
        InputPauseWriterThreshold = listener.InputPauseThreshold,
        InputResumeWriterThreshold = listener.InputResumeThreshold,
        OutputPauseWriterThreshold = listener.OutputPauseThreshold,
        OutputResumeWriterThreshold = listener.OutputResumeThreshold,
        MinimumSegmentSize = listener.MinimumSegmentSize,
    };
}
