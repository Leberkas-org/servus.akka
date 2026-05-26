using System.Threading.Channels;

namespace Servus.Akka.Transport.Tcp;

internal sealed class ConnectionHandle
{
    private readonly ChannelWriter<TransportBuffer> _outboundWriter;
    private readonly ChannelReader<TransportBuffer> _inboundReader;
    private readonly CancellationToken _token;

    public ConnectionHandle(
        ChannelWriter<TransportBuffer> outboundWriter,
        ChannelReader<TransportBuffer> inboundReader,
        CancellationToken token)
    {
        _outboundWriter = outboundWriter;
        _inboundReader = inboundReader;
        _token = token;
    }

    public void Write(TransportBuffer buffer)
    {
        if (!_outboundWriter.TryWrite(buffer))
        {
            buffer.Dispose();
        }
    }

    public bool TryRead(out TransportBuffer? buffer)
    {
        return _inboundReader.TryRead(out buffer);
    }

    public void SignalClose()
    {
        _outboundWriter.TryComplete();
    }

    public bool IsCancelled => _token.IsCancellationRequested;
}
