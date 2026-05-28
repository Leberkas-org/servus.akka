using System.Threading.Channels;

namespace Servus.Akka.Transport.Tcp;

internal sealed class ConnectionHandle(
    ChannelWriter<TransportBuffer> outboundWriter,
    ChannelReader<TransportBuffer> inboundReader,
    CancellationToken token)
{
    public void Write(TransportBuffer buffer)
    {
        if (!outboundWriter.TryWrite(buffer))
        {
            buffer.Dispose();
        }
    }

    public bool TryRead(out TransportBuffer? buffer)
    {
        return inboundReader.TryRead(out buffer);
    }

    public void SignalClose()
    {
        outboundWriter.TryComplete();
    }

    public bool IsCancelled => token.IsCancellationRequested;
}
