using System.Buffers;
using System.IO.Pipelines;
using System.Threading.Channels;

namespace Servus.Akka.Transport.Tcp;

internal sealed class ClientState(Stream stream, PipeMode direction = PipeMode.Bidirectional) : IDisposable
{
    private static readonly PipeOptions InboundPipeOptions = new(
        pool: MemoryPool<byte>.Shared,
        minimumSegmentSize: 4096,
        pauseWriterThreshold: 0,
        resumeWriterThreshold: 0,
        useSynchronizationContext: false);

    private static readonly PipeOptions OutboundPipeOptions = new(
        pool: MemoryPool<byte>.Shared,
        minimumSegmentSize: 4096,
        pauseWriterThreshold: 1024 * 1024,
        resumeWriterThreshold: 512 * 1024,
        useSynchronizationContext: false);

    private static readonly UnboundedChannelOptions ChannelOptions = new()
    {
        SingleReader = true,
        SingleWriter = true
    };

    public Stream Stream { get; } = stream;
    public PipeMode Direction { get; } = direction;

    public Pipe InboundPipe { get; } = new(InboundPipeOptions);
    public Pipe OutboundPipe { get; } = new(OutboundPipeOptions);

    private readonly Channel<TransportBuffer> _inboundChannel = Channel.CreateUnbounded<TransportBuffer>(ChannelOptions);
    private readonly Channel<TransportBuffer> _outboundChannel = Channel.CreateUnbounded<TransportBuffer>(ChannelOptions);

    public ChannelReader<TransportBuffer> InboundReader => _inboundChannel.Reader;
    public ChannelWriter<TransportBuffer> InboundWriter => _inboundChannel.Writer;
    public ChannelReader<TransportBuffer> OutboundReader => _outboundChannel.Reader;
    public ChannelWriter<TransportBuffer> OutboundWriter => _outboundChannel.Writer;

    public Action? OnWritesComplete { get; init; }

    public void Dispose()
    {
        _inboundChannel.Writer.TryComplete();
        _outboundChannel.Writer.TryComplete();

        while (_inboundChannel.Reader.TryRead(out var buf))
        {
            buf.Dispose();
        }

        while (_outboundChannel.Reader.TryRead(out var buf))
        {
            buf.Dispose();
        }

        try
        {
            InboundPipe.Writer.Complete();
        }
        catch (InvalidOperationException)
        {
            // noop
        }

        try
        {
            InboundPipe.Reader.Complete();
        }
        catch (InvalidOperationException)
        {
            // noop
        }

        try
        {
            OutboundPipe.Writer.Complete();
        }
        catch (InvalidOperationException)
        {
            // noop
        }

        try
        {
            OutboundPipe.Reader.Complete();
        }
        catch (InvalidOperationException)
        {
            // noop
        }

        Stream.Dispose();
    }
}