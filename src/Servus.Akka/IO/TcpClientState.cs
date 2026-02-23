using System.Buffers;
using System.IO.Pipelines;
using System.Threading.Channels;

namespace Servus.Akka.IO;

internal sealed class TcpClientState
{
    public int MaxFrameSize { get; }

    private readonly Channel<(IMemoryOwner<byte> buffer, int readableBytes)> _inboundChannel;
    private readonly Channel<(IMemoryOwner<byte> buffer, int readableBytes)> _outboundChannel;

    public ChannelReader<(IMemoryOwner<byte> buffer, int readableBytes)> OutboundReader => _outboundChannel.Reader;
    public ChannelWriter<(IMemoryOwner<byte> buffer, int readableBytes)> OutboundWriter => _outboundChannel.Writer;

    public ChannelReader<(IMemoryOwner<byte> buffer, int readableBytes)> InboundReader => _inboundChannel.Reader;
    public ChannelWriter<(IMemoryOwner<byte> buffer, int readableBytes)> InboundWriter => _inboundChannel.Writer;
    public Pipe Pipe { get; }

    public TcpClientState(int maxFrameSize,
        Channel<(IMemoryOwner<byte> buffer, int readableBytes)>? inboundChannel,
        Channel<(IMemoryOwner<byte> buffer, int readableBytes)>? outboundChannel)
    {
        _inboundChannel = inboundChannel ?? Channel.CreateUnbounded<(IMemoryOwner<byte> buffer, int readableBytes)>();
        _outboundChannel = outboundChannel ?? Channel.CreateUnbounded<(IMemoryOwner<byte> buffer, int readableBytes)>();
        
        MaxFrameSize = maxFrameSize;
        Pipe = new Pipe(new PipeOptions(pauseWriterThreshold: GetBufferSize(),
            resumeWriterThreshold: GetBufferSize() / 2,
            useSynchronizationContext: false));
    }


    public Memory<byte> GetWriteMemory() => Pipe.Writer.GetMemory(MaxFrameSize / 4);

    private int GetBufferSize()
    {
        return MaxFrameSize switch
        {
            // if the max frame size is under 128kb, scale it up to 512kb
            <= 128 * 1024 => 512 * 1024,
            // between 128kb and 1mb, scale it up to 2mb
            <= 1024 * 1024 => 2 * 1024 * 1024,
            // if the max frame size is above 1mb, 2x it
            _ => MaxFrameSize * 2
        };
    }
}
