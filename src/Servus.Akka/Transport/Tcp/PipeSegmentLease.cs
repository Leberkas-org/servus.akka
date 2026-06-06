using System.Buffers;
using System.IO.Pipelines;

namespace Servus.Akka.Transport.Tcp;

internal sealed class PipeSegmentLease : ITransportInbound, IDisposable
{
    private readonly PipeReader _reader;
    private readonly ReadOnlySequence<byte> _buffer;
    private LeaseTracker? _tracker;
    private bool _disposed;

    public PipeSegmentLease(ReadOnlySequence<byte> buffer, PipeReader reader)
    {
        _buffer = buffer;
        _reader = reader;
    }

    public ReadOnlyMemory<byte> Memory => _buffer.First;
    public ReadOnlySequence<byte> Buffer => _buffer;
    public int Length => (int)_buffer.Length;

    internal void SetTracker(LeaseTracker tracker) => _tracker = tracker;

    public TransportBuffer ToTransportBuffer()
    {
        var buf = TransportBuffer.Rent((int)_buffer.Length);
        _buffer.CopyTo(buf.FullMemory.Span);
        buf.Length = (int)_buffer.Length;
        return buf;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _reader.AdvanceTo(_buffer.End);
        _tracker?.Return(this);
    }
}
