using System.Buffers;
using System.IO.Pipelines;

namespace Servus.Akka.Transport.Tcp;

internal sealed class PipeSegmentLease(ReadOnlySequence<byte> buffer, PipeReader reader)
    : ITransportInbound, IDisposable
{
    private LeaseTracker? _tracker;
    private bool _disposed;

    public ReadOnlyMemory<byte> Memory => buffer.First;
    public ReadOnlySequence<byte> Buffer => buffer;
    public int Length => (int)buffer.Length;

    internal void SetTracker(LeaseTracker tracker) => _tracker = tracker;

    public TransportBuffer ToTransportBuffer()
    {
        var buf = TransportBuffer.Rent((int)buffer.Length);
        buffer.CopyTo(buf.FullMemory.Span);
        buf.Length = (int)buffer.Length;
        return buf;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        reader.AdvanceTo(buffer.End);
        _tracker?.Return(this);
    }
}
