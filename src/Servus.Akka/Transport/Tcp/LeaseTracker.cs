using System.Buffers;
using System.IO.Pipelines;

namespace Servus.Akka.Transport.Tcp;

internal sealed class LeaseTracker(int maxOutstanding)
{
    private readonly List<PipeSegmentLease> _outstanding = [];

    public int Outstanding => _outstanding.Count;
    public bool CanAcquire => _outstanding.Count < maxOutstanding;

    public PipeSegmentLease Acquire(ReadOnlySequence<byte> buffer, PipeReader reader)
    {
        var lease = new PipeSegmentLease(buffer, reader);
        lease.SetTracker(this);
        _outstanding.Add(lease);
        return lease;
    }

    public void Return(PipeSegmentLease lease) => _outstanding.Remove(lease);

    public void ForceReturnAll()
    {
        for (var i = _outstanding.Count - 1; i >= 0; i--)
        {
            var lease = _outstanding[i];
            _outstanding.RemoveAt(i);
            lease.Dispose();
        }
    }
}