using System.IO.Pipelines;
using Servus.Akka.Transport.Tcp;

namespace Servus.Akka.Tests.Transport.Tcp;

public sealed class LeaseTrackerSpec
{
    [Fact(Timeout = 5000)]
    public async Task Acquire_should_track_outstanding_lease()
    {
        var pipe = new Pipe();
        await pipe.Writer.WriteAsync("data"u8.ToArray());
        var result = await pipe.Reader.ReadAsync();

        var tracker = new LeaseTracker(4);
        var lease = tracker.Acquire(result.Buffer, pipe.Reader);

        Assert.Equal(1, tracker.Outstanding);

        lease.Dispose();

        Assert.Equal(0, tracker.Outstanding);
    }

    [Fact(Timeout = 5000)]
    public async Task CanAcquire_should_return_false_at_max()
    {
        var pipe = new Pipe();
        await pipe.Writer.WriteAsync("data"u8.ToArray());
        var result = await pipe.Reader.ReadAsync();

        var tracker = new LeaseTracker(1);
        var lease = tracker.Acquire(result.Buffer, pipe.Reader);

        Assert.False(tracker.CanAcquire);

        lease.Dispose();

        Assert.True(tracker.CanAcquire);
    }

    [Fact(Timeout = 5000)]
    public async Task ForceReturnAll_should_dispose_all_leases()
    {
        var pipe1 = new Pipe();
        var pipe2 = new Pipe();

        await pipe1.Writer.WriteAsync("a"u8.ToArray());
        await pipe2.Writer.WriteAsync("b"u8.ToArray());

        var result1 = await pipe1.Reader.ReadAsync();
        var result2 = await pipe2.Reader.ReadAsync();

        var tracker = new LeaseTracker(4);
        tracker.Acquire(result1.Buffer, pipe1.Reader);
        tracker.Acquire(result2.Buffer, pipe2.Reader);

        Assert.Equal(2, tracker.Outstanding);

        tracker.ForceReturnAll();

        Assert.Equal(0, tracker.Outstanding);
    }
}
