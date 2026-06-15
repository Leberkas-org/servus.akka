using Servus.Akka.Transport;

namespace Servus.Akka.Tests.Transport;

public sealed class IOQueueSpec
{
    private sealed class Counter
    {
        public int Ran;
        public int Target;
        public readonly TaskCompletionSource Done = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    [Fact(Timeout = 5000)]
    public async Task Schedule_should_run_every_callback_exactly_once_under_concurrent_producers()
    {
        var queue = new IOQueue();
        var counter = new Counter { Target = 20_000 };

        // Scheduling concurrently from many threads exercises the _doingWork claim/re-claim race:
        // every callback must run exactly once, none dropped, none double-run.
        Parallel.For(0, counter.Target, _ =>
            queue.Schedule(static state =>
            {
                var c = (Counter)state!;
                if (Interlocked.Increment(ref c.Ran) == c.Target)
                {
                    c.Done.SetResult();
                }
            }, counter));

        await counter.Done.Task.WaitAsync(TimeSpan.FromSeconds(4));

        Assert.Equal(counter.Target, Volatile.Read(ref counter.Ran));
    }

    [Fact(Timeout = 5000)]
    public void GetNext_should_hand_out_a_queue()
    {
        Assert.NotNull(IOQueue.GetNext());
    }
}
