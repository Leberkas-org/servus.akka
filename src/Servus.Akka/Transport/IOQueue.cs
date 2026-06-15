using System.Collections.Concurrent;
using System.IO.Pipelines;

namespace Servus.Akka.Transport;

// A batched PipeScheduler modeled on Kestrel's IOQueue. The default PipeScheduler.ThreadPool
// dispatches every pipe continuation as its own ThreadPool.UnsafeQueueUserWorkItem; under high
// connection counts that is one scheduling hop per socket wake. IOQueue instead enqueues callbacks
// and drains them in batches under a single ThreadPool work item per wake, and a small bounded set
// of queues (min(CPU, 16)) is shared across all connections and handed out round-robin so scheduler
// fan-out is capped the way Kestrel caps it.
//
// The Interlocked barriers here are correct precisely because this is a true cross-thread system
// boundary (producers on socket/app threads, drain on a ThreadPool thread); this is the kind of
// place CLAUDE.md permits explicit synchronization.
internal sealed class IOQueue : PipeScheduler, IThreadPoolWorkItem
{
    private static readonly IOQueue[] Queues = CreateQueues();
    private static int _assignCursor = -1;

    private readonly ConcurrentQueue<Work> _workItems = new();
    private int _doingWork;

    public static IOQueue GetNext()
    {
        var index = (uint)Interlocked.Increment(ref _assignCursor) % (uint)Queues.Length;
        return Queues[index];
    }

    private static IOQueue[] CreateQueues()
    {
        var count = Math.Min(Environment.ProcessorCount, 16);
        var queues = new IOQueue[count];
        for (var i = 0; i < count; i++)
        {
            queues[i] = new IOQueue();
        }

        return queues;
    }

    public override void Schedule(Action<object?> action, object? state)
    {
        _workItems.Enqueue(new Work(action, state));

        // Only the thread that flips _doingWork 0 -> 1 schedules the drain; later enqueues piggyback
        // on the in-flight Execute loop instead of queuing another work item.
        if (Interlocked.CompareExchange(ref _doingWork, 1, 0) == 0)
        {
            // Fully qualified: unqualified `ThreadPool` would bind to the inherited
            // PipeScheduler.ThreadPool property, not System.Threading.ThreadPool.
            System.Threading.ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false);
        }
    }

    void IThreadPoolWorkItem.Execute()
    {
        while (true)
        {
            while (_workItems.TryDequeue(out var item))
            {
                item.Callback(item.State);
            }

            // Clear the working flag, then re-check: an enqueue that raced between the empty dequeue
            // and this exchange is caught by the IsEmpty check below.
            Interlocked.Exchange(ref _doingWork, 0);

            if (_workItems.IsEmpty)
            {
                break;
            }

            // Work arrived after we cleared the flag. Re-claim the drain; if another Schedule call
            // already claimed it, that thread owns the next drain and we can leave.
            if (Interlocked.CompareExchange(ref _doingWork, 1, 0) == 1)
            {
                break;
            }
        }
    }

    private readonly record struct Work(Action<object?> Callback, object? State);
}
