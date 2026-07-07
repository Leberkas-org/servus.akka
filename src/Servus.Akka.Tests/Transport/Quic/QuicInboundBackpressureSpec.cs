using Akka.Actor;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Quic;
using Servus.Akka.Transport.Quic.Client;

namespace Servus.Akka.Tests.Transport.Quic;

/// <summary>
/// Pull-gating discipline for QUIC inbound reads: per stream, at most ONE in-flight read AND at most ONE
/// undelivered completed item, so <c>_pendingReads</c> (the stage's queue) is bounded by the live-stream
/// count under a slow consumer.
///
/// These tests drive <see cref="QuicTransportStateMachine"/> directly rather than the full Akka Streams
/// graph (<c>QuicConnectionStage</c>): the stage's <c>_pendingReads</c> queue and its pull-driven dequeue
/// site are private `GraphStageLogic` state that only exists inside a materialized stream. <see
/// cref="SlowConsumerOps"/> reproduces the exact contract the real stage Logic implements —
/// <c>OnPushInbound</c> returns whether the item reached downstream immediately or was queued, mirroring
/// <c>QuicConnectionStage.Logic.OnPushInbound</c> — so the SM-level tests exercise the identical
/// push/queue/re-arm boundary without needing a live Akka Streams graph. Streams are attached to a real
/// (in-memory) backing <see cref="Stream"/> so the sync fast-path in <c>RequestStreamRead</c> genuinely
/// re-arms/recurses exactly the way production code does — this is what lets the first test fail against
/// the old unconditional re-arm and pass against the fix without any test-only shortcuts.
/// </summary>
public sealed class QuicInboundBackpressureSpec
{
    private const int ReceiveHint = 16;

    /// <summary>
    /// Test-only <see cref="IConnectionOperations"/> mirroring the stage Logic's pull discipline:
    /// <see cref="OnPushInbound"/> pushes immediately while <see cref="DownstreamAvailable"/> is true,
    /// otherwise queues into <see cref="PendingReads"/> — exactly like
    /// QuicConnectionStage.Logic/QuicServerConnectionStage.Logic's own <c>_pendingReads</c>.
    /// </summary>
    private sealed class SlowConsumerOps : IConnectionOperations
    {
        public bool DownstreamAvailable;
        public readonly Queue<ITransportInbound> PendingReads = new();
        public readonly List<ITransportInbound> Delivered = [];

        public bool OnPushInbound(ITransportInbound item)
        {
            if (DownstreamAvailable)
            {
                Delivered.Add(item);
                return true;
            }

            PendingReads.Enqueue(item);
            return false;
        }

        public void OnSignalPullOutbound()
        {
        }

        public void OnCompleteStage()
        {
        }

        public void OnScheduleTimer(string key, TimeSpan delay)
        {
        }

        public void OnCancelTimer(string key)
        {
        }
    }

    private static QuicConnectionHandle CreateMockHandle()
    {
        return new QuicConnectionHandle(
            openStream: async (_, ct) =>
            {
                await Task.Delay(0, ct).ConfigureAwait(false);
                return (new MemoryStream(), 1L);
            },
            acceptInboundStream: async ct =>
            {
                await Task.Delay(Timeout.Infinite, ct).ConfigureAwait(false);
                return null;
            },
            getLocalEndPoint: () => new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 12345),
            getRemoteEndPoint: () => null,
            dispose: () => ValueTask.CompletedTask);
    }

    private static (SlowConsumerOps ops, QuicTransportStateMachine sm) CreateConnectedStateMachine()
    {
        var ops = new SlowConsumerOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);
        var options = new QuicTransportOptions
        {
            Host = "localhost",
            Port = 443,
            ReceiveBufferHint = ReceiveHint
        };

        sm.HandlePush(new ConnectTransport(options));

        var handle = CreateMockHandle();
        var lease = new QuicConnectionLease(handle, 100);
        sm.Dispatch(new ConnectionLeaseAcquired(lease));

        return (ops, sm);
    }

    /// <summary>A backing stream with plenty of synchronously-readable bytes, so the sync fast-path in
    /// RequestStreamRead keeps completing (and, under the old unconditional re-arm, keeps recursing)
    /// instead of falling back to the async PipeTo path.</summary>
    private static MemoryStream CreateAbundantDataStream()
        => new(new byte[ReceiveHint * 20]);

    private static void OpenAndAttachStream(QuicTransportStateMachine sm, long streamId)
    {
        sm.HandlePush(new OpenStream(streamId, StreamDirection.Bidirectional));
        sm.Dispatch(new StreamLeaseAcquired(CreateAbundantDataStream(), streamId));
    }

    /// <summary>
    /// The lifecycle events (TransportConnected, StreamOpened) that also flow through OnPushInbound and
    /// therefore also queue while downstream is slow are orthogonal to this pull-gating fix — the
    /// invariant under test is specifically about read completions (MultiplexedData), one per live
    /// stream at most, so assertions filter to that type rather than the queue's raw heterogeneous count.
    /// </summary>
    private static Dictionary<StreamTarget, int> MultiplexedCountsByStream(SlowConsumerOps ops)
        => ops.PendingReads.OfType<MultiplexedData>()
            .GroupBy(d => d.StreamId)
            .ToDictionary(g => g.Key, g => g.Count());

    [Fact(Timeout = 5000)]
    public void Slow_consumer_should_bound_pending_reads_to_stream_count()
    {
        var (ops, sm) = CreateConnectedStateMachine();
        ops.DownstreamAvailable = false;

        const int streamCount = 4;
        for (var i = 1; i <= streamCount; i++)
        {
            OpenAndAttachStream(sm, i);
        }

        var perStream = MultiplexedCountsByStream(ops);

        var totalQueuedReads = perStream.Values.Sum();
        Assert.True(
            totalQueuedReads <= streamCount,
            $"expected queued reads bounded by the live-stream count ({streamCount}), got {totalQueuedReads}");

        Assert.Equal(streamCount, perStream.Count);
        Assert.All(perStream.Values, count => Assert.Equal(1, count));
    }

    [Fact(Timeout = 5000)]
    public void Pulling_one_item_should_re_arm_only_that_streams_read()
    {
        var (ops, sm) = CreateConnectedStateMachine();
        ops.DownstreamAvailable = false;

        OpenAndAttachStream(sm, 1);
        OpenAndAttachStream(sm, 2);

        var before = MultiplexedCountsByStream(ops);
        Assert.Equal(1, before[new StreamTarget(1)]);
        Assert.Equal(1, before[new StreamTarget(2)]);

        // Deliver stream 1's queued read item — exactly what the stage's onPull does when it dequeues a
        // MultiplexedData item — without touching stream 2's.
        var remaining = new Queue<ITransportInbound>(
            ops.PendingReads.Where(item => item is not MultiplexedData md || md.StreamId != new StreamTarget(1)));
        ops.PendingReads.Clear();
        foreach (var item in remaining)
        {
            ops.PendingReads.Enqueue(item);
        }

        sm.NotifyItemDelivered(new StreamTarget(1));

        // Stream 1's read is re-armed and (downstream still slow) queues exactly one new item; stream 2's
        // untouched queued item is unaffected by the delivery of stream 1's — proving the re-arm is
        // targeted at the delivered stream, not a broadcast across all live streams.
        var after = MultiplexedCountsByStream(ops);
        Assert.Equal(1, after[new StreamTarget(1)]);
        Assert.Equal(1, after[new StreamTarget(2)]);
    }
}
