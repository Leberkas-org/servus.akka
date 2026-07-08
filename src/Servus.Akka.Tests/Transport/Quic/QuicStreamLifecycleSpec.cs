using Akka.Actor;
using Servus.Akka.Tests.Utils;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Quic;

namespace Servus.Akka.Tests.Transport.Quic;

public sealed class QuicStreamLifecycleSpec
{
    private const int MaxSyncReads = 8;

    private static (QuicStreamLifecycle Lifecycle, MockConnectionOperations Ops) CreateLifecycle()
    {
        var ops = new MockConnectionOperations();
        var lifecycle = new QuicStreamLifecycle(ops, ActorRefs.Nobody, new NoopReadHost(), MaxSyncReads);
        return (lifecycle, ops);
    }

    [Fact(Timeout = 5000)]
    public void OnStreamSendFlushed_with_matching_epoch_should_push_MultiplexedDataFlushed()
    {
        var (lifecycle, ops) = CreateLifecycle();
        var state = lifecycle.RegisterTestStream(4, StreamDirection.Bidirectional, null);

        lifecycle.OnStreamSendFlushed(streamId: 4, bytes: 4096, epoch: state.Epoch);

        var ack = Assert.Single(ops.PushedInbound.OfType<MultiplexedDataFlushed>());
        Assert.Equal(4, ack.StreamId.Value);
        Assert.Equal(4096, ack.Bytes);
    }

    [Fact(Timeout = 5000)]
    public void OnStreamSendFlushed_with_stale_epoch_should_be_dropped()
    {
        var (lifecycle, ops) = CreateLifecycle();
        var state = lifecycle.RegisterTestStream(4, StreamDirection.Bidirectional, null);

        lifecycle.OnStreamSendFlushed(streamId: 4, bytes: 4096, epoch: state.Epoch - 1);

        Assert.Empty(ops.PushedInbound.OfType<MultiplexedDataFlushed>());
    }

    [Fact(Timeout = 5000)]
    public void OnStreamSendFlushed_for_unknown_stream_should_be_dropped()
    {
        var (lifecycle, ops) = CreateLifecycle();

        lifecycle.OnStreamSendFlushed(streamId: 999, bytes: 128, epoch: 1);

        Assert.Empty(ops.PushedInbound.OfType<MultiplexedDataFlushed>());
    }

    [Fact(Timeout = 5000)]
    public void OnStreamSendFlushed_with_zero_bytes_should_be_ignored()
    {
        var (lifecycle, ops) = CreateLifecycle();
        var state = lifecycle.RegisterTestStream(4, StreamDirection.Bidirectional, null);

        lifecycle.OnStreamSendFlushed(streamId: 4, bytes: 0, epoch: state.Epoch);

        Assert.Empty(ops.PushedInbound.OfType<MultiplexedDataFlushed>());
    }

    private sealed class NoopReadHost : IQuicStreamReadHost
    {
        public IConnectionOperations Ops => throw new NotSupportedException();

        public bool TryGetStream(StreamTarget id, out QuicStreamState state)
            => throw new NotSupportedException();

        public void RequestStreamRead(StreamTarget streamId)
        {
        }

        public void OnInboundComplete(DisconnectReason reason, long rawStreamId)
        {
        }

        public void OnReadFailure(QuicStreamState state, Exception error)
        {
        }
    }
}
