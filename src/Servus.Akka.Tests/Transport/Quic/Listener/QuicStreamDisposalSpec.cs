using System.Net;
using Akka.Actor;
using Servus.Akka.Tests.Utils;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Quic;
using Servus.Akka.Transport.Quic.Listener;

namespace Servus.Akka.Tests.Transport.Quic.Listener;

public sealed class QuicStreamDisposalSpec
{
    private static readonly ConnectionInfo TestConnectionInfo = new(
        new IPEndPoint(IPAddress.Loopback, 5000),
        new IPEndPoint(IPAddress.Loopback, 12345),
        TransportProtocol.Tcp);

    private static QuicConnectionHandle CreateTestHandle()
    {
        return new QuicConnectionHandle(
            openStream: (_, _) => Task.FromResult<(Stream, long)>((Stream.Null, 1)),
            acceptInboundStream: async ct =>
            {
                await Task.Delay(Timeout.Infinite, ct).ConfigureAwait(false);
                return null;
            },
            getLocalEndPoint: () => new IPEndPoint(IPAddress.Loopback, 5000),
            getRemoteEndPoint: () => null,
            dispose: () => default);
    }

    private static (QuicServerStateMachine Sm, MockConnectionOperations Ops) CreateStateMachine()
    {
        var ops = new MockConnectionOperations();
        var sm = new QuicServerStateMachine(
            ops,
            ActorRefs.Nobody,
            CreateTestHandle(),
            TestConnectionInfo);
        return (sm, ops);
    }

    [Fact(Timeout = 5000)]
    public void ReadComplete_then_CompleteWrites_should_remove_stream()
    {
        var (sm, _) = CreateStateMachine();
        sm.Start();

        var state = sm.RegisterTestStream(4, StreamDirection.Bidirectional);
        Assert.Equal(1, sm.ActiveStreamCount);

        sm.Dispatch(new StreamReceiveCompleted(state, null, state.Epoch));
        sm.HandlePush(new CompleteWrites(4));

        Assert.Equal(0, sm.ActiveStreamCount);
    }

    [Fact(Timeout = 5000)]
    public void CompleteWrites_then_ReadComplete_should_remove_stream()
    {
        var (sm, _) = CreateStateMachine();
        sm.Start();

        var state = sm.RegisterTestStream(4, StreamDirection.Bidirectional);
        Assert.Equal(1, sm.ActiveStreamCount);

        sm.HandlePush(new CompleteWrites(4));
        Assert.Equal(1, sm.ActiveStreamCount);

        sm.Dispatch(new StreamReceiveCompleted(state, null, state.Epoch));
        Assert.Equal(0, sm.ActiveStreamCount);
    }

    [Fact(Timeout = 5000)]
    public void ResetStream_should_remove_stream_immediately()
    {
        var (sm, _) = CreateStateMachine();
        sm.Start();

        sm.RegisterTestStream(4, StreamDirection.Bidirectional);
        Assert.Equal(1, sm.ActiveStreamCount);

        sm.HandlePush(new ResetStream(4));

        Assert.Equal(0, sm.ActiveStreamCount);
    }

    [Fact(Timeout = 5000)]
    public async Task InboundStream_full_lifecycle_should_dispose_underlying_stream()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();

        var trackingStream = new TrackingStream();
        sm.Dispatch(new InboundStreamAccepted(trackingStream, 4));

        Assert.Equal(1, sm.ActiveStreamCount);
        Assert.Contains(ops.PushedInbound, item => item is ServerStreamAccepted { Id.Value: 4 });

        var lifecycleState = sm.GetStreamForTest(4)!;
        sm.Dispatch(new StreamReceiveCompleted(lifecycleState, null, lifecycleState.Epoch));
        sm.HandlePush(new CompleteWrites(4));

        Assert.Equal(0, sm.ActiveStreamCount);

        await trackingStream.WaitForDisposalAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task InboundStream_CompleteWrites_before_ReadComplete_should_dispose_underlying_stream()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();

        var trackingStream = new TrackingStream();
        sm.Dispatch(new InboundStreamAccepted(trackingStream, 4));

        Assert.Equal(1, sm.ActiveStreamCount);

        sm.HandlePush(new CompleteWrites(4));
        Assert.Equal(1, sm.ActiveStreamCount);

        var readState = sm.GetStreamForTest(4)!;
        sm.Dispatch(new StreamReceiveCompleted(readState, null, readState.Epoch));
        Assert.Equal(0, sm.ActiveStreamCount);

        await trackingStream.WaitForDisposalAsync();
    }

    [Fact(Timeout = 5000)]
    public void Multiple_streams_should_all_be_cleaned_up()
    {
        var (sm, _) = CreateStateMachine();
        sm.Start();

        var states = new Dictionary<long, QuicStreamState>();
        for (var i = 0; i < 150; i++)
        {
            var streamId = i * 4;
            states[streamId] = sm.RegisterTestStream(streamId, StreamDirection.Bidirectional);
        }

        Assert.Equal(150, sm.ActiveStreamCount);

        for (var i = 0; i < 150; i++)
        {
            var streamId = i * 4;
            sm.Dispatch(new StreamReceiveCompleted(states[streamId], null, states[streamId].Epoch));
            sm.HandlePush(new CompleteWrites(streamId));
        }

        Assert.Equal(0, sm.ActiveStreamCount);
    }

    private sealed class TrackingStream : MemoryStream
    {
        private readonly TaskCompletionSource _disposed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task WaitForDisposalAsync() => _disposed.Task;

        // Deliberately never completes synchronously (or at all, short of cancellation): these tests
        // drive read completion entirely via manually-dispatched StreamReceiveCompleted events, so the
        // real read must stay pending — an empty MemoryStream's default ReadAsync would otherwise
        // synchronously EOF the moment the server SM's sync-read budget (mirroring the client's) issues
        // the initial read on attach, completing the read before the test gets to assert the interim
        // (write-completed-but-not-read-completed) state. Cancellation is honored so QuiesceAsync (part
        // of the real disposal path this test exercises) still settles instead of hanging.
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            var tcs = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
            await using (cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken)))
            {
                return await tcs.Task.ConfigureAwait(false);
            }
        }

        protected override void Dispose(bool disposing)
        {
            _disposed.TrySetResult();
            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            _disposed.TrySetResult();
            await base.DisposeAsync();
        }
    }
}
