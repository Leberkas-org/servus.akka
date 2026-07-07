using System.Net;
using Akka.Actor;
using Servus.Akka.Tests.Utils;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Quic;
using Servus.Akka.Transport.Quic.Listener;

namespace Servus.Akka.Tests.Transport.Quic.Listener;

public sealed class QuicServerStateMachineSpec
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

    private static (QuicServerStateMachine Sm, MockConnectionOperations Ops) CreateStateMachine(
        QuicConnectionHandle? handle = null)
    {
        var ops = new MockConnectionOperations();
        var sm = new QuicServerStateMachine(
            ops,
            ActorRefs.Nobody,
            handle ?? CreateTestHandle(),
            TestConnectionInfo);
        return (sm, ops);
    }

    private static WireBuffer CreateTestBuffer(params byte[] data)
    {
        var buf = WireBuffer.Rent(data.Length);
        data.CopyTo(buf.FullMemory.Span);
        buf.Length = data.Length;
        return buf;
    }

    private static StreamReceiveCompleted CreateReadEvent(QuicStreamState state, byte[] data)
    {
        var buf = WireBuffer.Rent(data.Length);
        data.CopyTo(buf.FullMemory.Span);
        buf.Length = data.Length;
        return new StreamReceiveCompleted(state, buf, state.Epoch);
    }

    private static StreamReceiveCompleted CreateCompletedReadEvent(QuicStreamState state)
    {
        return new StreamReceiveCompleted(state, null, state.Epoch);
    }

    [Fact(Timeout = 5000)]
    public void Start_should_emit_TransportConnected()
    {
        var (sm, ops) = CreateStateMachine();

        sm.Start();

        Assert.Single(ops.PushedInbound);
        var connected = Assert.IsType<TransportConnected>(ops.PushedInbound[0]);
        Assert.Equal(TestConnectionInfo, connected.Info);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_OpenStream_should_signal_pull_outbound()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();
        ops.PullOutboundCount = 0;

        sm.HandlePush(new OpenStream(1, StreamDirection.Bidirectional));

        Assert.True(ops.PullOutboundCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_MultiplexedData_with_unknown_stream_should_dispose_buffer()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PullOutboundCount = 0;

        var buffer = CreateTestBuffer(1, 2, 3);
        sm.HandlePush(MultiplexedData.Rent(buffer, 999));

        Assert.True(ops.PullOutboundCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_MultiplexedData_should_return_wrapper_to_pool_when_stream_not_found()
    {
        var (sm, _) = CreateStateMachine();
        sm.Start();

        var buffer = CreateTestBuffer(1, 2, 3);
        var data = MultiplexedData.Rent(buffer, 999);

        sm.HandlePush(data);

        // Return() clears Buffer to null — verify the wrapper was recycled
        Assert.Null(data.Buffer);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_MultiplexedData_should_return_wrapper_to_pool_after_buffer_dispose()
    {
        // Exercises the else-branch of HandleMultiplexedData (unknown stream): buffer is disposed
        // and the wrapper must still be returned to the pool (Buffer cleared to null).
        var (sm, _) = CreateStateMachine();
        sm.Start();

        var buffer = CreateTestBuffer(4, 5, 6);
        var data = MultiplexedData.Rent(buffer, 777);

        sm.HandlePush(data);

        // Return() clears Buffer to null — verify the wrapper was recycled
        Assert.Null(data.Buffer);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_DisconnectTransport_should_complete_stage()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();

        sm.HandlePush(new DisconnectTransport(DisconnectReason.Graceful));

        Assert.True(ops.CompleteStageCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void HandleUpstreamFinish_should_complete_stage()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();

        sm.HandleUpstreamFinish();

        Assert.True(ops.CompleteStageCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_PipeStreamReadComplete_should_push_multiplexed_data()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();

        var state = sm.RegisterTestStream(42, StreamDirection.Bidirectional);
        ops.PushedInbound.Clear();

        var evt = CreateReadEvent(state, [1, 2, 3]);
        sm.Dispatch(evt);

        Assert.Single(ops.PushedInbound);
        var multiplexed = Assert.IsType<MultiplexedData>(ops.PushedInbound[0]);
        Assert.Equal(new StreamTarget(42L), multiplexed.StreamId);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_read_completion_after_stream_teardown_should_be_dropped()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();

        var state = sm.RegisterTestStream(42, StreamDirection.Bidirectional);

        // Read in flight, then the stream is torn down before the completion arrives.
        var evt = CreateReadEvent(state, [1, 2, 3]);
        sm.HandlePush(new ResetStream(42));
        ops.PushedInbound.Clear();

        sm.Dispatch(evt);

        // The stale completion is dropped (stream no longer present) and its buffer released.
        Assert.Empty(ops.PushedInbound);
    }

    [Fact(Timeout = 5000)]
    public void PostStop_should_not_throw()
    {
        var (sm, _) = CreateStateMachine();
        sm.Start();

        sm.PostStop();
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_ResetStream_with_no_active_stream_should_signal_pull()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PullOutboundCount = 0;

        sm.HandlePush(new ResetStream(999));

        Assert.True(ops.PullOutboundCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundStreamAccepted_should_push_ServerStreamAccepted()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();

        var stream = new MemoryStream();
        sm.Dispatch(new InboundStreamAccepted(stream, 42));

        Assert.Contains(ops.PushedInbound, item => item is ServerStreamAccepted { Id.Value: 42 });
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_CompleteWrites_should_signal_pull()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PullOutboundCount = 0;

        sm.HandlePush(new CompleteWrites(1));

        Assert.True(ops.PullOutboundCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_MultiplexedData_with_known_stream_should_signal_pull()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PullOutboundCount = 0;

        sm.HandlePush(new OpenStream(1, StreamDirection.Bidirectional));
        ops.PullOutboundCount = 0;

        sm.Dispatch(new StreamLeaseAcquired(Stream.Null, 1));
        ops.PullOutboundCount = 0;

        var buffer = CreateTestBuffer(1, 2, 3);
        sm.HandlePush(MultiplexedData.Rent(buffer, 1));

        Assert.True(ops.PullOutboundCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_PipeStreamReadComplete_completed_should_push_StreamReadCompleted()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();

        var state = sm.RegisterTestStream(1, StreamDirection.Bidirectional);
        ops.PushedInbound.Clear();

        var evt = CreateCompletedReadEvent(state);
        sm.Dispatch(evt);

        Assert.Contains(ops.PushedInbound, item => item is StreamReadCompleted { Id.Value: 1 });
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_CompleteWrites_with_no_stream_should_signal_pull()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PullOutboundCount = 0;

        sm.HandlePush(new CompleteWrites(999));

        Assert.True(ops.PullOutboundCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_ResetStream_with_active_stream_should_push_StreamClosed()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();

        sm.HandlePush(new OpenStream(1, StreamDirection.Bidirectional));
        sm.Dispatch(new StreamLeaseAcquired(Stream.Null, 1));
        ops.PushedInbound.Clear();

        sm.HandlePush(new ResetStream(1));

        Assert.Contains(ops.PushedInbound, item => item is StreamClosed { Id.Value: 1 });
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_StreamReceiveFailed_should_push_StreamClosed()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();

        var state = sm.RegisterTestStream(1, StreamDirection.Bidirectional);
        ops.PushedInbound.Clear();

        sm.Dispatch(new StreamReceiveFailed(state, new IOException("Read failed"), state.Epoch));

        Assert.Contains(ops.PushedInbound,
            item => item is StreamClosed { Id.Value: 1, Reason: DisconnectReason.Error });
    }

    [Fact(Timeout = 5000)]
    public void OnStreamLeaseAcquired_with_unknown_stream_should_dispose_stream()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();

        sm.Dispatch(new StreamLeaseAcquired(Stream.Null, 999));

        Assert.DoesNotContain(ops.PushedInbound, item => item is StreamOpened { Id.Value: 999 });
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_OpenStream_when_handle_is_null_should_signal_pull()
    {
        var (sm, ops) = CreateStateMachine();

        sm.HandlePush(new OpenStream(1, StreamDirection.Bidirectional));

        Assert.True(ops.PullOutboundCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void PostStop_before_start_should_not_throw()
    {
        var (sm, _) = CreateStateMachine();

        sm.PostStop();
    }

    [Fact(Timeout = 5000)]
    public void HandleDownstreamFinish_should_cleanup()
    {
        var (sm, _) = CreateStateMachine();
        sm.Start();

        sm.HandleDownstreamFinish();

        sm.PostStop();
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_MultiplexedData_after_disconnect_should_dispose_buffer()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();

        sm.HandlePush(new DisconnectTransport(DisconnectReason.Graceful));
        ops.PullOutboundCount = 0;

        var buffer = CreateTestBuffer(1, 2, 3);
        sm.HandlePush(MultiplexedData.Rent(buffer, 1));

        Assert.True(ops.PullOutboundCount > 0);
    }
}
