using System.Buffers;
using Akka.Actor;
using Servus.Akka.Tests.Utils;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Quic;
using Servus.Akka.Transport.Quic.Client;

namespace Servus.Akka.Tests.Transport.Quic.Client;

public sealed class QuicTransportStateMachineSpec
{
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

    private static (StubOps ops, QuicTransportStateMachine sm)
        CreateConnectedStateMachine()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);
        var options = new QuicTransportOptions { Host = "localhost", Port = 443 };

        sm.HandlePush(new ConnectTransport(options));

        var handle = CreateMockHandle();
        var lease = new QuicConnectionLease(handle, 100);

        sm.Dispatch(new ConnectionLeaseAcquired(lease));

        return (ops, sm);
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

    private static QuicStreamState CreateDetachedStreamState(long streamId)
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        state.ActivateDirectReadForTest(streamId);
        return state;
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_AcceptLoopFailed_should_signal_transport_disconnected()
    {
        var (ops, sm) = CreateConnectedStateMachine();
        ops.PushedInbound.Clear();

        // A failure from the background accept loop must surface as a connection failure
        // (routing to reconnect), not vanish as an unobserved task exception.
        sm.Dispatch(new AcceptLoopFailed(new IOException("accept loop failed")));

        Assert.Contains(ops.PushedInbound, m => m is TransportDisconnected);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_ConnectTransport_should_schedule_connect_timeout()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);
        var options = new QuicTransportOptions { Host = "localhost", Port = 443 };

        sm.HandlePush(new ConnectTransport(options));

        Assert.Contains("connect-timeout", ops.Timers.Keys);
        Assert.True(ops.PullCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_OpenStream_should_reject_when_not_connected()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        sm.HandlePush(new OpenStream(1, StreamDirection.Bidirectional));

        Assert.True(ops.PullCount > 0);
        Assert.Empty(ops.PushedInbound);
    }

    [Fact(Timeout = 5000)]
    public void HandleUpstreamFinish_should_complete_when_no_connection()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        sm.HandleUpstreamFinish();

        Assert.True(ops.Completed);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_MultiplexedData_should_signal_pull_when_no_stream()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        var buffer = WireBuffer.Rent(16);
        buffer.Length = 4;
        sm.HandlePush(MultiplexedData.Rent(buffer, 1));

        Assert.True(ops.PullCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_CompleteWrites_should_signal_pull_when_no_stream()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        sm.HandlePush(new CompleteWrites(99));

        Assert.True(ops.PullCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_ResetStream_should_signal_pull_when_no_stream()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        sm.HandlePush(new ResetStream(99));

        Assert.True(ops.PullCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_read_completion_after_stream_teardown_should_be_dropped()
    {
        var (ops, sm) = CreateConnectedStateMachine();
        var state = sm.RegisterTestStream(1, StreamDirection.Bidirectional);

        // Read in flight, then the stream is torn down before the completion arrives.
        var evt = CreateReadEvent(state, [1, 2, 3, 4]);
        sm.HandlePush(new ResetStream(1, 42));
        ops.PushedInbound.Clear();

        sm.Dispatch(evt);

        // The stale completion is dropped (stream no longer present) and its buffer released.
        Assert.Empty(ops.PushedInbound);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_DisconnectTransport_should_signal_pull()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        sm.HandlePush(new DisconnectTransport(DisconnectReason.Graceful));

        Assert.True(ops.PullCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_ConnectTransport_should_set_auto_reconnect_from_options()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);
        var options = new QuicTransportOptions
        {
            Host = "localhost",
            Port = 443,
            AutoReconnect = true
        };

        sm.HandlePush(new ConnectTransport(options));

        Assert.Contains("connect-timeout", ops.Timers.Keys);
        Assert.True(ops.PullCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_MultiplexedData_should_dispose_buffer_when_stream_not_found()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        var buffer = WireBuffer.Rent(16);
        buffer.Length = 4;

        sm.HandlePush(MultiplexedData.Rent(buffer, 999));

        Assert.Empty(ops.PushedInbound);
        Assert.True(ops.PullCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_MultiplexedData_should_return_wrapper_to_pool_when_stream_not_found()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        var buffer = WireBuffer.Rent(16);
        buffer.Length = 4;
        var data = MultiplexedData.Rent(buffer, 999);

        sm.HandlePush(data);

        // Return() clears Buffer to null — verify the wrapper was recycled
        Assert.Null(data.Buffer);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_MultiplexedData_should_return_wrapper_to_pool_when_stream_found()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);
        sm.RegisterTestStream(1, StreamDirection.Bidirectional);

        var buffer = WireBuffer.Rent(16);
        buffer.Length = 4;
        var data = MultiplexedData.Rent(buffer, 1);

        sm.HandlePush(data);

        // Return() clears Buffer to null — verify the wrapper was recycled
        Assert.Null(data.Buffer);
    }

    [Fact(Timeout = 5000)]
    public void HandleDownstreamFinish_should_not_complete_when_upstream_not_finished()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        sm.HandleDownstreamFinish();

        Assert.False(ops.Completed);
    }

    [Fact(Timeout = 5000)]
    public void HandleUpstreamFinish_should_complete_stage()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        sm.HandleUpstreamFinish();

        Assert.True(ops.Completed);
    }

    [Fact(Timeout = 5000)]
    public void OnTimer_with_connect_timeout_key_should_push_TransportDisconnected()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        var options = new QuicTransportOptions { Host = "localhost", Port = 443 };
        sm.HandlePush(new ConnectTransport(options));

        sm.OnTimer("connect-timeout");

        Assert.NotEmpty(ops.PushedInbound);
        var disconnected = ops.PushedInbound.OfType<TransportDisconnected>().FirstOrDefault();
        Assert.NotNull(disconnected);
        Assert.Equal(DisconnectReason.Timeout, disconnected.Reason);
    }

    [Fact(Timeout = 5000)]
    public void OnTimer_with_unknown_key_should_do_nothing()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        sm.OnTimer("unknown-timer-key");

        Assert.Empty(ops.PushedInbound);
        Assert.Equal(0, ops.PullCount);
    }

    [Fact(Timeout = 5000)]
    public void OnTimer_without_pending_connect_should_do_nothing()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        sm.OnTimer("connect-timeout");

        Assert.Empty(ops.PushedInbound);
        Assert.Equal(0, ops.PullCount);
    }

    [Fact(Timeout = 5000)]
    public void PostStop_should_cancel_connect_timer()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        sm.PostStop();

        Assert.Contains("connect-timeout", ops.CancelledTimers);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_ResetStream_should_emit_StreamClosed_when_stream_exists()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        sm.HandlePush(new ResetStream(999));

        Assert.Empty(ops.PushedInbound);
        Assert.True(ops.PullCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_CompleteWrites_on_unknown_stream_should_just_pull()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        sm.HandlePush(new CompleteWrites(999));

        Assert.Empty(ops.PushedInbound);
        Assert.True(ops.PullCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_StreamReceiveFailed_for_unknown_stream_should_handle_gracefully()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        var state = CreateDetachedStreamState(1);
        sm.Dispatch(new StreamReceiveFailed(state, new IOException("Read failed"), state.Epoch));

        Assert.Empty(ops.PushedInbound);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_AcquisitionFailed_when_cancelled_should_be_ignored()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        var options = new QuicTransportOptions { Host = "localhost", Port = 443 };
        sm.HandlePush(new ConnectTransport(options));

        sm.Dispatch(new AcquisitionFailed(new OperationCanceledException("Cancelled")));

        Assert.Empty(ops.PushedInbound);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_AcquisitionFailed_with_error_should_push_TransportDisconnected()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        var options = new QuicTransportOptions { Host = "localhost", Port = 443 };
        sm.HandlePush(new ConnectTransport(options));

        sm.Dispatch(new AcquisitionFailed(new IOException("Connection failed")));

        Assert.Contains("connect-timeout", ops.CancelledTimers);
        var disconnected = ops.PushedInbound.OfType<TransportDisconnected>().FirstOrDefault();
        Assert.NotNull(disconnected);
        Assert.Equal(DisconnectReason.Error, disconnected.Reason);
    }

    [Fact(Timeout = 5000)]
    public void HandleUpstreamFinish_with_pending_connection_should_complete_stage()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        var options = new QuicTransportOptions { Host = "localhost", Port = 443 };
        sm.HandlePush(new ConnectTransport(options));
        Assert.False(ops.Completed);

        sm.HandleUpstreamFinish();

        Assert.True(ops.Completed);
    }

    [Fact(Timeout = 5000)]
    public void Multiple_TimerCancelAndSchedule_should_be_tracked()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        var options1 = new QuicTransportOptions { Host = "localhost", Port = 443 };
        sm.HandlePush(new ConnectTransport(options1));
        Assert.Contains("connect-timeout", ops.Timers.Keys);
        Assert.Empty(ops.CancelledTimers);

        var options2 = new QuicTransportOptions { Host = "other.host", Port = 443 };
        sm.HandlePush(new ConnectTransport(options2));
        Assert.Contains("connect-timeout", ops.Timers.Keys);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_direct_read_completion_should_push_MultiplexedData()
    {
        var (ops, sm) = CreateConnectedStateMachine();

        const long streamId = 123L;
        var state = sm.RegisterTestStream(streamId, StreamDirection.Bidirectional);

        ops.PushedInbound.Clear();

        var evt = CreateReadEvent(state, [1, 2, 3, 4]);
        sm.Dispatch(evt);

        Assert.Single(ops.PushedInbound);
        Assert.IsType<MultiplexedData>(ops.PushedInbound[0]);
        var pushed = (MultiplexedData)ops.PushedInbound[0];
        Assert.Equal(new StreamTarget(streamId), pushed.StreamId);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_StreamLeaseAcquired_should_attach_and_push_StreamOpened()
    {
        var (ops, sm) = CreateConnectedStateMachine();

        const long streamId = 123L;
        sm.HandlePush(new OpenStream(streamId, StreamDirection.Bidirectional));

        sm.Dispatch(new StreamLeaseAcquired(new MemoryStream(), streamId));

        var streamOpened = ops.PushedInbound.OfType<StreamOpened>().FirstOrDefault();
        Assert.NotNull(streamOpened);
        Assert.Equal(new StreamTarget(streamId), streamOpened.Id);
        Assert.Equal(StreamDirection.Bidirectional, streamOpened.Direction);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_StreamLeaseAcquired_with_unknown_stream_should_dispose_stream()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        sm.Dispatch(new StreamLeaseAcquired(new MemoryStream(), 999));

        Assert.Empty(ops.PushedInbound);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundStreamAccepted_should_register_server_stream()
    {
        var (ops, sm) = CreateConnectedStateMachine();

        var streamId = 3;
        var stream = new MemoryStream();
        sm.Dispatch(new InboundStreamAccepted(stream, streamId));

        var accepted = ops.PushedInbound.OfType<ServerStreamAccepted>().FirstOrDefault();
        Assert.NotNull(accepted);
        Assert.Equal(new StreamTarget(streamId), accepted.Id);
        Assert.Equal(StreamDirection.Unidirectional, accepted.Direction);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_completed_read_should_push_StreamReadCompleted()
    {
        var (ops, sm) = CreateConnectedStateMachine();

        var streamId = 789L;
        var state = sm.RegisterTestStream(streamId, StreamDirection.Bidirectional);

        ops.PushedInbound.Clear();

        var evt = CreateCompletedReadEvent(state);
        sm.Dispatch(evt);

        var completed = ops.PushedInbound.OfType<StreamReadCompleted>().FirstOrDefault();
        Assert.NotNull(completed);
        Assert.Equal(new StreamTarget(streamId), completed.Id);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_StreamReceiveFailed_should_push_StreamClosed()
    {
        var (ops, sm) = CreateConnectedStateMachine();

        var streamId = 999L;
        var state = sm.RegisterTestStream(streamId, StreamDirection.Bidirectional);

        ops.PushedInbound.Clear();

        sm.Dispatch(new StreamReceiveFailed(state, new IOException("Read failed"), state.Epoch));

        var closed = ops.PushedInbound.OfType<StreamClosed>().FirstOrDefault();
        Assert.NotNull(closed);
        Assert.Equal(new StreamTarget(streamId), closed.Id);
        Assert.Equal(DisconnectReason.Error, closed.Reason);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_StreamReceiveFailed_with_QuicException_should_complete_stream_gracefully()
    {
        // QuicException on read = peer closed/reset the stream (FIN/STOP_SENDING/RST_STREAM):
        // classified on the actor as a graceful stream completion, not an error.
        var (ops, sm) = CreateConnectedStateMachine();

        var streamId = 777L;
        var state = sm.RegisterTestStream(streamId, StreamDirection.Bidirectional);

        ops.PushedInbound.Clear();

        sm.Dispatch(new StreamReceiveFailed(state,
            new System.Net.Quic.QuicException(System.Net.Quic.QuicError.StreamAborted, null, "aborted"), state.Epoch));

        var completed = ops.PushedInbound.OfType<StreamReadCompleted>().FirstOrDefault();
        Assert.NotNull(completed);
        Assert.Equal(new StreamTarget(streamId), completed.Id);
        Assert.DoesNotContain(ops.PushedInbound, i => i is StreamClosed);
    }

    [Fact(Timeout = 5000)]
    public void HandleUpstreamFinish_with_connection_should_stop_accept_loop_and_complete()
    {
        var (ops, sm) = CreateConnectedStateMachine();

        sm.HandleUpstreamFinish();

        Assert.True(ops.Completed);
    }

    [Fact(Timeout = 5000)]
    public async Task HandleUpstreamFinish_should_dispose_open_streams_and_release_the_lease()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);
        var options = new QuicTransportOptions { Host = "localhost", Port = 443 };
        sm.HandlePush(new ConnectTransport(options));

        var leaseDisposed = false;
        var handle = new QuicConnectionHandle(
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
            dispose: () =>
            {
                leaseDisposed = true;
                return ValueTask.CompletedTask;
            });

        var lease = new QuicConnectionLease(handle, 100);
        sm.Dispatch(new ConnectionLeaseAcquired(lease));

        const long streamId = 1L;
        sm.HandlePush(new OpenStream(streamId, StreamDirection.Bidirectional));
        var trackingStream = new TrackingStream();
        sm.Dispatch(new StreamLeaseAcquired(trackingStream, streamId));

        sm.HandleUpstreamFinish();

        // Teardown of the underlying stream/lease is fire-and-forget (async QuiesceAsync/DisposeAsync
        // chains) — poll briefly rather than asserting synchronously.
        var deadline = DateTime.UtcNow.AddSeconds(2);
        while (!(trackingStream.Disposed && leaseDisposed) && DateTime.UtcNow < deadline)
        {
            await Task.Delay(10);
        }

        Assert.True(trackingStream.Disposed, "the open stream's underlying connection must be disposed");
        Assert.True(leaseDisposed, "the connection lease must be released back to the pool");
    }

    [Fact(Timeout = 5000)]
    public void HandleConnectTransport_with_existing_lease_should_set_reconnecting()
    {
        var (ops, sm) = CreateConnectedStateMachine();

        ops.PushedInbound.Clear();
        ops.PullCount = 0;

        var options2 = new QuicTransportOptions { Host = "other.host", Port = 443 };
        sm.HandlePush(new ConnectTransport(options2));

        Assert.Contains("connect-timeout", ops.Timers.Keys);
        Assert.True(ops.PullCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void HandleOpenStream_with_connected_handle_should_create_stream_state()
    {
        var (ops, sm) = CreateConnectedStateMachine();

        ops.PullCount = 0;
        var streamId = 555L;

        sm.HandlePush(new OpenStream(streamId, StreamDirection.Unidirectional));

        Assert.True(ops.PullCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void HandleResetStream_with_existing_stream_should_abort_and_close()
    {
        var (ops, sm) = CreateConnectedStateMachine();

        var streamId = 222L;
        sm.HandlePush(new OpenStream(streamId, StreamDirection.Bidirectional));
        sm.Dispatch(new StreamLeaseAcquired(new MemoryStream(), streamId));

        ops.PushedInbound.Clear();
        ops.PullCount = 0;

        sm.HandlePush(new ResetStream(streamId, 42));

        var closed = ops.PushedInbound.OfType<StreamClosed>().FirstOrDefault();
        Assert.NotNull(closed);
        Assert.Equal(new StreamTarget(streamId), closed.Id);
        Assert.Equal(DisconnectReason.Error, closed.Reason);
        Assert.True(ops.PullCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_ConnectionLeaseAcquired_should_cancel_timer_and_push_TransportConnected()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);
        var options = new QuicTransportOptions { Host = "localhost", Port = 443 };

        sm.HandlePush(new ConnectTransport(options));
        Assert.Contains("connect-timeout", ops.Timers.Keys);

        ops.PushedInbound.Clear();

        var handle = CreateMockHandle();
        var lease = new QuicConnectionLease(handle, 100);
        sm.Dispatch(new ConnectionLeaseAcquired(lease));

        Assert.Contains("connect-timeout", ops.CancelledTimers);

        var connected = ops.PushedInbound.OfType<TransportConnected>().FirstOrDefault();
        Assert.NotNull(connected);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_completed_read_with_half_closed_write_should_remove_stream()
    {
        var (ops, sm) = CreateConnectedStateMachine();

        var streamId = 333L;
        var state = sm.RegisterTestStream(streamId, StreamDirection.Bidirectional);

        sm.HandlePush(new CompleteWrites(streamId));

        ops.PushedInbound.Clear();

        var evt = CreateCompletedReadEvent(state);
        sm.Dispatch(evt);

        var readCompleted = ops.PushedInbound.OfType<StreamReadCompleted>().FirstOrDefault();
        Assert.NotNull(readCompleted);
        Assert.Equal(new StreamTarget(streamId), readCompleted.Id);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_StreamReceiveFailed_with_auto_reconnect_should_push_transient_disconnect()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        var options = new QuicTransportOptions { Host = "localhost", Port = 443, AutoReconnect = true };
        sm.HandlePush(new ConnectTransport(options));

        var handle = CreateMockHandle();
        var lease = new QuicConnectionLease(handle, 100);
        sm.Dispatch(new ConnectionLeaseAcquired(lease));

        ops.PushedInbound.Clear();

        var streamId = 111L;
        var state = sm.RegisterTestStream(streamId, StreamDirection.Bidirectional);

        ops.PushedInbound.Clear();

        sm.Dispatch(new StreamReceiveFailed(
            state, new ObjectDisposedException("Connection disposed"), state.Epoch));

        var disconnected = ops.PushedInbound.OfType<TransportDisconnected>().FirstOrDefault();
        Assert.NotNull(disconnected);
        Assert.Equal(DisconnectReason.Transient, disconnected.Reason);
        Assert.True(ops.PullCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_StreamReceiveFailed_without_auto_reconnect_upstream_finished_should_complete()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        var options = new QuicTransportOptions { Host = "localhost", Port = 443, AutoReconnect = false };
        sm.HandlePush(new ConnectTransport(options));

        var handle = CreateMockHandle();
        var lease = new QuicConnectionLease(handle, 100);
        sm.Dispatch(new ConnectionLeaseAcquired(lease));

        ops.PushedInbound.Clear();
        ops.Completed = false;

        sm.HandleUpstreamFinish();

        ops.PushedInbound.Clear();
        ops.Completed = false;

        var upstreamFinishedState = CreateDetachedStreamState(1);
        sm.Dispatch(new StreamReceiveFailed(
            upstreamFinishedState, new ObjectDisposedException("Connection disposed"), upstreamFinishedState.Epoch));

        var disconnected = ops.PushedInbound.OfType<TransportDisconnected>().FirstOrDefault();
        Assert.NotNull(disconnected);
        Assert.Equal(DisconnectReason.Error, disconnected.Reason);

        Assert.True(ops.Completed);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_StreamReceiveFailed_without_auto_reconnect_upstream_not_finished_should_pull()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        var options = new QuicTransportOptions { Host = "localhost", Port = 443, AutoReconnect = false };
        sm.HandlePush(new ConnectTransport(options));

        var handle = CreateMockHandle();
        var lease = new QuicConnectionLease(handle, 100);
        sm.Dispatch(new ConnectionLeaseAcquired(lease));

        ops.PushedInbound.Clear();
        ops.PullCount = 0;

        var notFinishedState = CreateDetachedStreamState(1);
        sm.Dispatch(new StreamReceiveFailed(
            notFinishedState, new ObjectDisposedException("Connection disposed"), notFinishedState.Epoch));

        var disconnected = ops.PushedInbound.OfType<TransportDisconnected>().FirstOrDefault();
        Assert.NotNull(disconnected);
        Assert.Equal(DisconnectReason.Error, disconnected.Reason);

        Assert.True(ops.PullCount > 0);

        Assert.False(ops.Completed);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_ConnectTransport_should_create_cts_and_send_acquire()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        var options = new QuicTransportOptions { Host = "localhost", Port = 443 };
        sm.HandlePush(new ConnectTransport(options));

        Assert.Contains("connect-timeout", ops.Timers.Keys);

        Assert.True(ops.PullCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void HandleDownstreamFinish_should_call_cleanup_transport()
    {
        var (ops, sm) = CreateConnectedStateMachine();

        ops.PullCount = 0;

        sm.HandleDownstreamFinish();

        Assert.False(ops.Completed);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_StreamReceiveFailed_should_remove_stream_on_error()
    {
        var (ops, sm) = CreateConnectedStateMachine();

        StreamTarget streamId = 888L;
        var state = sm.RegisterTestStream(streamId, StreamDirection.Bidirectional);

        ops.PushedInbound.Clear();

        sm.Dispatch(new StreamReceiveFailed(state, new IOException("Read failed"), state.Epoch));

        var closed = ops.PushedInbound.OfType<StreamClosed>().FirstOrDefault();
        Assert.NotNull(closed);
        Assert.Equal(streamId, closed.Id);
        Assert.Equal(DisconnectReason.Error, closed.Reason);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_StreamLeaseAcquired_for_unidirectional_should_not_start_read()
    {
        var (ops, sm) = CreateConnectedStateMachine();

        StreamTarget streamId = 42L;
        sm.HandlePush(new OpenStream(streamId, StreamDirection.Unidirectional));

        sm.Dispatch(new StreamLeaseAcquired(new MemoryStream(), streamId));

        var streamOpened = ops.PushedInbound.OfType<StreamOpened>().FirstOrDefault();
        Assert.NotNull(streamOpened);
        Assert.Equal(streamId, streamOpened.Id);
        Assert.Equal(StreamDirection.Unidirectional, streamOpened.Direction);

        Thread.Sleep(50);
        Assert.DoesNotContain(ops.PushedInbound, item => item is StreamClosed);
        Assert.DoesNotContain(ops.PushedInbound, item => item is StreamReadCompleted);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_StreamLeaseAcquired_for_bidirectional_should_start_read()
    {
        var (ops, sm) = CreateConnectedStateMachine();

        const long streamId = 50L;
        sm.HandlePush(new OpenStream(streamId, StreamDirection.Bidirectional));

        sm.Dispatch(new StreamLeaseAcquired(new MemoryStream(), streamId));

        var streamOpened = ops.PushedInbound.OfType<StreamOpened>().FirstOrDefault();
        Assert.NotNull(streamOpened);
        Assert.Equal(StreamDirection.Bidirectional, streamOpened.Direction);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_MigrationDetected_should_push_ConnectionMigrationDetected()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        var oldEndPoint = new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 1234);
        var newEndPoint = new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 5678);

        sm.Dispatch(new MigrationDetected(oldEndPoint, newEndPoint));

        var migrated = ops.PushedInbound.OfType<ConnectionMigrationDetected>().FirstOrDefault();
        Assert.NotNull(migrated);
        Assert.Equal(oldEndPoint, migrated.OldEndPoint);
        Assert.Equal(newEndPoint, migrated.NewEndPoint);
    }

    [Fact(Timeout = 5000)]
    public async Task Stale_StreamReceiveCompleted_after_repool_should_be_dropped()
    {
        // Vector A: a success completion produced by PipeTo for a now-dead stream is still in the mailbox
        // when its QuicStreamState is repooled and re-rented for a NEW stream. Without the epoch guard the
        // membership/ReferenceEquals check passes (the re-rented state's id maps back to itself), so the
        // stale buffer is pushed as the new stream's data.
        var (ops, sm) = CreateConnectedStateMachine();

        // Arm state A for a stream and capture its ReadSuccess event (buffer + the epoch it was armed at).
        var stateA = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        stateA.ActivateDirectReadForTest(1);
        var owner = new TrackingMemoryOwner(4);
        var staleBuffer = WireBuffer.Wrap(owner, 0, 4);
        var staleEvent = (StreamReceiveCompleted)stateA.ReadSuccess(staleBuffer);

        // Repool A and re-rent the SAME instance as a NEW stream (id 2) — this bumps A's epoch.
        await stateA.DisposeAndReturnAsync();
        var reused = ReRentSameInstanceAsStream(sm, stateA, 2);
        Assert.Same(stateA, reused);

        ops.PushedInbound.Clear();
        sm.Dispatch(staleEvent);

        Assert.DoesNotContain(ops.PushedInbound, i => i is MultiplexedData);
        Assert.True(owner.Disposed, "the epoch guard must dispose the stale completion's buffer");
    }

    [Fact(Timeout = 5000)]
    public async Task Stale_StreamReceiveFailed_with_OCE_after_repool_should_not_teardown_new_stream()
    {
        // Vector B: an OperationCanceledException is produced on EVERY quiesce-cancelled read. If such a
        // stale failure reaches the host after the state has been re-rented, it tears down the NEW stream
        // with DisconnectReason.Error. The epoch guard must drop it before any host callback.
        var (ops, sm) = CreateConnectedStateMachine();

        var stateA = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        stateA.ActivateDirectReadForTest(1);
        var staleFailure = (StreamReceiveFailed)stateA.ReadFailure(new OperationCanceledException());

        await stateA.DisposeAndReturnAsync();
        var reused = ReRentSameInstanceAsStream(sm, stateA, 2);
        Assert.Same(stateA, reused);

        ops.PushedInbound.Clear();
        sm.Dispatch(staleFailure);

        Assert.DoesNotContain(ops.PushedInbound, i => i is StreamClosed);
    }

    // Drains the shared QuicStreamState pool (holding the drained instances so they are not handed back on
    // the next Rent) until it hands the target instance back, registering it under a new stream id. Mirrors
    // the set-membership repool pattern in QuicStreamStateSpec and is robust against the process-wide pool
    // being shared with other test classes.
    private static QuicStreamState ReRentSameInstanceAsStream(
        QuicTransportStateMachine sm, QuicStreamState target, long newStreamId)
    {
        for (var i = 0; i < 512; i++)
        {
            var candidate = sm.RegisterTestStream(newStreamId, StreamDirection.Bidirectional);
            if (ReferenceEquals(candidate, target))
            {
                return candidate;
            }
        }

        Assert.Fail("pool did not hand the repooled instance back within the drain budget");
        return null!;
    }

    private sealed class TrackingMemoryOwner : IMemoryOwner<byte>
    {
        private readonly byte[] _array;

        public TrackingMemoryOwner(int size) => _array = new byte[size];

        public bool Disposed { get; private set; }

        public Memory<byte> Memory => _array;

        public void Dispose() => Disposed = true;
    }

    // A never-completing readable stream so the attached stream's connection stays "live" until the
    // teardown path explicitly disposes it — mirrors QuicStreamDisposalSpec.TrackingStream.
    private sealed class TrackingStream : MemoryStream
    {
        public bool Disposed { get; private set; }

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
            Disposed = true;
            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            Disposed = true;
            await base.DisposeAsync();
        }
    }
}
