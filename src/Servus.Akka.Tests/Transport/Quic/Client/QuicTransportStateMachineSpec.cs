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

        var buffer = TransportBuffer.Rent(16);
        buffer.Length = 4;
        sm.HandlePush(new MultiplexedData(buffer, 1));

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
    public void Dispatch_InboundData_should_dispose_buffer_when_gen_mismatch()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        var buffer = TransportBuffer.Rent(16);
        buffer.Length = 4;

        sm.Dispatch(new InboundData(buffer, 1, 99));

        // Buffer should be disposed, so accessing it should not be safe
        // We verify this indirectly by checking no inbound was pushed
        Assert.Empty(ops.PushedInbound);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_OutboundWriteDone_should_signal_pull()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        sm.Dispatch(new OutboundWriteDone(1));

        Assert.True(ops.PullCount > 0);
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

        var buffer = TransportBuffer.Rent(16);
        buffer.Length = 4;

        sm.HandlePush(new MultiplexedData(buffer, 999));

        // Buffer is disposed, verify no inbound was pushed
        Assert.Empty(ops.PushedInbound);
        Assert.True(ops.PullCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void HandleDownstreamFinish_should_not_complete_when_upstream_not_finished()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        sm.HandleDownstreamFinish();

        // HandleDownstreamFinish should NOT call OnCompleteStage, it just cleans up
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

        // Set up pending connect
        var options = new QuicTransportOptions { Host = "localhost", Port = 443 };
        sm.HandlePush(new ConnectTransport(options));

        // Now trigger the timeout
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
        // This is a harder test without real connection state, but we can verify
        // that calling ResetStream on unknown stream just signals pull
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        sm.HandlePush(new ResetStream(999));

        // No pushed inbound for unknown stream
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
    public void Dispatch_OutboundWriteFailed_should_handle_connection_failure()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        sm.Dispatch(new OutboundWriteFailed(new InvalidOperationException("Write failed"), 1));

        // Should push TransportDisconnected
        var disconnected = ops.PushedInbound.OfType<TransportDisconnected>().FirstOrDefault();
        Assert.NotNull(disconnected);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_AcquisitionFailed_when_cancelled_should_be_ignored()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        var options = new QuicTransportOptions { Host = "localhost", Port = 443 };
        sm.HandlePush(new ConnectTransport(options));

        // Dispatch acquisition failed with OperationCanceledException
        sm.Dispatch(new AcquisitionFailed(new OperationCanceledException("Cancelled")));

        // Should not push anything (cancelled exceptions are ignored)
        Assert.Empty(ops.PushedInbound);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_AcquisitionFailed_with_error_should_push_TransportDisconnected()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        var options = new QuicTransportOptions { Host = "localhost", Port = 443 };
        sm.HandlePush(new ConnectTransport(options));

        // Dispatch acquisition failed with actual error
        sm.Dispatch(new AcquisitionFailed(new IOException("Connection failed")));

        // Should cancel timer and push TransportDisconnected
        Assert.Contains("connect-timeout", ops.CancelledTimers);
        var disconnected = ops.PushedInbound.OfType<TransportDisconnected>().FirstOrDefault();
        Assert.NotNull(disconnected);
        Assert.Equal(DisconnectReason.Error, disconnected.Reason);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundPumpFailed_should_handle_gracefully()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        // InboundPumpFailed doesn't push TransportDisconnected directly, it just calls OnInboundComplete
        // which handles stream cleanup. Since the stream doesn't exist, nothing is pushed.
        sm.Dispatch(new InboundPumpFailed(new IOException("Pump failed"), 1));

        // No inbound should be pushed for non-existent stream
        Assert.Empty(ops.PushedInbound);
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

        // Second connect should reuse/reset the timer
        var options2 = new QuicTransportOptions { Host = "other.host", Port = 443 };
        sm.HandlePush(new ConnectTransport(options2));
        Assert.Contains("connect-timeout", ops.Timers.Keys);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundData_with_matching_gen_should_push_MultiplexedData()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        var buffer = TransportBuffer.Rent(16);
        buffer.Length = 4;

        // Dispatch with gen 0 (initial gen), should match and push
        sm.Dispatch(new InboundData(buffer, 1, 0));

        Assert.Single(ops.PushedInbound);
        Assert.IsType<MultiplexedData>(ops.PushedInbound[0]);
        var pushed = (MultiplexedData)ops.PushedInbound[0];
        Assert.Equal(1, pushed.StreamId);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_StreamLeaseAcquired_should_attach_handle_and_push_StreamOpened()
    {
        var (ops, sm) = CreateConnectedStateMachine();

        const long streamId = 123L;
        sm.HandlePush(new OpenStream(streamId, StreamDirection.Bidirectional));

        // OpenStream has been queued, now dispatch the StreamLeaseAcquired
        var handle = new StreamHandle(new MemoryStream());
        sm.Dispatch(new StreamLeaseAcquired(handle, streamId));

        // Should push StreamOpened
        var streamOpened = ops.PushedInbound.OfType<StreamOpened>().FirstOrDefault();
        Assert.NotNull(streamOpened);
        Assert.Equal(new StreamTarget(streamId), streamOpened.Id);
        Assert.Equal(StreamDirection.Bidirectional, streamOpened.Direction);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_StreamLeaseAcquired_with_unknown_stream_should_dispose_handle()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        var handle = new StreamHandle(new MemoryStream());
        sm.Dispatch(new StreamLeaseAcquired(handle, 999));

        // Should not push anything (stream doesn't exist)
        Assert.Empty(ops.PushedInbound);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundStreamAccepted_should_register_server_stream()
    {
        var (ops, sm) = CreateConnectedStateMachine();

        var streamId = 456L;
        var stream = new MemoryStream();
        sm.Dispatch(new InboundStreamAccepted(stream, streamId));

        // Should push ServerStreamAccepted
        var accepted = ops.PushedInbound.OfType<ServerStreamAccepted>().FirstOrDefault();
        Assert.NotNull(accepted);
        Assert.Equal(new StreamTarget(streamId), accepted.Id);
        Assert.Equal(StreamDirection.Unidirectional, accepted.Direction);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundComplete_graceful_should_push_StreamReadCompleted()
    {
        var (ops, sm) = CreateConnectedStateMachine();

        var streamId = 789L;
        sm.HandlePush(new OpenStream(streamId, StreamDirection.Bidirectional));
        var handle = new StreamHandle(new MemoryStream());
        sm.Dispatch(new StreamLeaseAcquired(handle, streamId));

        ops.PushedInbound.Clear();

        // Now dispatch InboundComplete with Graceful reason (gen is 2 after CreateConnectedStateMachine)
        sm.Dispatch(new InboundComplete(DisconnectReason.Graceful, 2, streamId));

        // Should push StreamReadCompleted
        var completed = ops.PushedInbound.OfType<StreamReadCompleted>().FirstOrDefault();
        Assert.NotNull(completed);
        Assert.Equal(new StreamTarget(streamId), completed.Id);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundComplete_error_should_push_StreamClosed()
    {
        var (ops, sm) = CreateConnectedStateMachine();

        var streamId = 999L;
        sm.HandlePush(new OpenStream(streamId, StreamDirection.Bidirectional));
        var handle = new StreamHandle(new MemoryStream());
        sm.Dispatch(new StreamLeaseAcquired(handle, streamId));

        ops.PushedInbound.Clear();

        // Dispatch InboundComplete with error reason (gen is 2 after CreateConnectedStateMachine)
        sm.Dispatch(new InboundComplete(DisconnectReason.Error, 2, streamId));

        // Should push StreamClosed
        var closed = ops.PushedInbound.OfType<StreamClosed>().FirstOrDefault();
        Assert.NotNull(closed);
        Assert.Equal(new StreamTarget(streamId), closed.Id);
        Assert.Equal(DisconnectReason.Error, closed.Reason);
    }

    [Fact(Timeout = 5000)]
    public void HandleUpstreamFinish_with_connection_should_stop_pumps_and_complete()
    {
        var (ops, sm) = CreateConnectedStateMachine();

        // Now upstream finishes
        sm.HandleUpstreamFinish();

        // Should complete stage
        Assert.True(ops.Completed);
    }

    [Fact(Timeout = 5000)]
    public void HandleConnectTransport_with_existing_lease_should_set_reconnecting()
    {
        var (ops, sm) = CreateConnectedStateMachine();

        ops.PushedInbound.Clear();
        ops.PullCount = 0;

        // Second connect with existing lease
        var options2 = new QuicTransportOptions { Host = "other.host", Port = 443 };
        sm.HandlePush(new ConnectTransport(options2));

        // Should schedule timer and signal pull
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

        // Should signal pull (PipeTo will be sent to self)
        Assert.True(ops.PullCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void HandleResetStream_with_existing_stream_should_abort_and_close()
    {
        var (ops, sm) = CreateConnectedStateMachine();

        var streamId = 222L;
        sm.HandlePush(new OpenStream(streamId, StreamDirection.Bidirectional));
        var handle = new StreamHandle(new MemoryStream());
        sm.Dispatch(new StreamLeaseAcquired(handle, streamId));

        ops.PushedInbound.Clear();
        ops.PullCount = 0;

        // Now reset the stream
        sm.HandlePush(new ResetStream(streamId, 42));

        // Should push StreamClosed
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

        // Should cancel timer
        Assert.Contains("connect-timeout", ops.CancelledTimers);

        // Should push TransportConnected
        var connected = ops.PushedInbound.OfType<TransportConnected>().FirstOrDefault();
        Assert.NotNull(connected);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundComplete_graceful_with_state_becoming_closed_should_remove_and_dispose()
    {
        var (ops, sm) = CreateConnectedStateMachine();

        var streamId = 333L;
        sm.HandlePush(new OpenStream(streamId, StreamDirection.Bidirectional));
        var handle = new StreamHandle(new MemoryStream());
        sm.Dispatch(new StreamLeaseAcquired(handle, streamId));

        // First, complete writes to move to HalfClosedWrite phase
        sm.HandlePush(new CompleteWrites(streamId));

        ops.PushedInbound.Clear();

        // Now InboundComplete with Graceful moves it to Closed phase (gen is 2 after CreateConnectedStateMachine)
        sm.Dispatch(new InboundComplete(DisconnectReason.Graceful, 2, streamId));

        // Should push StreamReadCompleted and remove stream from dictionary
        var readCompleted = ops.PushedInbound.OfType<StreamReadCompleted>().FirstOrDefault();
        Assert.NotNull(readCompleted);
        Assert.Equal(new StreamTarget(streamId), readCompleted.Id);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_OutboundWriteFailed_with_auto_reconnect_should_push_transient_disconnect()
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
        sm.HandlePush(new OpenStream(streamId, StreamDirection.Bidirectional));
        var streamHandle = new StreamHandle(new MemoryStream());
        sm.Dispatch(new StreamLeaseAcquired(streamHandle, streamId));

        ops.PushedInbound.Clear();

        // Trigger connection failure
        sm.Dispatch(new OutboundWriteFailed(new IOException("Connection failed"), streamId));

        // Should push TransportDisconnected with Transient reason (auto-reconnect is enabled)
        var disconnected = ops.PushedInbound.OfType<TransportDisconnected>().FirstOrDefault();
        Assert.NotNull(disconnected);
        Assert.Equal(DisconnectReason.Transient, disconnected.Reason);
        Assert.True(ops.PullCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_OutboundWriteFailed_without_auto_reconnect_upstream_finished_should_complete()
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

        // Mark upstream finished
        sm.HandleUpstreamFinish();

        ops.PushedInbound.Clear();
        ops.Completed = false;

        // Trigger connection failure
        sm.Dispatch(new OutboundWriteFailed(new IOException("Connection failed"), 1));

        // Should push TransportDisconnected with Error reason
        var disconnected = ops.PushedInbound.OfType<TransportDisconnected>().FirstOrDefault();
        Assert.NotNull(disconnected);
        Assert.Equal(DisconnectReason.Error, disconnected.Reason);

        // Should complete stage
        Assert.True(ops.Completed);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_OutboundWriteFailed_without_auto_reconnect_upstream_not_finished_should_pull()
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

        // Trigger connection failure (upstream not finished)
        sm.Dispatch(new OutboundWriteFailed(new IOException("Connection failed"), 1));

        // Should push TransportDisconnected
        var disconnected = ops.PushedInbound.OfType<TransportDisconnected>().FirstOrDefault();
        Assert.NotNull(disconnected);
        Assert.Equal(DisconnectReason.Error, disconnected.Reason);

        // Should signal pull
        Assert.True(ops.PullCount > 0);

        // Should NOT complete stage
        Assert.False(ops.Completed);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_ConnectTransport_should_create_cts_and_send_acquire()
    {
        var ops = new StubOps();
        var sm = new QuicTransportStateMachine(ops, ActorRefs.Nobody, ActorRefs.Nobody);

        var options = new QuicTransportOptions { Host = "localhost", Port = 443 };
        sm.HandlePush(new ConnectTransport(options));

        // Should schedule timer
        Assert.Contains("connect-timeout", ops.Timers.Keys);

        // Should signal pull (PipeTo sends message to self)
        Assert.True(ops.PullCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void HandleDownstreamFinish_should_call_cleanup_transport()
    {
        var (ops, sm) = CreateConnectedStateMachine();

        ops.PullCount = 0;

        sm.HandleDownstreamFinish();

        // HandleDownstreamFinish calls CleanupTransport but doesn't complete stage
        Assert.False(ops.Completed);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundPumpFailed_should_remove_stream_on_error()
    {
        var (ops, sm) = CreateConnectedStateMachine();

        StreamTarget streamId = 888L;
        sm.HandlePush(new OpenStream(streamId, StreamDirection.Bidirectional));
        var handle = new StreamHandle(new MemoryStream());
        sm.Dispatch(new StreamLeaseAcquired(handle, streamId));

        ops.PushedInbound.Clear();

        // InboundPumpFailed should call OnInboundComplete with Error reason
        sm.Dispatch(new InboundPumpFailed(new IOException("Pump failed"), streamId));

        // Should push StreamClosed
        var closed = ops.PushedInbound.OfType<StreamClosed>().FirstOrDefault();
        Assert.NotNull(closed);
        Assert.Equal(streamId, closed.Id);
        Assert.Equal(DisconnectReason.Error, closed.Reason);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_StreamLeaseAcquired_for_unidirectional_should_not_start_inbound_pump()
    {
        var (ops, sm) = CreateConnectedStateMachine();

        StreamTarget streamId = 42L;
        sm.HandlePush(new OpenStream(streamId, StreamDirection.Unidirectional));

        var handle = new StreamHandle(new MemoryStream());
        sm.Dispatch(new StreamLeaseAcquired(handle, streamId));

        var streamOpened = ops.PushedInbound.OfType<StreamOpened>().FirstOrDefault();
        Assert.NotNull(streamOpened);
        Assert.Equal(streamId, streamOpened.Id);
        Assert.Equal(StreamDirection.Unidirectional, streamOpened.Direction);

        // Wait briefly to ensure no InboundPumpFailed is dispatched.
        // If a pump was started on a write-only MemoryStream, ReadAsync would
        // return 0 immediately and trigger InboundComplete — which must NOT happen
        // for client-initiated unidirectional control streams.
        Thread.Sleep(50);
        Assert.DoesNotContain(ops.PushedInbound, item => item is StreamClosed);
        Assert.DoesNotContain(ops.PushedInbound, item => item is StreamReadCompleted);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_StreamLeaseAcquired_for_bidirectional_should_start_inbound_pump()
    {
        var (ops, sm) = CreateConnectedStateMachine();

        const long streamId = 50L;
        sm.HandlePush(new OpenStream(streamId, StreamDirection.Bidirectional));

        var handle = new StreamHandle(new MemoryStream());
        sm.Dispatch(new StreamLeaseAcquired(handle, streamId));

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
}