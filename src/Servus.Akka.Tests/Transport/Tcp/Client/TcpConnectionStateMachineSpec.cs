using Akka.Actor;
using Servus.Akka.Tests.Utils;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Tcp;
using Servus.Akka.Transport.Tcp.Client;

namespace Servus.Akka.Tests.Transport.Tcp.Client;

public sealed class TcpConnectionStateMachineSpec
{
    private static readonly TcpTransportOptions TestOptions = new()
    {
        Host = "localhost",
        Port = 8080
    };

    private static readonly IPoolingStrategy TestStrategy = new TestPoolingStrategy();

    private static (TcpConnectionStateMachine Sm, MockConnectionOperations Ops) CreateStateMachine()
    {
        var ops = new MockConnectionOperations();
        var sm = new TcpConnectionStateMachine(
            ops,
            ActorRefs.Nobody,
            TestStrategy,
            ActorRefs.Nobody);
        return (sm, ops);
    }

    private static ConnectionLease CreateTestLease()
    {
        var connection = SocketPipeConnection.CreateInert();
        var cts = new CancellationTokenSource();
        return new ConnectionLease(connection, cts, ConnectionInfo.None);
    }

    private static WireBuffer CreateTestBuffer(params byte[] data)
    {
        var buf = WireBuffer.Rent(data.Length);
        data.CopyTo(buf.FullMemory.Span);
        buf.Length = data.Length;
        return buf;
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_ConnectTransport_should_start_acquisition()
    {
        var (sm, ops) = CreateStateMachine();

        sm.HandlePush(new ConnectTransport(TestOptions));

        Assert.Contains(ops.ScheduledTimers, t => t.Key == "connect-timeout");
        Assert.True(ops.PullOutboundCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_ConnectTransport_should_schedule_connect_timeout()
    {
        var (sm, ops) = CreateStateMachine();

        sm.HandlePush(new ConnectTransport(TestOptions));

        var timer = ops.ScheduledTimers.First(t => t.Key == "connect-timeout");
        Assert.Equal(TimeSpan.FromSeconds(10), timer.Delay);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_ConnectTransport_with_custom_timeout_should_use_custom_value()
    {
        var (sm, ops) = CreateStateMachine();
        var options = new TcpTransportOptions { Host = "localhost", Port = 8080, ConnectTimeout = TimeSpan.FromSeconds(5) };

        sm.HandlePush(new ConnectTransport(options));

        var timer = ops.ScheduledTimers.First(t => t.Key == "connect-timeout");
        Assert.Equal(TimeSpan.FromSeconds(5), timer.Delay);
    }

    [Fact(Timeout = 5000)]
    public void RequestRead_should_be_callable_after_connect()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();

        sm.Dispatch(new LeaseAcquired(lease));

        // Should not throw
        sm.RequestRead();
    }

    [Fact(Timeout = 5000)]
    public void RequestRead_without_connection_should_be_noop()
    {
        var (sm, ops) = CreateStateMachine();

        // Should not throw when no connection
        sm.RequestRead();

        Assert.Empty(ops.PushedInbound);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_TransportData_should_write_to_pipe_when_connected()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));

        var buffer = CreateTestBuffer(7, 8, 9);
        var pullBefore = ops.PullOutboundCount;
        sm.HandlePush(TransportData.Rent(buffer));

        Assert.True(ops.PullOutboundCount > pullBefore);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_TransportData_without_connection_should_buffer()
    {
        var (sm, ops) = CreateStateMachine();

        sm.HandlePush(new ConnectTransport(TestOptions));
        var pullBefore = ops.PullOutboundCount;

        var buffer = CreateTestBuffer(1, 2, 3);
        sm.HandlePush(TransportData.Rent(buffer));

        Assert.True(ops.PullOutboundCount > pullBefore);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_DisconnectTransport_should_cleanup()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));
        var pullBefore = ops.PullOutboundCount;

        sm.HandlePush(new DisconnectTransport(DisconnectReason.Graceful));

        Assert.True(ops.PullOutboundCount > pullBefore);
        Assert.False(lease.IsAlive());
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_DisconnectTransport_without_connection_should_signal_pull()
    {
        var (sm, ops) = CreateStateMachine();
        var pullBefore = ops.PullOutboundCount;

        sm.HandlePush(new DisconnectTransport(DisconnectReason.Graceful));

        Assert.True(ops.PullOutboundCount > pullBefore);
    }

    [Fact(Timeout = 5000)]
    public void PostStop_should_force_return_leases()
    {
        var (sm, ops) = CreateStateMachine();

        sm.PostStop();

        Assert.Contains(ops.CancelledTimers, k => k == "connect-timeout");
    }

    [Fact(Timeout = 5000)]
    public void PostStop_with_active_lease_should_cleanup()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));

        sm.PostStop();

        Assert.False(lease.IsAlive());
        Assert.Contains(ops.CancelledTimers, k => k == "connect-timeout");
    }

    [Fact(Timeout = 5000)]
    public void PostStop_with_pending_writes_should_dispose_all()
    {
        var (sm, ops) = CreateStateMachine();

        sm.HandlePush(new ConnectTransport(TestOptions));
        var buf1 = CreateTestBuffer(1, 2);
        var buf2 = CreateTestBuffer(3, 4);
        sm.HandlePush(TransportData.Rent(buf1));
        sm.HandlePush(TransportData.Rent(buf2));

        sm.PostStop();

        Assert.Contains(ops.CancelledTimers, k => k == "connect-timeout");
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_LeaseAcquired_should_signal_pull_outbound()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();

        sm.Dispatch(new LeaseAcquired(lease));

        Assert.True(ops.PullOutboundCount > 0);
        Assert.Contains(ops.CancelledTimers, k => k == "connect-timeout");
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_LeaseAcquired_with_pending_writes_should_flush()
    {
        var (sm, ops) = CreateStateMachine();

        sm.HandlePush(new ConnectTransport(TestOptions));

        var buffer = CreateTestBuffer(1, 2, 3);
        sm.HandlePush(TransportData.Rent(buffer));

        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));

        Assert.Contains(ops.CancelledTimers, k => k == "connect-timeout");
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_LeaseAcquired_first_time_should_not_signal_connected()
    {
        var (sm, ops) = CreateStateMachine();

        sm.HandlePush(new ConnectTransport(TestOptions));
        ops.PushedInbound.Clear();

        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));

        Assert.DoesNotContain(ops.PushedInbound, item => item is TransportConnected);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_LeaseAcquired_after_reconnect_should_signal_connected()
    {
        var (sm, ops) = CreateStateMachine();

        sm.HandlePush(new ConnectTransport(TestOptions));
        var lease1 = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease1));

        sm.HandlePush(new ConnectTransport(TestOptions));
        ops.PushedInbound.Clear();

        var lease2 = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease2));

        Assert.Contains(ops.PushedInbound, item => item is TransportConnected);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_DisconnectTransport_should_echo_disconnected()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));
        ops.PushedInbound.Clear();

        sm.HandlePush(new DisconnectTransport(DisconnectReason.Error));

        Assert.Contains(ops.PushedInbound, item => item is TransportDisconnected { Reason: DisconnectReason.Error });
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_DisconnectTransport_without_connection_should_echo_disconnected()
    {
        var (sm, ops) = CreateStateMachine();
        ops.PushedInbound.Clear();

        sm.HandlePush(new DisconnectTransport(DisconnectReason.Graceful));

        Assert.Contains(ops.PushedInbound, item => item is TransportDisconnected { Reason: DisconnectReason.Graceful });
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_LeaseAcquired_after_disconnect_transport_should_signal_connected()
    {
        var (sm, ops) = CreateStateMachine();
        var lease1 = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease1));

        sm.HandlePush(new DisconnectTransport(DisconnectReason.Error));
        sm.HandlePush(new ConnectTransport(TestOptions));
        ops.PushedInbound.Clear();

        var lease2 = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease2));

        Assert.Contains(ops.PushedInbound, item => item is TransportConnected);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_LeaseAcquired_after_failed_first_acquisition_should_signal_connected()
    {
        var (sm, ops) = CreateStateMachine();

        sm.HandlePush(new ConnectTransport(TestOptions));
        sm.Dispatch(new AcquisitionFailed(new IOException("connection refused")));

        sm.HandlePush(new ConnectTransport(TestOptions));
        ops.PushedInbound.Clear();

        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));

        Assert.Contains(ops.PushedInbound, item => item is TransportConnected);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_AcquisitionFailed_should_push_disconnected_and_pull()
    {
        var (sm, ops) = CreateStateMachine();

        sm.HandlePush(new ConnectTransport(TestOptions));
        ops.PushedInbound.Clear();
        var pullBefore = ops.PullOutboundCount;

        sm.Dispatch(new AcquisitionFailed(new IOException("connection refused")));

        Assert.Contains(ops.PushedInbound, item => item is TransportDisconnected { Reason: DisconnectReason.Error });
        Assert.True(ops.PullOutboundCount > pullBefore);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_AcquisitionFailed_cancelled_should_be_ignored()
    {
        var (sm, ops) = CreateStateMachine();

        sm.HandlePush(new ConnectTransport(TestOptions));
        ops.PushedInbound.Clear();
        var pullBefore = ops.PullOutboundCount;

        sm.Dispatch(new AcquisitionFailed(new OperationCanceledException()));

        Assert.Empty(ops.PushedInbound);
        Assert.Equal(pullBefore, ops.PullOutboundCount);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_AcquisitionFailed_without_pending_connect_should_be_ignored()
    {
        var (sm, ops) = CreateStateMachine();
        ops.PushedInbound.Clear();

        sm.Dispatch(new AcquisitionFailed(new IOException("connection refused")));

        Assert.Empty(ops.PushedInbound);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_ReadCompleted_stale_gen_should_be_ignored()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));
        ops.PushedInbound.Clear();

        var buffer = CreateTestBuffer(1, 2, 3);
        sm.Dispatch(new ReadCompleted(buffer, 999));

        Assert.Empty(ops.PushedInbound);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_ReadFailed_stale_gen_should_be_ignored()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));
        ops.PushedInbound.Clear();

        sm.Dispatch(new ReadFailed(new IOException("read error"), 999));

        Assert.Empty(ops.PushedInbound);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_ReadCompleted_with_data_should_push_lease()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));
        ops.PushedInbound.Clear();

        var buffer = CreateTestBuffer(1, 2, 3);
        var gen = 1;

        sm.Dispatch(new ReadCompleted(buffer, gen));

        Assert.Single(ops.PushedInbound);
        var pushed = ops.PushedInbound[0];
        var transportData = Assert.IsType<TransportData>(pushed);
        Assert.Equal(3, transportData.Buffer.Length);

        transportData.Buffer.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_ReadCompleted_completed_should_push_disconnected()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));
        ops.PushedInbound.Clear();

        sm.Dispatch(new ReadCompleted(null, 1));

        Assert.Contains(ops.PushedInbound, item => item is TransportDisconnected { Reason: DisconnectReason.Graceful });
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_ReadFailed_should_push_disconnected_error()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));
        ops.PushedInbound.Clear();

        sm.Dispatch(new ReadFailed(new IOException("read error"), 1));

        Assert.Contains(ops.PushedInbound, item => item is TransportDisconnected { Reason: DisconnectReason.Error });
    }

    [Fact(Timeout = 5000)]
    public void HandleUpstreamFinish_without_connection_should_complete_stage()
    {
        var (sm, ops) = CreateStateMachine();

        sm.HandleUpstreamFinish();

        Assert.Equal(1, ops.CompleteStageCount);
    }

    [Fact(Timeout = 5000)]
    public void HandleUpstreamFinish_with_idle_connection_should_complete_stage()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));

        sm.HandleUpstreamFinish();

        Assert.Equal(1, ops.CompleteStageCount);
    }

    [Fact(Timeout = 5000)]
    public void HandleDownstreamFinish_should_cleanup_transport()
    {
        var (sm, _) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));

        sm.HandleDownstreamFinish();

        Assert.False(lease.IsAlive());
    }

    [Fact(Timeout = 5000)]
    public void OnTimer_connect_timeout_should_push_disconnected()
    {
        var (sm, ops) = CreateStateMachine();
        sm.HandlePush(new ConnectTransport(TestOptions));
        ops.PushedInbound.Clear();

        sm.OnTimer("connect-timeout");

        Assert.Contains(ops.PushedInbound, item => item is TransportDisconnected { Reason: DisconnectReason.Timeout });
    }

    [Fact(Timeout = 5000)]
    public void OnTimer_unknown_key_should_be_ignored()
    {
        var (sm, ops) = CreateStateMachine();

        sm.OnTimer("unknown-timer");

        Assert.Empty(ops.PushedInbound);
    }

    [Fact(Timeout = 5000)]
    public void OnTimer_without_pending_connect_should_be_ignored()
    {
        var (sm, ops) = CreateStateMachine();

        sm.OnTimer("connect-timeout");

        Assert.Empty(ops.PushedInbound);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_ConnectTransport_with_existing_lease_should_reconnect()
    {
        var (sm, ops) = CreateStateMachine();
        var lease1 = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease1));

        sm.HandlePush(new ConnectTransport(TestOptions));

        Assert.False(lease1.IsAlive());
        Assert.Contains(ops.ScheduledTimers, t => t.Key == "connect-timeout");
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_ConnectTransport_with_tcp_options_should_set_auto_reconnect()
    {
        var (sm, ops) = CreateStateMachine();
        var options = new TcpTransportOptions { Host = "localhost", Port = 8080, AutoReconnect = true };

        sm.HandlePush(new ConnectTransport(options));

        Assert.Contains(ops.ScheduledTimers, t => t.Key == "connect-timeout");
    }

    [Fact(Timeout = 5000)]
    public void HandleUpstreamFinish_with_idle_connection_should_complete_stage_immediately()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));

        sm.HandleUpstreamFinish();

        Assert.Equal(1, ops.CompleteStageCount);

        // Stale gen events should be silently ignored after completion
        ops.PushedInbound.Clear();
        sm.Dispatch(new ReadFailed(new IOException("stale"), 1));
        Assert.Empty(ops.PushedInbound);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_ReadFailed_without_upstream_finished_should_signal_pull()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));
        ops.PushedInbound.Clear();
        var pullBefore = ops.PullOutboundCount;

        sm.Dispatch(new ReadFailed(new IOException("read error"), 1));

        Assert.Contains(ops.PushedInbound, item => item is TransportDisconnected { Reason: DisconnectReason.Error });
        Assert.True(ops.PullOutboundCount > pullBefore);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundComplete_with_auto_reconnect_should_push_transient_disconnect()
    {
        var ops = new MockConnectionOperations();
        var sm = new TcpConnectionStateMachine(
            ops, ActorRefs.Nobody, new ReusablePoolingStrategy(), ActorRefs.Nobody);
        var options = new TcpTransportOptions { Host = "localhost", Port = 8080, AutoReconnect = true };

        sm.HandlePush(new ConnectTransport(options));
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));
        ops.PushedInbound.Clear();

        sm.Dispatch(new ReadCompleted(null, 2));

        Assert.Contains(ops.PushedInbound, item => item is TransportDisconnected { Reason: DisconnectReason.Transient });
        Assert.True(ops.PullOutboundCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void Multiple_reconnects_should_increment_generation()
    {
        var (sm, ops) = CreateStateMachine();

        sm.HandlePush(new ConnectTransport(TestOptions));
        var lease1 = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease1));

        ops.PushedInbound.Clear();

        sm.HandlePush(new ConnectTransport(TestOptions));
        var lease2 = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease2));

        // Stale gen ReadCompleted should be ignored
        var buffer = CreateTestBuffer(1, 2, 3);

        ops.PushedInbound.Clear();
        sm.Dispatch(new ReadCompleted(buffer, 1));

        Assert.Empty(ops.PushedInbound);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_ReadCompleted_with_data_should_push_transport_data()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Dispatch(new LeaseAcquired(CreateTestLease()));
        ops.PushedInbound.Clear();

        var buffer = CreateTestBuffer(4, 5, 6);
        sm.Dispatch(new ReadCompleted(buffer, Gen: 1));

        var data = Assert.IsType<TransportData>(Assert.Single(ops.PushedInbound));
        Assert.Equal(new byte[] { 4, 5, 6 }, data.Buffer.Span.ToArray());
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_ReadCompleted_with_stale_gen_should_dispose_buffer_and_push_nothing()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Dispatch(new LeaseAcquired(CreateTestLease()));
        ops.PushedInbound.Clear();

        var buffer = CreateTestBuffer(4, 5, 6);
        sm.Dispatch(new ReadCompleted(buffer, Gen: 0));

        Assert.Empty(ops.PushedInbound);
        Assert.Equal(0, buffer.Capacity); // disposed buffers release their owner
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_ReadCompleted_null_should_signal_graceful_disconnect()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Dispatch(new LeaseAcquired(CreateTestLease()));
        ops.PushedInbound.Clear();

        sm.Dispatch(new ReadCompleted(null, Gen: 1));

        var disconnected = Assert.IsType<TransportDisconnected>(Assert.Single(ops.PushedInbound));
        Assert.Equal(DisconnectReason.Graceful, disconnected.Reason);
    }

    // The inert connection (SocketPipeConnection.CreateInert) is not reentrant-instrumented: its
    // ReceiveAsync() parks on Task.Delay(Timeout.Infinite) forever, which itself models "a read is
    // in flight" and never completes synchronously, so there is no fixture-level call counter to
    // observe a second receive. The bug/fix under test is entirely captured by whether
    // _readInProgress survives a stale-generation event, so that private flag (inspected via
    // reflection, no production seam added) is the observable assertion here.
    [Fact(Timeout = 5000)]
    public void Dispatch_stale_ReadCompleted_should_not_clear_read_in_progress()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();

        // Acquiring the lease bumps the generation to 1 and immediately calls RequestRead(),
        // which parks on the inert connection's ReceiveAsync() — a read is now in flight for gen 1.
        sm.Dispatch(new LeaseAcquired(lease));

        Assert.True(GetReadInProgress(sm));

        var staleBuffer = CreateTestBuffer(1, 2, 3);
        sm.Dispatch(new ReadCompleted(staleBuffer, Gen: 0));

        // The stale event (gen 0, current gen is 1) must not clear the in-flight flag for the
        // current-gen read, and it must dispose its own rented buffer.
        Assert.True(GetReadInProgress(sm));
        Assert.Equal(0, staleBuffer.Capacity);

        ops.PushedInbound.Clear();
        var pullBefore = ops.PullOutboundCount;

        // Simulating the next downstream pull: with the flag still set, this must be a no-op —
        // no second receive is issued and no inbound/pull side effects occur.
        sm.RequestRead();

        Assert.True(GetReadInProgress(sm));
        Assert.Empty(ops.PushedInbound);
        Assert.Equal(pullBefore, ops.PullOutboundCount);
    }

    private static bool GetReadInProgress(TcpConnectionStateMachine sm)
    {
        var field = typeof(TcpConnectionStateMachine).GetField(
            "_readInProgress",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;
        return (bool)field.GetValue(sm)!;
    }
}
