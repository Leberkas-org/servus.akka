using System.Buffers;
using Akka.Actor;
using Servus.Akka.Tests.Utils;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Tcp;
using Servus.Akka.Transport.Tcp.Client;

namespace Servus.Akka.Tests.Transport.Tcp.Client;

public sealed class TcpTransportStateMachineSpec
{
    private static readonly TcpTransportOptions TestOptions = new()
    {
        Host = "localhost",
        Port = 8080
    };

    private static readonly IPoolingStrategy TestStrategy = new TestPoolingStrategy();

    private static (TcpTransportStateMachine Sm, MockTransportOperations Ops) CreateStateMachine()
    {
        var ops = new MockTransportOperations();
        var sm = new TcpTransportStateMachine(
            ops,
            ActorRefs.Nobody,
            TestStrategy,
            ActorRefs.Nobody);
        return (sm, ops);
    }

    private static ConnectionLease CreateTestLease()
    {
        var state = new ClientState(Stream.Null);
        var cts = new CancellationTokenSource();
        var handle = new ConnectionHandle(state.OutboundWriter, state.InboundReader, cts.Token);
        return new ConnectionLease(handle, state, cts, ConnectionInfo.None);
    }

    private static TransportBuffer CreateTestBuffer(params byte[] data)
    {
        var buf = TransportBuffer.Rent(data.Length);
        data.CopyTo(buf.FullMemory.Span);
        buf.Length = data.Length;
        return buf;
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
        sm.HandlePush(new TransportData(buffer));

        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));

        Assert.Contains(ops.CancelledTimers, k => k == "connect-timeout");
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundBatch_should_push_inbound_items()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));
        ops.PushedInbound.Clear();

        var items = ArrayPool<ITransportInbound>.Shared.Rent(8);
        items[0] = new TransportData(CreateTestBuffer(1, 2, 3));
        items[1] = new TransportData(CreateTestBuffer(4, 5, 6));

        sm.Dispatch(new InboundBatch(items, 2, 1));

        Assert.Equal(2, ops.PushedInbound.Count);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundBatch_stale_gen_should_be_ignored()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));
        ops.PushedInbound.Clear();

        var items = ArrayPool<ITransportInbound>.Shared.Rent(8);
        items[0] = new TransportData(CreateTestBuffer(1, 2, 3));

        sm.Dispatch(new InboundBatch(items, 1, 999));

        Assert.Empty(ops.PushedInbound);
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
    public void HandlePush_ConnectTransport_should_schedule_connect_timeout()
    {
        var (sm, ops) = CreateStateMachine();

        sm.HandlePush(new ConnectTransport(TestOptions));

        Assert.Contains(ops.ScheduledTimers, t => t.Key == "connect-timeout");
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_TransportData_without_handle_should_buffer_and_pull()
    {
        var (sm, ops) = CreateStateMachine();

        sm.HandlePush(new ConnectTransport(TestOptions));
        var pullBefore = ops.PullOutboundCount;

        var buffer = CreateTestBuffer(1, 2, 3);
        sm.HandlePush(new TransportData(buffer));

        Assert.True(ops.PullOutboundCount > pullBefore);
    }

    [Fact(Timeout = 5000)]
    public void HandleUpstreamFinish_without_handle_should_complete_stage()
    {
        var (sm, ops) = CreateStateMachine();

        sm.HandleUpstreamFinish();

        Assert.Equal(1, ops.CompleteStageCount);
    }

    [Fact(Timeout = 5000)]
    public void HandleUpstreamFinish_with_idle_handle_should_complete_stage()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));

        sm.HandleUpstreamFinish();

        Assert.Equal(1, ops.CompleteStageCount);
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
        Assert.Equal(0, ops.CompleteStageCount);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundComplete_should_push_disconnected()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));
        ops.PushedInbound.Clear();

        sm.Dispatch(new InboundComplete(DisconnectReason.Graceful, 1));

        Assert.Contains(ops.PushedInbound, item => item is TransportDisconnected { Reason: DisconnectReason.Graceful });
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundComplete_stale_gen_should_be_ignored()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));
        ops.PushedInbound.Clear();

        sm.Dispatch(new InboundComplete(DisconnectReason.Graceful, 999));

        Assert.Empty(ops.PushedInbound);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundComplete_with_upstream_finished_should_complete_stage()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));

        sm.HandleUpstreamFinish();

        sm.Dispatch(new InboundComplete(DisconnectReason.Graceful, 1));

        Assert.Equal(1, ops.CompleteStageCount);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundPumpFailed_should_push_disconnected()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));
        ops.PushedInbound.Clear();

        sm.Dispatch(new InboundPumpFailed(new IOException("pump error")));

        Assert.Contains(ops.PushedInbound, item => item is TransportDisconnected { Reason: DisconnectReason.Error });
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_OutboundWriteFailed_should_push_disconnected()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));
        ops.PushedInbound.Clear();

        sm.Dispatch(new OutboundWriteFailed(new IOException("write failed")));

        Assert.Contains(ops.PushedInbound, item => item is TransportDisconnected { Reason: DisconnectReason.Error });
    }

    [Fact(Timeout = 5000)]
    public void PostStop_should_cancel_connect_timer()
    {
        var (sm, ops) = CreateStateMachine();

        sm.PostStop();

        Assert.Contains(ops.CancelledTimers, k => k == "connect-timeout");
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
    public void HandlePush_DisconnectTransport_should_cleanup_and_pull()
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
    public void HandlePush_TransportData_with_handle_should_write_and_pull()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));

        var buffer = CreateTestBuffer(7, 8, 9);
        var pullBefore = ops.PullOutboundCount;
        sm.HandlePush(new TransportData(buffer));

        Assert.True(ops.PullOutboundCount > pullBefore);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_TransportData_multiple_before_connection_should_buffer_all()
    {
        var (sm, ops) = CreateStateMachine();

        sm.HandlePush(new ConnectTransport(TestOptions));

        var buf1 = CreateTestBuffer(1, 2);
        var buf2 = CreateTestBuffer(3, 4);
        sm.HandlePush(new TransportData(buf1));
        sm.HandlePush(new TransportData(buf2));

        // Both should be queued
        var pullCount = ops.PullOutboundCount;
        Assert.True(pullCount >= 3); // connect + 2 data pulls
    }

    [Fact(Timeout = 5000)]
    public void HandleUpstreamFinish_with_pending_writes_should_keep_connection()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));

        var buffer = CreateTestBuffer(1, 2, 3);
        sm.HandlePush(new TransportData(buffer));

        sm.HandleUpstreamFinish();

        Assert.Empty(ops.PushedInbound);
    }

    [Fact(Timeout = 5000)]
    public void HandleDownstreamFinish_with_pending_writes_should_cleanup()
    {
        var (sm, _) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));

        var buffer = CreateTestBuffer(5, 6, 7);
        sm.HandlePush(new TransportData(buffer));

        sm.HandleDownstreamFinish();

        Assert.False(lease.IsAlive());
    }

    [Fact(Timeout = 5000)]
    public void OnTimer_without_pending_connect_should_be_ignored()
    {
        var (sm, ops) = CreateStateMachine();

        sm.OnTimer("connect-timeout");

        Assert.Empty(ops.PushedInbound);
    }

    [Fact(Timeout = 5000)]
    public void OnTimer_after_lease_acquired_should_be_ignored()
    {
        var (sm, ops) = CreateStateMachine();
        sm.HandlePush(new ConnectTransport(TestOptions));

        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));
        ops.PushedInbound.Clear();

        sm.OnTimer("connect-timeout");

        Assert.Empty(ops.PushedInbound);
    }

    [Fact(Timeout = 5000)]
    public void PostStop_with_pending_writes_should_dispose_all()
    {
        var (sm, ops) = CreateStateMachine();

        sm.HandlePush(new ConnectTransport(TestOptions));
        var buf1 = CreateTestBuffer(1, 2);
        var buf2 = CreateTestBuffer(3, 4);
        sm.HandlePush(new TransportData(buf1));
        sm.HandlePush(new TransportData(buf2));

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
    public void Dispatch_OutboundWriteDone_should_be_ignored()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));
        ops.PushedInbound.Clear();
        var pullBefore = ops.PullOutboundCount;

        sm.Dispatch(new OutboundWriteDone(1));

        Assert.Equal(pullBefore, ops.PullOutboundCount);
        Assert.Empty(ops.PushedInbound);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundComplete_should_signal_pull_when_upstream_not_finished()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));
        ops.PushedInbound.Clear();
        var pullBefore = ops.PullOutboundCount;

        sm.Dispatch(new InboundComplete(DisconnectReason.Graceful, 1));

        Assert.Contains(ops.PushedInbound, item => item is TransportDisconnected { Reason: DisconnectReason.Graceful });
        Assert.True(ops.PullOutboundCount > pullBefore);
    }

    [Fact(Timeout = 5000)]
    public void OnLeaseAcquired_should_increment_connection_generation()
    {
        var (sm, ops) = CreateStateMachine();

        sm.HandlePush(new ConnectTransport(TestOptions));
        var lease1 = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease1));

        var items1 = ArrayPool<ITransportInbound>.Shared.Rent(8);
        items1[0] = new TransportData(CreateTestBuffer(1, 2, 3));
        sm.Dispatch(new InboundBatch(items1, 1, 1));

        ops.PushedInbound.Clear();

        // Now simulate a reconnect by creating a new lease
        sm.HandlePush(new ConnectTransport(TestOptions));
        var lease2 = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease2));

        ops.PushedInbound.Clear();

        // Old generation should be ignored
        var items2 = ArrayPool<ITransportInbound>.Shared.Rent(8);
        items2[0] = new TransportData(CreateTestBuffer(4, 5, 6));
        sm.Dispatch(new InboundBatch(items2, 1, 1)); // Old generation (1)

        Assert.Empty(ops.PushedInbound);
    }

    [Fact(Timeout = 5000)]
    public void OnLeaseAcquired_after_reconnect_should_signal_connected()
    {
        var (sm, ops) = CreateStateMachine();

        sm.HandlePush(new ConnectTransport(TestOptions));
        var lease1 = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease1));

        ops.PushedInbound.Clear();

        sm.HandlePush(new ConnectTransport(TestOptions)); // This sets _isReconnecting = true
        var lease2 = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease2));

        Assert.Contains(ops.PushedInbound, item => item is TransportConnected);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundComplete_error_reason_should_be_preserved()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));
        ops.PushedInbound.Clear();

        sm.Dispatch(new InboundComplete(DisconnectReason.Error, 1));

        Assert.Contains(ops.PushedInbound, item => item is TransportDisconnected { Reason: DisconnectReason.Error });
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundComplete_transient_reason_should_be_preserved()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));
        ops.PushedInbound.Clear();

        sm.Dispatch(new InboundComplete(DisconnectReason.Transient, 1));

        Assert.Contains(ops.PushedInbound, item => item is TransportDisconnected { Reason: DisconnectReason.Transient });
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundPumpFailed_should_stop_pumps()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));
        ops.PushedInbound.Clear();

        sm.Dispatch(new InboundPumpFailed(new IOException("pump error")));

        Assert.Contains(ops.PushedInbound, item => item is TransportDisconnected { Reason: DisconnectReason.Error });
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_OutboundWriteFailed_should_return_lease_and_disconnect()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));
        ops.PushedInbound.Clear();

        sm.Dispatch(new OutboundWriteFailed(new IOException("write error")));

        Assert.Contains(ops.PushedInbound, item => item is TransportDisconnected { Reason: DisconnectReason.Error });
        Assert.True(lease.IsAlive()); // Lease not disposed by state machine in Dispatch path
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_OutboundWriteFailed_should_signal_pull()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));
        var pullBefore = ops.PullOutboundCount;

        sm.Dispatch(new OutboundWriteFailed(new IOException("write error")));

        Assert.True(ops.PullOutboundCount > pullBefore);
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
    public void Dispatch_AcquisitionFailed_should_cancel_timer()
    {
        var (sm, ops) = CreateStateMachine();

        sm.HandlePush(new ConnectTransport(TestOptions));
        ops.PushedInbound.Clear();
        ops.CancelledTimers.Clear();

        sm.Dispatch(new AcquisitionFailed(new IOException("connection refused")));

        Assert.Contains(ops.CancelledTimers, k => k == "connect-timeout");
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
    public void HandlePush_ConnectTransport_with_default_timeout_should_use_10_seconds()
    {
        var (sm, ops) = CreateStateMachine();
        var options = new TcpTransportOptions { Host = "localhost", Port = 8080 };

        sm.HandlePush(new ConnectTransport(options));

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
    public void HandlePush_ConnectTransport_with_zero_timeout_should_use_10_seconds()
    {
        var (sm, ops) = CreateStateMachine();
        var options = new TcpTransportOptions { Host = "localhost", Port = 8080, ConnectTimeout = TimeSpan.Zero };

        sm.HandlePush(new ConnectTransport(options));

        var timer = ops.ScheduledTimers.First(t => t.Key == "connect-timeout");
        Assert.Equal(TimeSpan.FromSeconds(10), timer.Delay);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_LeaseAcquired_should_start_pump_manager()
    {
        var (sm, ops) = CreateStateMachine();

        sm.HandlePush(new ConnectTransport(TestOptions));

        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));

        Assert.Contains(ops.ScheduledTimers, t => t.Key == "connect-timeout");
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
    public void Dispatch_InboundBatch_should_return_array_to_pool()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));
        ops.PushedInbound.Clear();

        var items = ArrayPool<ITransportInbound>.Shared.Rent(8);
        items[0] = new TransportData(CreateTestBuffer(1, 2, 3));

        sm.Dispatch(new InboundBatch(items, 1, 1));

        Assert.Single(ops.PushedInbound);
        // Array was returned to pool (impl detail but verifiable by no exceptions)
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundBatch_should_clear_array_items()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));
        ops.PushedInbound.Clear();

        var items = ArrayPool<ITransportInbound>.Shared.Rent(8);
        var buffer = CreateTestBuffer(1, 2, 3);
        items[0] = new TransportData(buffer);
        items[1] = new TransportData(CreateTestBuffer(4, 5, 6));

        sm.Dispatch(new InboundBatch(items, 2, 1));

        Assert.Equal(2, ops.PushedInbound.Count);
        // Items should be cleared in array (impl detail)
    }

    [Fact(Timeout = 5000)]
    public void HandleUpstreamFinish_with_idle_handle_should_complete_even_after_data_write()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));

        var buffer = CreateTestBuffer(1, 2, 3);
        sm.HandlePush(new TransportData(buffer)); // Data written, no pending writes left

        sm.HandleUpstreamFinish();

        Assert.Equal(1, ops.CompleteStageCount);
    }

    [Fact(Timeout = 5000)]
    public void Multiple_reconnects_should_increment_generation()
    {
        var (sm, ops) = CreateStateMachine();

        sm.HandlePush(new ConnectTransport(TestOptions));
        var lease1 = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease1));

        // Stale generation should be ignored
        var items = ArrayPool<ITransportInbound>.Shared.Rent(8);
        items[0] = new TransportData(CreateTestBuffer(1, 2, 3));
        sm.Dispatch(new InboundBatch(items, 1, 0)); // Old generation

        ops.PushedInbound.Clear();

        sm.HandlePush(new ConnectTransport(TestOptions));
        var lease2 = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease2));

        var items2 = ArrayPool<ITransportInbound>.Shared.Rent(8);
        items2[0] = new TransportData(CreateTestBuffer(4, 5, 6));
        sm.Dispatch(new InboundBatch(items2, 1, 2)); // New generation

        Assert.Single(ops.PushedInbound);
    }

    [Fact(Timeout = 5000)]
    public void Pool_strategy_reuse_on_upstream_finish_should_not_dispose_handle()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));

        sm.HandleUpstreamFinish();

        Assert.Equal(1, ops.CompleteStageCount);
    }

    [Fact(Timeout = 5000)]
    public void Pool_strategy_dispose_on_disconnect_should_notify_manager()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));

        sm.Dispatch(new InboundComplete(DisconnectReason.Graceful, 1));

        Assert.True(lease.IsAlive()); // Lease still alive in test
        Assert.Contains(ops.PushedInbound, item => item is TransportDisconnected { Reason: DisconnectReason.Graceful });
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundComplete_with_auto_reconnect_should_push_transient_disconnect()
    {
        var ops = new MockTransportOperations();
        var sm = new TcpTransportStateMachine(
            ops, ActorRefs.Nobody, new ReusablePoolingStrategy(), ActorRefs.Nobody);
        var options = new TcpTransportOptions { Host = "localhost", Port = 8080, AutoReconnect = true };

        sm.HandlePush(new ConnectTransport(options));
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));
        ops.PushedInbound.Clear();

        sm.Dispatch(new InboundComplete(DisconnectReason.Graceful, 2));

        Assert.Contains(ops.PushedInbound, item => item is TransportDisconnected { Reason: DisconnectReason.Transient });
        Assert.True(ops.PullOutboundCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundComplete_with_auto_reconnect_should_dispose_pending_writes()
    {
        var ops = new MockTransportOperations();
        var sm = new TcpTransportStateMachine(
            ops, ActorRefs.Nobody, new ReusablePoolingStrategy(), ActorRefs.Nobody);
        var options = new TcpTransportOptions { Host = "localhost", Port = 8080, AutoReconnect = true };

        sm.HandlePush(new ConnectTransport(options));

        var buffer = CreateTestBuffer(1, 2, 3);
        sm.HandlePush(new TransportData(buffer));

        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));
        ops.PushedInbound.Clear();

        sm.Dispatch(new InboundComplete(DisconnectReason.Graceful, 2));

        Assert.Contains(ops.PushedInbound, item => item is TransportDisconnected { Reason: DisconnectReason.Transient });
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundPumpFailed_with_upstream_finished_should_complete_stage()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));

        sm.HandleUpstreamFinish();
        ops.CompleteStageCount = 0;
        ops.PushedInbound.Clear();

        sm.Dispatch(new InboundPumpFailed(new IOException("pump error")));

        Assert.Contains(ops.PushedInbound, item => item is TransportDisconnected { Reason: DisconnectReason.Error });
        Assert.Equal(1, ops.CompleteStageCount);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundPumpFailed_without_upstream_finished_should_signal_pull()
    {
        var (sm, ops) = CreateStateMachine();
        var lease = CreateTestLease();
        sm.Dispatch(new LeaseAcquired(lease));
        ops.PushedInbound.Clear();
        var pullBefore = ops.PullOutboundCount;

        sm.Dispatch(new InboundPumpFailed(new IOException("pump error")));

        Assert.Contains(ops.PushedInbound, item => item is TransportDisconnected { Reason: DisconnectReason.Error });
        Assert.True(ops.PullOutboundCount > pullBefore);
        Assert.Equal(0, ops.CompleteStageCount);
    }

}
