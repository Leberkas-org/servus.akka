using System.Net;
using Akka.Actor;
using Servus.Akka.Tests.Transport;
using Servus.Akka.Tests.Utils;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Tcp;
using Servus.Akka.Transport.Tcp.Listener;

namespace Servus.Akka.Tests.Transport.Tcp.Listener;

public sealed class TcpServerStateMachineSpec
{
    private static readonly ConnectionInfo TestConnectionInfo = new(
        new IPEndPoint(IPAddress.Loopback, 5000),
        new IPEndPoint(IPAddress.Loopback, 12345),
        TransportProtocol.Tcp);

    private static (TcpServerStateMachine Sm, MockConnectionOperations Ops) CreateStateMachine(Stream? stream = null)
    {
        var ops = new MockConnectionOperations();
        var sm = new TcpServerStateMachine(ops, ActorRefs.Nobody, stream ?? Stream.Null, TestConnectionInfo);
        return (sm, ops);
    }

    /// <summary>
    /// Injects a <see cref="FakeDuplexConnection"/> via the SM's test-only <c>connectionFactory</c> seam
    /// so watermark/enqueue/flush behavior can be observed deterministically — a real
    /// <see cref="StreamConnection"/> over <see cref="Stream.Null"/> completes reads/writes
    /// asynchronously on background tasks and cannot be raced against synchronously in a unit test.
    /// </summary>
    private static (TcpServerStateMachine Sm, MockConnectionOperations Ops, FakeDuplexConnection Connection)
        CreateStateMachineWithFakeConnection()
    {
        var ops = new MockConnectionOperations();
        var connection = new FakeDuplexConnection();
        var sm = new TcpServerStateMachine(
            ops, ActorRefs.Nobody, Stream.Null, TestConnectionInfo,
            connectionFactory: () => connection);
        sm.Start();
        return (sm, ops, connection);
    }

    private static WireBuffer CreateTestBuffer(params byte[] data)
    {
        var buf = WireBuffer.Rent(data.Length);
        data.CopyTo(buf.FullMemory.Span);
        buf.Length = data.Length;
        return buf;
    }

    [Fact(Timeout = 5000)]
    public void Start_should_push_connected_and_create_connection()
    {
        var (sm, ops) = CreateStateMachine();

        sm.Start();

        Assert.Single(ops.PushedInbound);
        var connected = Assert.IsType<TransportConnected>(ops.PushedInbound[0]);
        Assert.Equal(TestConnectionInfo, connected.Info);
    }

    [Fact(Timeout = 5000)]
    public void Start_should_include_tls_info_when_allow_delayed_negotiation()
    {
        var ops = new MockConnectionOperations();
        var sm = new TcpServerStateMachine(ops, ActorRefs.Nobody, Stream.Null, TestConnectionInfo,
            allowDelayedNegotiation: true);

        sm.Start();

        Assert.Single(ops.PushedInbound);
        var connected = Assert.IsType<TransportConnected>(ops.PushedInbound[0]);
        Assert.NotNull(connected.Info.Security);
        Assert.True(connected.Info.Security.AllowDelayedNegotiation);
    }

    [Fact(Timeout = 5000)]
    public void RequestRead_should_be_callable_after_start()
    {
        var (sm, _) = CreateStateMachine();
        sm.Start();

        sm.RequestRead();
    }

    [Fact(Timeout = 5000)]
    public void RequestRead_without_start_should_be_noop()
    {
        var (sm, ops) = CreateStateMachine();

        sm.RequestRead();

        Assert.Empty(ops.PushedInbound);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_TransportData_should_write_to_pipe()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();
        var pullBefore = ops.PullOutboundCount;

        var buffer = CreateTestBuffer(1, 2, 3);
        sm.HandlePush(TransportData.Rent(buffer));

        Assert.True(ops.PullOutboundCount > pullBefore);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_TransportData_without_connection_should_dispose_buffer_and_signal_pull()
    {
        var (sm, ops) = CreateStateMachine();
        var pullBefore = ops.PullOutboundCount;

        var buffer = CreateTestBuffer(1, 2, 3);
        sm.HandlePush(TransportData.Rent(buffer));

        Assert.True(ops.PullOutboundCount > pullBefore);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_Disconnect_should_cleanup_and_complete_stage()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();

        sm.HandlePush(new DisconnectTransport(DisconnectReason.Graceful));

        Assert.True(ops.CompleteStageCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_unknown_message_should_signal_pull()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        var pullBefore = ops.PullOutboundCount;

        sm.HandlePush(new OpenStream(1L, StreamDirection.Bidirectional));

        Assert.True(ops.PullOutboundCount > pullBefore);
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
    public void HandleDownstreamFinish_should_cleanup()
    {
        var (sm, _) = CreateStateMachine();
        sm.Start();

        sm.HandleDownstreamFinish();

        sm.PostStop();
    }

    [Fact(Timeout = 5000)]
    public void PostStop_should_not_throw()
    {
        var (sm, _) = CreateStateMachine();
        sm.Start();

        sm.PostStop();
    }

    [Fact(Timeout = 5000)]
    public void PostStop_before_start_should_not_throw()
    {
        var (sm, _) = CreateStateMachine();

        sm.PostStop();
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_ReadCompleted_with_data_should_push_lease()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();

        var buffer = CreateTestBuffer(1, 2, 3);
        sm.Dispatch(new ReadCompleted(buffer, 1));

        Assert.Single(ops.PushedInbound);
        var transportData = Assert.IsType<TransportData>(ops.PushedInbound[0]);
        Assert.Equal(3, transportData.Buffer.Length);

        transportData.Buffer.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_ReadCompleted_completed_should_push_disconnected()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();

        sm.Dispatch(new ReadCompleted(null, 1));

        Assert.Contains(ops.PushedInbound, item => item is TransportDisconnected { Reason: DisconnectReason.Graceful });
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_ReadFailed_should_push_disconnected_error()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();

        sm.Dispatch(new ReadFailed(new IOException("read error"), 1));

        Assert.Contains(ops.PushedInbound, item => item is TransportDisconnected { Reason: DisconnectReason.Error });
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_ReadCompleted_stale_gen_should_be_ignored()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();

        var buffer = CreateTestBuffer(1, 2, 3);
        sm.Dispatch(new ReadCompleted(buffer, 999));

        Assert.Empty(ops.PushedInbound);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_ReadFailed_stale_gen_should_be_ignored()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();

        sm.Dispatch(new ReadFailed(new IOException("stale"), 999));

        Assert.Empty(ops.PushedInbound);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundComplete_without_upstream_finished_should_signal_pull()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();
        var pullBefore = ops.PullOutboundCount;

        sm.Dispatch(new ReadFailed(new IOException("read error"), 1));

        Assert.True(ops.PullOutboundCount > pullBefore);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundComplete_with_upstream_finished_should_complete_stage()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();

        sm.HandleUpstreamFinish();
        var completeBefore = ops.CompleteStageCount;

        sm.Dispatch(new ReadFailed(new IOException("read error"), 2));

        Assert.True(ops.CompleteStageCount > completeBefore);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_ReadCompleted_with_data_should_push_transport_data()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();

        var buffer = CreateTestBuffer(1, 2, 3);
        sm.Dispatch(new ReadCompleted(buffer, Gen: 1));

        var data = Assert.IsType<TransportData>(Assert.Single(ops.PushedInbound));
        Assert.Equal(new byte[] { 1, 2, 3 }, data.Buffer.Span.ToArray());
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_ReadCompleted_with_stale_gen_should_dispose_buffer_and_push_nothing()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();

        var buffer = CreateTestBuffer(1, 2, 3);
        sm.Dispatch(new ReadCompleted(buffer, Gen: 0));

        Assert.Empty(ops.PushedInbound);
        Assert.Equal(0, buffer.Capacity); // disposed buffers release their owner
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_ReadCompleted_null_should_signal_graceful_disconnect()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();

        sm.Dispatch(new ReadCompleted(null, Gen: 1));

        var disconnected = Assert.IsType<TransportDisconnected>(Assert.Single(ops.PushedInbound));
        Assert.Equal(DisconnectReason.Graceful, disconnected.Reason);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_ReadFailed_should_signal_error_disconnect()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();

        sm.Dispatch(new ReadFailed(new IOException("boom"), Gen: 1));

        var disconnected = Assert.IsType<TransportDisconnected>(Assert.Single(ops.PushedInbound));
        Assert.Equal(DisconnectReason.Error, disconnected.Reason);
    }

    [Fact(Timeout = 5000)]
    public void HandleTransportData_should_enqueue_buffer_and_return_wrapper()
    {
        var (sm, _, connection) = CreateStateMachineWithFakeConnection();

        var buffer = CreateTestBuffer(1, 2, 3);
        var wrapper = TransportData.Rent(buffer);

        sm.HandlePush(wrapper);

        Assert.Contains(buffer, connection.Enqueued);
        Assert.Null(wrapper.Buffer); // the wrapper was returned to the pool after the enqueue
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_above_high_watermark_should_not_pull_upstream()
    {
        var (sm, ops, connection) = CreateStateMachineWithFakeConnection();

        var big = WireBuffer.Rent(512 * 1024);
        big.Length = 512 * 1024;
        var pullBefore = ops.PullOutboundCount;

        sm.HandlePush(TransportData.Rent(big));

        Assert.Contains(big, connection.Enqueued);
        Assert.Equal(pullBefore, ops.PullOutboundCount); // at/above high watermark: no pull
    }

    [Fact(Timeout = 5000)]
    public void SendFlushed_below_low_watermark_should_resume_pull()
    {
        var (sm, ops, _) = CreateStateMachineWithFakeConnection();

        var big = WireBuffer.Rent(512 * 1024);
        big.Length = 512 * 1024;
        sm.HandlePush(TransportData.Rent(big)); // 512K in flight, at high watermark, no pull
        var pullBefore = ops.PullOutboundCount;

        // 512K - 300K = 212K, crossing below the 256K low watermark.
        sm.Dispatch(new SendFlushed(300 * 1024, Gen: 1));

        Assert.True(ops.PullOutboundCount > pullBefore);
    }

    [Fact(Timeout = 5000)]
    public void SendFlushed_stale_gen_should_be_ignored()
    {
        var (sm, ops, _) = CreateStateMachineWithFakeConnection();

        var big = WireBuffer.Rent(512 * 1024);
        big.Length = 512 * 1024;
        sm.HandlePush(TransportData.Rent(big));
        var pullBefore = ops.PullOutboundCount;

        sm.Dispatch(new SendFlushed(300 * 1024, Gen: 999));

        Assert.Equal(pullBefore, ops.PullOutboundCount);
    }

    [Fact(Timeout = 5000)]
    public void RequestRead_async_path_should_reuse_cached_transforms()
    {
        var (sm, ops, _) = CreateStateMachineWithFakeConnection();
        sm.RequestRead(); // async read #1 parks on the fake connection

        var success1 = sm.ReadState.ReadSuccess;
        var failure1 = sm.ReadState.ReadFailure;

        // Complete read #1 in the current gen so a second read can be issued.
        var buffer = CreateTestBuffer(1);
        sm.Dispatch(new ReadCompleted(buffer, Gen: 1));
        ((TransportData)ops.PushedInbound[^1]).Buffer.Dispose();

        sm.RequestRead(); // async read #2, same generation

        Assert.Same(success1, sm.ReadState.ReadSuccess);
        Assert.Same(failure1, sm.ReadState.ReadFailure);
    }

    // The fake connection's ReceiveAsync() returns a TCS-backed ValueTask that never completes,
    // which itself models "a read is in flight" and never completes synchronously, so there is no
    // fixture-level call counter to observe a second receive. The bug/fix under test is entirely
    // captured by whether _readInProgress survives a stale-generation event, so that private flag
    // (inspected via reflection, no production seam added) is the observable assertion here.
    [Fact(Timeout = 5000)]
    public void Dispatch_stale_ReadCompleted_should_not_clear_read_in_progress()
    {
        var (sm, ops, _) = CreateStateMachineWithFakeConnection();

        // RequestRead parks on the inert fake connection's ReceiveAsync() — a read is now in flight.
        sm.RequestRead();

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

    private static bool GetReadInProgress(TcpServerStateMachine sm)
    {
        var field = typeof(TcpServerStateMachine).GetField(
            "_readInProgress",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;
        return (bool)field.GetValue(sm)!;
    }
}
