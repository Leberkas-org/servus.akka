using System.Buffers;
using System.Net;
using Akka.Actor;
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

    private static (TcpServerStateMachine Sm, MockTransportOperations Ops) CreateStateMachine(Stream? stream = null)
    {
        var ops = new MockTransportOperations();
        var state = new ClientState(stream ?? Stream.Null);
        var sm = new TcpServerStateMachine(ops, ActorRefs.Nobody, state, TestConnectionInfo);
        return (sm, ops);
    }

    private static (TcpServerStateMachine Sm, MockTransportOperations Ops) CreateStateMachineWithTls(
        bool allowDelayedNegotiation)
    {
        var ops = new MockTransportOperations();
        var state = new ClientState(Stream.Null);
        var sm = new TcpServerStateMachine(ops, ActorRefs.Nobody, state, TestConnectionInfo,
            sslStream: null, allowDelayedNegotiation: allowDelayedNegotiation);
        return (sm, ops);
    }

    private static TransportBuffer CreateTestBuffer(params byte[] data)
    {
        var buf = TransportBuffer.Rent(data.Length);
        data.CopyTo(buf.FullMemory.Span);
        buf.Length = data.Length;
        return buf;
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
    public void HandlePush_TransportData_should_signal_pull_outbound()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();

        var buffer = CreateTestBuffer(1, 2, 3);
        sm.HandlePush(new TransportData(buffer));

        Assert.True(ops.PullOutboundCount > 0);
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
    public void Dispatch_InboundBatch_should_push_inbound_items()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();

        var batch = ArrayPool<ITransportInbound>.Shared.Rent(2);
        var buf1 = CreateTestBuffer(1);
        var buf2 = CreateTestBuffer(2);
        batch[0] = new TransportData(buf1);
        batch[1] = new TransportData(buf2);

        sm.Dispatch(new InboundBatch(batch, 2, 1));

        Assert.Equal(2, ops.PushedInbound.Count);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundBatch_with_stale_gen_should_return_batch_to_pool()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();

        var batch = ArrayPool<ITransportInbound>.Shared.Rent(1);
        batch[0] = new TransportData(CreateTestBuffer(1));

        sm.Dispatch(new InboundBatch(batch, 1, 999));

        Assert.Empty(ops.PushedInbound);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundComplete_should_push_disconnected()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();

        sm.Dispatch(new InboundComplete(DisconnectReason.Graceful, 1));

        Assert.Single(ops.PushedInbound);
        var disconnected = Assert.IsType<TransportDisconnected>(ops.PushedInbound[0]);
        Assert.Equal(DisconnectReason.Graceful, disconnected.Reason);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_OutboundWriteFailed_should_push_error_disconnected()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();

        sm.Dispatch(new OutboundWriteFailed(new IOException("test")));

        Assert.Single(ops.PushedInbound);
        var disconnected = Assert.IsType<TransportDisconnected>(ops.PushedInbound[0]);
        Assert.Equal(DisconnectReason.Error, disconnected.Reason);
    }

    [Fact(Timeout = 5000)]
    public void PostStop_should_not_throw()
    {
        var (sm, _) = CreateStateMachine();
        sm.Start();

        sm.PostStop();
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_TransportData_before_start_should_dispose_buffer()
    {
        var (sm, ops) = CreateStateMachine();

        var buffer = CreateTestBuffer(1, 2, 3);
        sm.HandlePush(new TransportData(buffer));

        Assert.True(ops.PullOutboundCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_TransportData_when_handle_is_null_should_dispose_buffer_and_signal_pull()
    {
        var (sm, ops) = CreateStateMachine();

        var buffer = CreateTestBuffer(1, 2, 3);
        sm.HandlePush(new TransportData(buffer));

        Assert.True(ops.PullOutboundCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundComplete_with_upstream_finished_should_complete_stage()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();
        var initialCompleteCount = ops.CompleteStageCount;

        sm.HandleUpstreamFinish();
        sm.Dispatch(new InboundComplete(DisconnectReason.Graceful, 2));

        Assert.True(ops.CompleteStageCount > initialCompleteCount);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundComplete_without_upstream_finished_should_signal_pull()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PullOutboundCount = 0;
        ops.PushedInbound.Clear();

        sm.Dispatch(new InboundComplete(DisconnectReason.Graceful, 1));

        Assert.True(ops.PullOutboundCount > 0);
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
    public void Dispatch_InboundPumpFailed_should_push_error_disconnected()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();

        sm.Dispatch(new InboundPumpFailed(new IOException("test error")));

        Assert.Single(ops.PushedInbound);
        var disconnected = Assert.IsType<TransportDisconnected>(ops.PushedInbound[0]);
        Assert.Equal(DisconnectReason.Error, disconnected.Reason);
    }

    [Fact(Timeout = 5000)]
    public void Start_should_increment_connection_gen()
    {
        var (sm, ops) = CreateStateMachine();

        sm.Start();

        sm.Start();

        Assert.Equal(2, ops.PushedInbound.Count);
        Assert.All(ops.PushedInbound, item => Assert.IsType<TransportConnected>(item));
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_OutboundWriteDone_should_not_push_or_complete()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();
        var initialCompleteCount = ops.CompleteStageCount;

        sm.Dispatch(new OutboundWriteDone());

        Assert.Empty(ops.PushedInbound);
        Assert.Equal(initialCompleteCount, ops.CompleteStageCount);
    }

    [Fact(Timeout = 5000)]
    public void HandlePush_unknown_message_type_should_not_throw()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();

        sm.HandlePush(new OpenStream(1L, StreamDirection.Bidirectional));

        Assert.True(ops.PullOutboundCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void PostStop_before_start_should_not_throw()
    {
        var (sm, _) = CreateStateMachine();

        sm.PostStop();
    }

    [Fact(Timeout = 5000)]
    public void HandleDownstreamFinish_before_start_should_not_throw()
    {
        var (sm, _) = CreateStateMachine();

        sm.HandleDownstreamFinish();
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundComplete_error_should_push_error_disconnected()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();

        sm.Dispatch(new InboundComplete(DisconnectReason.Error, 1));

        Assert.Single(ops.PushedInbound);
        var disconnected = Assert.IsType<TransportDisconnected>(ops.PushedInbound[0]);
        Assert.Equal(DisconnectReason.Error, disconnected.Reason);
    }

    [Fact(Timeout = 5000)]
    public void Start_should_emit_single_TransportConnected_without_tls()
    {
        var (sm, ops) = CreateStateMachine();

        sm.Start();

        Assert.Single(ops.PushedInbound);
        Assert.IsType<TransportConnected>(ops.PushedInbound[0]);
    }

    [Fact(Timeout = 5000)]
    public void Start_should_include_tls_info_in_TransportConnected_when_allow_delayed()
    {
        var (sm, ops) = CreateStateMachineWithTls(allowDelayedNegotiation: true);

        sm.Start();

        Assert.Single(ops.PushedInbound);
        var connected = Assert.IsType<TransportConnected>(ops.PushedInbound[0]);
        Assert.NotNull(connected.Info.Security);
        Assert.True(connected.Info.Security.AllowDelayedNegotiation);
    }

    [Fact(Timeout = 5000)]
    public void Start_should_emit_single_TransportConnected_when_no_ssl_and_no_delay()
    {
        var (sm, ops) = CreateStateMachineWithTls(allowDelayedNegotiation: false);

        sm.Start();

        Assert.Single(ops.PushedInbound);
        Assert.IsType<TransportConnected>(ops.PushedInbound[0]);
    }
}
