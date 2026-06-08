using System.IO.Pipelines;
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

    private static (TcpServerStateMachine Sm, MockConnectionOperations Ops) CreateStateMachine(Stream? stream = null)
    {
        var ops = new MockConnectionOperations();
        var sm = new TcpServerStateMachine(ops, ActorRefs.Nobody, stream ?? Stream.Null, TestConnectionInfo);
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
    public async Task Dispatch_PipeReadComplete_with_data_should_push_lease()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();

        var pipe = new Pipe();
        await pipe.Writer.WriteAsync(new byte[] { 1, 2, 3 }, TestContext.Current.CancellationToken);
        var readResult = await pipe.Reader.ReadAsync(TestContext.Current.CancellationToken);

        sm.Dispatch(new PipeReadComplete(readResult, 1));

        Assert.Single(ops.PushedInbound);
        var transportData = Assert.IsType<TransportData>(ops.PushedInbound[0]);
        Assert.Equal(3, transportData.Buffer.Length);

        transportData.Buffer.Dispose();
        pipe.Writer.Complete();
        pipe.Reader.Complete();
    }

    [Fact(Timeout = 5000)]
    public async Task Dispatch_PipeReadComplete_completed_should_push_disconnected()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();

        var pipe = new Pipe();
        pipe.Writer.Complete();
        var readResult = await pipe.Reader.ReadAsync(TestContext.Current.CancellationToken);

        sm.Dispatch(new PipeReadComplete(readResult, 1));

        Assert.Contains(ops.PushedInbound, item => item is TransportDisconnected { Reason: DisconnectReason.Graceful });

        pipe.Reader.Complete();
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_PipeReadFailed_should_push_disconnected_error()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();

        sm.Dispatch(new PipeReadFailed(new IOException("pipe error"), 1));

        Assert.Contains(ops.PushedInbound, item => item is TransportDisconnected { Reason: DisconnectReason.Error });
    }

    [Fact(Timeout = 5000)]
    public async Task Dispatch_PipeReadComplete_stale_gen_should_be_ignored()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();

        var pipe = new Pipe();
        await pipe.Writer.WriteAsync(new byte[] { 1, 2, 3 }, TestContext.Current.CancellationToken);
        var readResult = await pipe.Reader.ReadAsync(TestContext.Current.CancellationToken);

        sm.Dispatch(new PipeReadComplete(readResult, 999));

        Assert.Empty(ops.PushedInbound);

        pipe.Reader.AdvanceTo(readResult.Buffer.End);
        pipe.Writer.Complete();
        pipe.Reader.Complete();
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_PipeReadFailed_stale_gen_should_be_ignored()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();

        sm.Dispatch(new PipeReadFailed(new IOException("stale"), 999));

        Assert.Empty(ops.PushedInbound);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundComplete_without_upstream_finished_should_signal_pull()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();
        var pullBefore = ops.PullOutboundCount;

        sm.Dispatch(new PipeReadFailed(new IOException("read error"), 1));

        Assert.True(ops.PullOutboundCount > pullBefore);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_InboundComplete_with_upstream_finished_should_complete_stage()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();

        sm.HandleUpstreamFinish();
        var completeBefore = ops.CompleteStageCount;

        sm.Dispatch(new PipeReadFailed(new IOException("read error"), 2));

        Assert.True(ops.CompleteStageCount > completeBefore);
    }
}
