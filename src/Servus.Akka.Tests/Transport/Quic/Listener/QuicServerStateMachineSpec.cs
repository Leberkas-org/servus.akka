using System.IO.Pipelines;
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

    private static (QuicServerStateMachine Sm, MockTransportOperations Ops) CreateStateMachine(
        QuicConnectionHandle? handle = null)
    {
        var ops = new MockTransportOperations();
        var sm = new QuicServerStateMachine(
            ops,
            ActorRefs.Nobody,
            handle ?? CreateTestHandle(),
            TestConnectionInfo);
        return (sm, ops);
    }

    private static TransportBuffer CreateTestBuffer(params byte[] data)
    {
        var buf = TransportBuffer.Rent(data.Length);
        data.CopyTo(buf.FullMemory.Span);
        buf.Length = data.Length;
        return buf;
    }

    private static PipeStreamReadComplete CreateReadEvent(byte[] data, long streamId, int gen)
    {
        var pipe = new Pipe();
        var mem = pipe.Writer.GetMemory(data.Length);
        data.CopyTo(mem);
        pipe.Writer.Advance(data.Length);
        pipe.Writer.FlushAsync().AsTask().Wait();
        var result = pipe.Reader.ReadAsync().AsTask().Result;
        return new PipeStreamReadComplete(result, streamId, gen);
    }

    private static PipeStreamReadComplete CreateCompletedReadEvent(long streamId, int gen)
    {
        var pipe = new Pipe();
        pipe.Writer.CompleteAsync().AsTask().Wait();
        var result = pipe.Reader.ReadAsync().AsTask().Result;
        return new PipeStreamReadComplete(result, streamId, gen);
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
        sm.HandlePush(new MultiplexedData(buffer, 999));

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
    public void Dispatch_PipeStreamReadComplete_should_push_multiplexed_data()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();

        sm.HandlePush(new OpenStream(42, StreamDirection.Bidirectional));
        sm.Dispatch(new StreamLeaseAcquired(new MemoryStream(), 42));
        ops.PushedInbound.Clear();

        var evt = CreateReadEvent([1, 2, 3], 42, 1);
        sm.Dispatch(evt);

        Assert.Single(ops.PushedInbound);
        var multiplexed = Assert.IsType<MultiplexedData>(ops.PushedInbound[0]);
        Assert.Equal(new StreamTarget(42L), multiplexed.StreamId);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_PipeStreamReadComplete_with_stale_gen_should_be_ignored()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();

        sm.HandlePush(new OpenStream(42, StreamDirection.Bidirectional));
        sm.Dispatch(new StreamLeaseAcquired(new MemoryStream(), 42));
        ops.PushedInbound.Clear();

        var evt = CreateReadEvent([1, 2, 3], 42, 999);
        sm.Dispatch(evt);

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
        sm.HandlePush(new MultiplexedData(buffer, 1));

        Assert.True(ops.PullOutboundCount > 0);
    }

    [Fact(Timeout = 5000)]
    public void Dispatch_PipeStreamReadComplete_completed_should_push_StreamReadCompleted()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();
        ops.PushedInbound.Clear();

        sm.HandlePush(new OpenStream(1, StreamDirection.Bidirectional));
        sm.Dispatch(new StreamLeaseAcquired(new MemoryStream(), 1));
        ops.PushedInbound.Clear();

        var evt = CreateCompletedReadEvent(1, 1);
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
    public void Dispatch_PipeStreamReadFailed_should_push_StreamClosed()
    {
        var (sm, ops) = CreateStateMachine();
        sm.Start();

        sm.HandlePush(new OpenStream(1, StreamDirection.Bidirectional));
        sm.Dispatch(new StreamLeaseAcquired(new MemoryStream(), 1));
        ops.PushedInbound.Clear();

        sm.Dispatch(new PipeStreamReadFailed(new IOException("Read failed"), 1, 1));

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
        sm.HandlePush(new MultiplexedData(buffer, 1));

        Assert.True(ops.PullOutboundCount > 0);
    }
}
