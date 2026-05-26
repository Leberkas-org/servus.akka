using Servus.Akka.Transport;

namespace Servus.Akka.Tests.Transport;

public sealed class MultiplexedMessagesSpec
{
    [Fact(Timeout = 5000)]
    public void OpenStream_should_implement_ITransportOutbound()
    {
        ITransportOutbound msg = new OpenStream(42, StreamDirection.Bidirectional);

        Assert.IsType<OpenStream>(msg);
    }

    [Fact(Timeout = 5000)]
    public void OpenStream_should_carry_stream_id_and_direction()
    {
        var msg = new OpenStream(7, StreamDirection.Unidirectional);

        Assert.Equal(new StreamTarget(7), msg.StreamId);
        Assert.Equal(StreamDirection.Unidirectional, msg.Direction);
    }

    [Fact(Timeout = 5000)]
    public void CloseStream_should_implement_ITransportOutbound()
    {
        ITransportOutbound msg = new CloseStream(99);

        Assert.IsType<CloseStream>(msg);
    }

    [Fact(Timeout = 5000)]
    public void CloseStream_should_carry_stream_id()
    {
        var msg = new CloseStream(55);

        Assert.Equal(new StreamTarget(55), msg.StreamId);
    }

    [Fact(Timeout = 5000)]
    public void StreamOpened_should_implement_ITransportInbound()
    {
        ITransportInbound msg = new StreamOpened(1, StreamDirection.Bidirectional);

        Assert.IsType<StreamOpened>(msg);
    }

    [Fact(Timeout = 5000)]
    public void StreamOpened_should_carry_stream_id_and_direction()
    {
        var msg = new StreamOpened(3, StreamDirection.Unidirectional);

        Assert.Equal(new StreamTarget(3), msg.Id);
        Assert.Equal(StreamDirection.Unidirectional, msg.Direction);
    }

    [Fact(Timeout = 5000)]
    public void StreamClosed_should_implement_ITransportInbound()
    {
        ITransportInbound msg = new StreamClosed(10, DisconnectReason.Graceful);

        Assert.IsType<StreamClosed>(msg);
    }

    [Fact(Timeout = 5000)]
    public void StreamClosed_should_carry_stream_id_and_reason()
    {
        var msg = new StreamClosed(22, DisconnectReason.Error);

        Assert.Equal(new StreamTarget(22), msg.Id);
        Assert.Equal(DisconnectReason.Error, msg.Reason);
    }

    [Fact(Timeout = 5000)]
    public void CompleteWrites_should_implement_ITransportOutbound()
    {
        ITransportOutbound msg = new CompleteWrites(42);
        var cw = Assert.IsType<CompleteWrites>(msg);
        Assert.Equal(new StreamTarget(42), cw.StreamId);
    }

    [Fact(Timeout = 5000)]
    public void ResetStream_should_implement_ITransportOutbound()
    {
        ITransportOutbound msg = new ResetStream(7, 0x0104);
        var rs = Assert.IsType<ResetStream>(msg);
        Assert.Equal(new StreamTarget(7), rs.StreamId);
        Assert.Equal(0x0104, rs.ErrorCode);
    }

    [Fact(Timeout = 5000)]
    public void ServerStreamAccepted_should_implement_ITransportInbound()
    {
        ITransportInbound msg = new ServerStreamAccepted(3, StreamDirection.Unidirectional);
        var ssa = Assert.IsType<ServerStreamAccepted>(msg);
        Assert.Equal(new StreamTarget(3), ssa.Id);
        Assert.Equal(StreamDirection.Unidirectional, ssa.Direction);
    }

    [Fact(Timeout = 5000)]
    public void StreamReadCompleted_should_implement_ITransportInbound()
    {
        ITransportInbound msg = new StreamReadCompleted(0);
        var src = Assert.IsType<StreamReadCompleted>(msg);
        Assert.Equal(new StreamTarget(0), src.Id);
    }
}
