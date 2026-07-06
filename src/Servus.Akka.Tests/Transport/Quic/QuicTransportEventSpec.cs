using System.Net;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Quic;
using Servus.Akka.Transport.Quic.Client;
using QuicInboundStreamAccepted = Servus.Akka.Transport.Quic.InboundStreamAccepted;

namespace Servus.Akka.Tests.Transport.Quic;

public sealed class QuicTransportEventSpec
{
    private QuicConnectionHandle CreateTestConnectionHandle() =>
        new(
            openStream: (_, _) => Task.FromResult((Stream: (Stream)new MemoryStream(), StreamId: 0L)),
            acceptInboundStream: _ => Task.FromResult<(Stream, long)?>(null),
            getLocalEndPoint: () => null,
            getRemoteEndPoint: () => null,
            dispose: () => ValueTask.CompletedTask);

    [Fact(Timeout = 5000)]
    public void ConnectionLeaseAcquired_should_implement_IQuicTransportEvent()
    {
        var handle = CreateTestConnectionHandle();
        var lease = new QuicConnectionLease(handle, 10);
        var evt = new ConnectionLeaseAcquired(lease);

        Assert.Same(lease, evt.Lease);
    }

    [Fact(Timeout = 5000)]
    public void StreamLeaseAcquired_should_carry_Stream_and_StreamId()
    {
        var stream = new MemoryStream();
        const long streamId = 42L;

        var evt = new StreamLeaseAcquired(stream, streamId);

        Assert.Same(stream, evt.Stream);
        Assert.Equal(streamId, evt.StreamId);
    }

    [Fact(Timeout = 5000)]
    public void AcquisitionFailed_should_implement_IQuicTransportEvent()
    {
        var error = new InvalidOperationException("Test error");
        var evt = new AcquisitionFailed(error);

        Assert.Same(error, evt.Error);
    }

    [Fact(Timeout = 5000)]
    public void DirectStreamReadComplete_should_carry_State_and_BytesRead()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        state.ActivateDirectReadForTest(42);

        var evt = new DirectStreamReadComplete(state, 4);

        Assert.Same(state, evt.State);
        Assert.Equal(4, evt.BytesRead);
        Assert.Equal(42, evt.State.StreamId);
    }

    [Fact(Timeout = 5000)]
    public void PipeStreamReadFailed_should_carry_State_and_Error()
    {
        var state = QuicStreamState.Rent(StreamDirection.Bidirectional, null);
        state.ActivateDirectReadForTest(789);
        var error = new IOException("Read failed");

        var evt = new PipeStreamReadFailed(state, error);

        Assert.Same(error, evt.Error);
        Assert.Same(state, evt.State);
        Assert.Equal(789, evt.State.StreamId);
    }

    [Fact(Timeout = 5000)]
    public void InboundStreamAccepted_should_implement_IQuicTransportEvent()
    {
        var stream = new MemoryStream();
        const long streamId = 999L;

        var evt = new QuicInboundStreamAccepted(stream, streamId);

        Assert.Same(stream, evt.Stream);
        Assert.Equal(streamId, evt.StreamId);
    }

    [Fact(Timeout = 5000)]
    public void MigrationDetected_should_implement_IQuicTransportEvent()
    {
        var oldEndPoint = new IPEndPoint(IPAddress.Loopback, 8000);
        var newEndPoint = new IPEndPoint(IPAddress.Loopback, 8001);

        var evt = new MigrationDetected(oldEndPoint, newEndPoint);

        Assert.Same(oldEndPoint, evt.OldEndPoint);
        Assert.Same(newEndPoint, evt.NewEndPoint);
    }
}
