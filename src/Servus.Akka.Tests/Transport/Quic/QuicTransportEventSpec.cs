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
    public void PipeStreamReadComplete_should_carry_Buffer_StreamId_Gen()
    {
        var buf = TransportBuffer.Rent(4);
        new byte[] { 1, 2, 3, 4 }.CopyTo(buf.FullMemory.Span);
        buf.Length = 4;

        var evt = new PipeStreamReadComplete(buf, 42, 1, false);

        Assert.Equal(4, evt.Buffer!.Length);
        Assert.Equal(42, evt.StreamId);
        Assert.Equal(1, evt.Gen);
        Assert.False(evt.IsCompleted);

        buf.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void PipeStreamReadComplete_completed_should_have_null_buffer()
    {
        var evt = new PipeStreamReadComplete(null, 42, 1, true);

        Assert.Null(evt.Buffer);
        Assert.True(evt.IsCompleted);
    }

    [Fact(Timeout = 5000)]
    public void PipeStreamReadFailed_should_carry_Error_StreamId_Gen()
    {
        var error = new IOException("Read failed");
        const long streamId = 789L;
        const int gen = 3;

        var evt = new PipeStreamReadFailed(error, streamId, gen);

        Assert.Same(error, evt.Error);
        Assert.Equal(streamId, evt.StreamId);
        Assert.Equal(gen, evt.Gen);
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
