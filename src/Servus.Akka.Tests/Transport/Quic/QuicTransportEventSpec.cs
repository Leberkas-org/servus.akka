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
    public void StreamLeaseAcquired_should_implement_IQuicTransportEvent()
    {
        var stream = new MemoryStream();
        var handle = new StreamHandle(stream);
        const long streamId = 42L;

        var evt = new StreamLeaseAcquired(handle, streamId);

        Assert.Same(handle, evt.Handle);
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
    public void InboundData_should_implement_IQuicTransportEvent()
    {
        var buffer = TransportBuffer.Rent(16);
        try
        {
            const long streamId = 123L;
            const int gen = 5;

            var evt = new InboundData(buffer, streamId, gen);

            Assert.NotNull(evt.Buffer);
            Assert.Equal(streamId, evt.StreamId);
            Assert.Equal(gen, evt.Gen);
        }
        finally
        {
            buffer.Dispose();
        }
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
    public void InboundComplete_should_implement_IQuicTransportEvent()
    {
        const DisconnectReason reason = DisconnectReason.Graceful;
        const int gen = 3;
        const long streamId = 456L;

        var evt = new InboundComplete(reason, gen, streamId);

        Assert.Equal(reason, evt.Reason);
        Assert.Equal(gen, evt.Gen);
        Assert.Equal(streamId, evt.StreamId);
    }

    [Fact(Timeout = 5000)]
    public void InboundPumpFailed_should_implement_IQuicTransportEvent()
    {
        var error = new TimeoutException("Pump failed");
        const long streamId = 789L;

        var evt = new InboundPumpFailed(error, streamId);

        Assert.Same(error, evt.Error);
        Assert.Equal(streamId, evt.StreamId);
    }

    [Fact(Timeout = 5000)]
    public void OutboundWriteDone_should_implement_IQuicTransportEvent()
    {
        const long streamId = 321L;

        var evt = new OutboundWriteDone(streamId);

        Assert.Equal(streamId, evt.StreamId);
    }

    [Fact(Timeout = 5000)]
    public void OutboundWriteFailed_should_implement_IQuicTransportEvent()
    {
        var error = new IOException("Write failed");
        const long streamId = 654L;

        var evt = new OutboundWriteFailed(error, streamId);

        Assert.Same(error, evt.Error);
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