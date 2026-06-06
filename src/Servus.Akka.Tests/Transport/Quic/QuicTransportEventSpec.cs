using System.IO.Pipelines;
using System.Net;
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
    public void PipeStreamReadComplete_should_carry_ReadResult_StreamId_Gen()
    {
        var pipe = new Pipe();
        var mem = pipe.Writer.GetMemory(1);
        mem.Span[0] = 0xAB;
        pipe.Writer.Advance(1);
        pipe.Writer.FlushAsync().AsTask().Wait();
        var result = pipe.Reader.ReadAsync().AsTask().Result;

        const long streamId = 123L;
        const int gen = 5;

        var evt = new PipeStreamReadComplete(result, streamId, gen);

        Assert.Equal(streamId, evt.StreamId);
        Assert.Equal(gen, evt.Gen);
        Assert.True(evt.Result.Buffer.Length > 0);

        pipe.Reader.AdvanceTo(result.Buffer.End);
        pipe.Reader.CompleteAsync().AsTask().Wait();
        pipe.Writer.CompleteAsync().AsTask().Wait();
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
