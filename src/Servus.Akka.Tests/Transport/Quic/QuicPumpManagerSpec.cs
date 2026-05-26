using Akka.TestKit.Xunit;
using Servus.Akka.Tests.Utils;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Quic;

namespace Servus.Akka.Tests.Transport.Quic;

public sealed class QuicPumpManagerSpec : TestKit
{
    [Fact(Timeout = 5000)]
    public void StartInboundPump_should_emit_InboundData_for_readable_stream()
    {
        var ms = new MemoryStream([0x01, 0x02, 0x03]);
        var handle = new StreamHandle(ms);
        var manager = new QuicPumpManager(TestActor);

        manager.StartInboundPump(handle, streamId: 42, gen: 1);

        var msg = ExpectMsg<InboundData>(TimeSpan.FromSeconds(3),
            cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(42, msg.StreamId);
        Assert.Equal(1, msg.Gen);
        Assert.True(msg.Buffer.Length > 0);
        msg.Buffer.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void StartInboundPump_should_emit_InboundComplete_when_stream_ends()
    {
        var ms = new MemoryStream([]);
        var handle = new StreamHandle(ms);
        var manager = new QuicPumpManager(TestActor);

        manager.StartInboundPump(handle, streamId: 43, gen: 2);

        var msg = ExpectMsg<InboundComplete>(TimeSpan.FromSeconds(3),
            cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(43, msg.StreamId);
        Assert.Equal(2, msg.Gen);
        Assert.Equal(DisconnectReason.Graceful, msg.Reason);
    }

    [Fact(Timeout = 5000)]
    public void StopAll_should_cancel_pumps()
    {
        var ms = new SlowStream();
        var handle = new StreamHandle(ms);
        var manager = new QuicPumpManager(TestActor);

        manager.StartInboundPump(handle, streamId: 44, gen: 3);

        // Give pump a moment to start
        Thread.Sleep(50);

        manager.StopAll();

        // Verify pump is cancelled — expect no messages after a brief timeout
        ExpectNoMsg(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);
    }

    [Fact(Timeout = 5000)]
    public void StartInboundPump_should_emit_InboundPumpFailed_on_error()
    {
        var failStream = new FailingStream();
        var handle = new StreamHandle(failStream);
        var manager = new QuicPumpManager(TestActor);

        manager.StartInboundPump(handle, streamId: 45, gen: 4);

        var msg = ExpectMsg<InboundPumpFailed>(TimeSpan.FromSeconds(3),
            cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(45, msg.StreamId);
        Assert.IsType<IOException>(msg.Error);
    }
}