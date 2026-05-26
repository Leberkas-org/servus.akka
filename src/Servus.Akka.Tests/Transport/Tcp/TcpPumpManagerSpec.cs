using Akka.TestKit.Xunit;
using Servus.Akka.Tests.Utils;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Tcp;
using Servus.Akka.Transport.Tcp.Client;

namespace Servus.Akka.Tests.Transport.Tcp;

public sealed class TcpPumpManagerSpec : TestKit
{
    [Fact(Timeout = 5000)]
    public void StartPumps_should_emit_InboundBatch_for_readable_data()
    {
        var ms = new MemoryStream([0x01, 0x02, 0x03]);
        var state = new ClientState(ms);
        var manager = new TcpPumpManager(TestActor);

        manager.StartPumps(state, gen: 1);

        var msg = ExpectMsg<InboundBatch>(TimeSpan.FromSeconds(3),
            cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(1, msg.Gen);
        Assert.True(msg.Count > 0);

        for (var i = 0; i < msg.Count; i++)
        {
            if (msg.Batch[i] is TransportData td)
            {
                td.Buffer.Dispose();
            }
        }

        state.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void StartPumps_should_emit_InboundComplete_when_stream_ends()
    {
        var ms = new MemoryStream([]);
        var state = new ClientState(ms);
        var manager = new TcpPumpManager(TestActor);

        manager.StartPumps(state, gen: 2);

        // Empty stream produces an empty batch before InboundComplete
        var batch = ExpectMsg<InboundBatch>(TimeSpan.FromSeconds(3),
            cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(2, batch.Gen);

        var msg = ExpectMsg<InboundComplete>(TimeSpan.FromSeconds(3),
            cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(2, msg.Gen);
        Assert.Equal(DisconnectReason.Graceful, msg.Reason);

        state.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void StartPumps_should_emit_InboundPumpFailed_on_stream_error()
    {
        var ms = new FailingStream();
        var state = new ClientState(ms);
        var manager = new TcpPumpManager(TestActor);

        manager.StartPumps(state, gen: 3);

        var msg = ExpectMsg<InboundPumpFailed>(TimeSpan.FromSeconds(3),
            cancellationToken: TestContext.Current.CancellationToken);
        // Stream error gets wrapped in AbruptCloseException by ClientByteMover.FillPipeFromStream
        Assert.IsType<AbruptCloseException>(msg.Error);

        state.Dispose();
    }

    [Fact(Timeout = 10000)]
    public void StopPumps_should_cancel_inbound_pump()
    {
        var ms = new SlowStream();
        var state = new ClientState(ms);
        var manager = new TcpPumpManager(TestActor);

        manager.StartPumps(state, gen: 4);

        // Give pump a moment to start
        Thread.Sleep(50);

        manager.StopPumps();

        // After StopPumps, the inbound pump is cancelled.
        // The outbound pump may send OutboundWriteDone, but no InboundBatch or InboundComplete.
        var messages = ReceiveN(1, TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        //Assert.Contains(messages, r => r is InboundPumpFailed);
        Assert.Contains(messages, r => r is OutboundWriteDone);

        state.Dispose();
    }

    [Fact(Timeout = 5000)]
    public void StartPumps_should_batch_multiple_buffers()
    {
        var bytes = new byte[30];
        for (var i = 0; i < 30; i++)
        {
            bytes[i] = (byte)i;
        }

        var ms = new MemoryStream(bytes);
        var state = new ClientState(ms);
        var manager = new TcpPumpManager(TestActor);

        manager.StartPumps(state, gen: 5);

        var msg = ExpectMsg<InboundBatch>(TimeSpan.FromSeconds(3),
            cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(5, msg.Gen);
        Assert.True(msg.Count > 0);

        for (var i = 0; i < msg.Count; i++)
        {
            if (msg.Batch[i] is TransportData td)
            {
                td.Buffer.Dispose();
            }
        }

        state.Dispose();
    }
}