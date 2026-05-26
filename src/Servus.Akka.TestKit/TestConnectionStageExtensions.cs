using System.Text;
using Servus.Akka.Transport;

namespace Servus.Akka.TestKit;

public static class TestConnectionStageExtensions
{
    public static void PushData(this TestConnectionStage stage, byte[] data)
        => stage.PushInbound(new TransportData(data));

    public static void PushData(this TestConnectionStage stage, string text)
        => stage.PushInbound(new TransportData(Encoding.UTF8.GetBytes(text)));

    public static void PushDisconnected(this TestConnectionStage stage,
        DisconnectReason reason = DisconnectReason.Graceful)
        => stage.PushInbound(new TransportDisconnected(reason));

    public static async Task<TransportData> WaitForDataAsync(this TestConnectionStage stage,
        CancellationToken ct = default)
    {
        while (true)
        {
            var msg = await stage.WaitForOutbound(ct).ConfigureAwait(false);
            if (msg is TransportData data)
            {
                return data;
            }
        }
    }

    public static void PushStreamOpened(this TestConnectionStage stage, long streamId,
        StreamDirection direction = StreamDirection.Bidirectional)
        => stage.PushInbound(new StreamOpened(streamId, direction));

    public static void PushStreamClosed(this TestConnectionStage stage, long streamId,
        DisconnectReason reason = DisconnectReason.Graceful)
        => stage.PushInbound(new StreamClosed(streamId, reason));

    public static void PushStreamReadCompleted(this TestConnectionStage stage, long streamId)
        => stage.PushInbound(new StreamReadCompleted(streamId));

    public static void PushServerStreamAccepted(this TestConnectionStage stage, long streamId,
        StreamDirection direction = StreamDirection.Unidirectional)
        => stage.PushInbound(new ServerStreamAccepted(streamId, direction));

    public static void PushMultiplexedData(this TestConnectionStage stage, long streamId, byte[] data)
    {
        var buf = TransportBuffer.Rent(data.Length);
        data.CopyTo(buf.FullMemory.Span);
        buf.Length = data.Length;
        stage.PushInbound(new MultiplexedData(buf, streamId));
    }

    public static void PushConnectionMigration(this TestConnectionStage stage, System.Net.EndPoint oldEndPoint,
        System.Net.EndPoint newEndPoint)
        => stage.PushInbound(new ConnectionMigrationDetected(oldEndPoint, newEndPoint));

    public static void SimulateInboundStream(this TestConnectionStage stage, long streamId, StreamDirection direction,
        params byte[][] frames)
    {
        stage.PushInbound(new ServerStreamAccepted(streamId, direction));

        foreach (var frame in frames)
        {
            var buf = TransportBuffer.Rent(frame.Length);
            frame.CopyTo(buf.FullMemory.Span);
            buf.Length = frame.Length;
            stage.PushInbound(new MultiplexedData(buf, streamId));
        }

        stage.PushInbound(new StreamReadCompleted(streamId));
    }

    public static async Task<MultiplexedData> WaitForMultiplexedDataAsync(this TestConnectionStage stage,
        CancellationToken ct = default)
    {
        while (true)
        {
            var msg = await stage.WaitForOutbound(ct).ConfigureAwait(false);
            if (msg is MultiplexedData data)
            {
                return data;
            }
        }
    }

    public static async Task<OpenStream> WaitForOpenStreamAsync(this TestConnectionStage stage,
        CancellationToken ct = default)
    {
        while (true)
        {
            var msg = await stage.WaitForOutbound(ct).ConfigureAwait(false);
            if (msg is OpenStream open)
            {
                return open;
            }
        }
    }
}