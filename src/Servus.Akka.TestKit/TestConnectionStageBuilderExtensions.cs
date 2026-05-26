using Servus.Akka.Transport;

namespace Servus.Akka.TestKit;

public static class TestConnectionStageBuilderExtensions
{
    public static TestConnectionStageBuilder OnData(this TestConnectionStageBuilder builder, Action<TransportData, IStageContext> handler)
        => builder.OnOutbound(handler);

    public static TestConnectionStageBuilder OnOpenStream(this TestConnectionStageBuilder builder, Action<OpenStream, IStageContext> handler)
        => builder.OnOutbound(handler);

    public static TestConnectionStageBuilder OnMultiplexedData(this TestConnectionStageBuilder builder, Action<MultiplexedData, IStageContext> handler)
        => builder.OnOutbound(handler);

    public static TestConnectionStageBuilder OnCompleteWrites(this TestConnectionStageBuilder builder, Action<CompleteWrites, IStageContext> handler)
        => builder.OnOutbound(handler);

    public static TestConnectionStageBuilder OnResetStream(this TestConnectionStageBuilder builder, Action<ResetStream, IStageContext> handler)
        => builder.OnOutbound(handler);

    public static TestConnectionStageBuilder OnDisconnect(this TestConnectionStageBuilder builder, Action<DisconnectTransport, IStageContext> handler)
        => builder.OnOutbound(handler);

    public static TestConnectionStageBuilder AutoStreamOpened(this TestConnectionStageBuilder builder, long streamId, StreamDirection direction = StreamDirection.Bidirectional)
    {
        return builder.OnOutbound<OpenStream>((open, ctx) =>
        {
            if (open.StreamId == streamId)
            {
                ctx.Push(new StreamOpened(streamId, direction));
            }
        });
    }

    public static TestConnectionStageBuilder EchoMultiplexedData(this TestConnectionStageBuilder builder)
    {
        return builder.OnOutbound<MultiplexedData>((data, ctx) =>
        {
            var echo = TransportBuffer.Rent(data.Buffer.Length);
            data.Buffer.Span.CopyTo(echo.FullMemory.Span);
            echo.Length = data.Buffer.Length;
            ctx.Push(data with { Buffer = echo });
        });
    }
}
