using Akka;
using Akka.Actor;
using Akka.Streams.Dsl;

namespace Servus.Akka.Transport.Quic.Client;

public sealed class QuicTransportFactory(IActorRef connectionManager) : ITransportFactory
{
    // No conflate batching: per-stream channel send loops (StreamConnection) already coalesce small
    // buffers into one write, so the stage takes single items — same shape as the server side
    // (QuicServerConnectionStage).
    public Flow<ITransportOutbound, ITransportInbound, NotUsed> Create()
        => Flow.FromGraph(new QuicConnectionStage(connectionManager));
}