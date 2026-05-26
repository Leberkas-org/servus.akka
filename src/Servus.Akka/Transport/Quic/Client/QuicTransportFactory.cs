using Akka;
using Akka.Actor;
using Akka.Streams.Dsl;

namespace Servus.Akka.Transport.Quic.Client;

public sealed class QuicTransportFactory(IActorRef connectionManager) : ITransportFactory
{
    public Flow<ITransportOutbound, ITransportInbound, NotUsed> Create()
    {
        var conflate = Flow.Create<ITransportOutbound>()
            .ConflateWithSeed(
                seed: item => new List<ITransportOutbound> { item },
                aggregate: (list, item) =>
                {
                    list.Add(item);
                    return list;
                });

        var stage = Flow.FromGraph(new QuicConnectionStage(connectionManager));

        return conflate.Via(stage);
    }
}