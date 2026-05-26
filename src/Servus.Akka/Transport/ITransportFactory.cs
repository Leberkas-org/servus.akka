using Akka;
using Akka.Streams.Dsl;

namespace Servus.Akka.Transport;

public interface ITransportFactory
{
    Flow<ITransportOutbound, ITransportInbound, NotUsed> Create();
}