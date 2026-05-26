using Akka;
using Akka.Streams.Dsl;

namespace Servus.Akka.Transport;

public interface IListenerFactory
{
    Source<Flow<ITransportOutbound, ITransportInbound, NotUsed>, Task> Bind(ListenerOptions options);
}
