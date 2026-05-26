using Akka;
using Akka.Streams.Dsl;

namespace Servus.Akka.Transport.Tcp.Listener;

public sealed class TcpListenerFactory : IListenerFactory
{
    public Source<Flow<ITransportOutbound, ITransportInbound, NotUsed>, Task> Bind(ListenerOptions options)
    {
        if (options is not TcpListenerOptions tcpOptions)
        {
            throw new ArgumentException(
                $"Expected {nameof(TcpListenerOptions)} but got {options.GetType().Name}",
                nameof(options));
        }

        return Source.FromGraph(new TcpListenerStage(tcpOptions));
    }
}
