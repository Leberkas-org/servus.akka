using Akka;
using Akka.Streams.Dsl;

namespace Servus.Akka.Transport.Quic.Listener;

public sealed class QuicListenerFactory : IListenerFactory
{
    public Source<Flow<ITransportOutbound, ITransportInbound, NotUsed>, Task> Bind(ListenerOptions options)
    {
        if (options is not QuicListenerOptions quicOptions)
        {
            throw new ArgumentException(
                $"Expected {nameof(QuicListenerOptions)} but got {options.GetType().Name}",
                nameof(options));
        }

        return Source.FromGraph(new QuicListenerStage(quicOptions));
    }
}
