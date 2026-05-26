using System.Net.Security;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Quic.Listener;

namespace Servus.Akka.Tests.Transport.Quic.Listener;

public sealed class QuicListenerFactorySpec
{
    [Fact(Timeout = 5000)]
    public void Bind_should_return_non_null_source()
    {
        var factory = new QuicListenerFactory();

        var source = factory.Bind(new QuicListenerOptions
        {
            Host = "127.0.0.1",
            Port = 0,
            ServerCertificate = null!,
            ApplicationProtocols = [SslApplicationProtocol.Http3]
        });

        Assert.NotNull(source);
    }

    [Fact(Timeout = 5000)]
    public void Bind_should_throw_for_wrong_options_type()
    {
        var factory = new QuicListenerFactory();

        Assert.Throws<ArgumentException>(() =>
            factory.Bind(new TcpListenerOptions
            {
                Host = "127.0.0.1",
                Port = 0
            }));
    }

    [Fact(Timeout = 5000)]
    public void Bind_should_return_independent_sources()
    {
        var factory = new QuicListenerFactory();
        var options = new QuicListenerOptions
        {
            Host = "127.0.0.1",
            Port = 0,
            ServerCertificate = null!,
            ApplicationProtocols = [SslApplicationProtocol.Http3]
        };

        var source1 = factory.Bind(options);
        var source2 = factory.Bind(options);

        Assert.NotSame(source1, source2);
    }

    [Fact(Timeout = 5000)]
    public void Bind_with_custom_options_should_not_throw()
    {
        var factory = new QuicListenerFactory();
        var options = new QuicListenerOptions
        {
            Host = "127.0.0.1",
            Port = 0,
            ServerCertificate = null!,
            ApplicationProtocols = [SslApplicationProtocol.Http3],
            MaxInboundBidirectionalStreams = 50,
            MaxInboundUnidirectionalStreams = 5,
            IdleTimeout = TimeSpan.FromSeconds(60),
            Backlog = 64
        };

        var source = factory.Bind(options);

        Assert.NotNull(source);
    }

    [Fact(Timeout = 5000)]
    public void QuicListenerFactory_should_implement_IListenerFactory()
    {
        var factory = new QuicListenerFactory();

        Assert.IsAssignableFrom<IListenerFactory>(factory);
    }
}