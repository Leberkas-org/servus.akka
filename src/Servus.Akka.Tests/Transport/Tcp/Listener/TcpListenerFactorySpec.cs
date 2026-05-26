using System.Net.Security;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Tcp.Listener;

namespace Servus.Akka.Tests.Transport.Tcp.Listener;

public sealed class TcpListenerFactorySpec
{
    [Fact(Timeout = 5000)]
    public void Bind_should_return_non_null_source()
    {
        var factory = new TcpListenerFactory();

        var source = factory.Bind(new TcpListenerOptions { Host = "127.0.0.1", Port = 0 });

        Assert.NotNull(source);
    }

    [Fact(Timeout = 5000)]
    public void Bind_should_throw_for_wrong_options_type()
    {
        var factory = new TcpListenerFactory();

        Assert.Throws<ArgumentException>(() =>
            factory.Bind(new QuicListenerOptions
            {
                Host = "127.0.0.1",
                Port = 0,
                ServerCertificate = null!,
                ApplicationProtocols = [SslApplicationProtocol.Http11]
            }));
    }

    [Fact(Timeout = 5000)]
    public void Bind_should_return_independent_sources()
    {
        var factory = new TcpListenerFactory();
        var options = new TcpListenerOptions { Host = "127.0.0.1", Port = 0 };

        var source1 = factory.Bind(options);
        var source2 = factory.Bind(options);

        Assert.NotSame(source1, source2);
    }

    [Fact(Timeout = 5000)]
    public void Bind_with_custom_options_should_not_throw()
    {
        var factory = new TcpListenerFactory();
        var options = new TcpListenerOptions
        {
            Host = "127.0.0.1",
            Port = 0,
            ReuseAddress = false,
            NoDelay = false,
            Backlog = 256,
            SocketSendBufferSize = 4096,
            SocketReceiveBufferSize = 4096
        };

        var source = factory.Bind(options);

        Assert.NotNull(source);
    }

    [Fact(Timeout = 5000)]
    public void TcpListenerFactory_should_implement_IListenerFactory()
    {
        var factory = new TcpListenerFactory();

        Assert.IsAssignableFrom<IListenerFactory>(factory);
    }
}
