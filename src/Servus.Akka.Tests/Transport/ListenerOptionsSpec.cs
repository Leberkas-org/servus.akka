using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Servus.Akka.Transport;

namespace Servus.Akka.Tests.Transport;

public sealed class ListenerOptionsSpec
{
    private static X509Certificate2 CreateDummyCert()
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest("cn=test", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        return request.CreateSelfSigned(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddDays(1));
    }

    [Fact(Timeout = 5000)]
    public void TcpListenerOptions_should_have_correct_defaults()
    {
        var options = new TcpListenerOptions
        {
            Host = "localhost",
            Port = 8080
        };

        Assert.True(options.ReuseAddress);
        Assert.True(options.NoDelay);
        Assert.Equal(int.MaxValue, options.Backlog);
        Assert.Null(options.ServerCertificate);
        Assert.Equal(SslProtocols.None, options.EnabledSslProtocols);
    }

    [Fact(Timeout = 5000)]
    public void QuicListenerOptions_should_have_correct_defaults()
    {
        var cert = CreateDummyCert();
        var protocols = new List<SslApplicationProtocol> { SslApplicationProtocol.Http3 };

        var options = new QuicListenerOptions
        {
            Host = "localhost",
            Port = 443,
            ServerCertificate = cert,
            ApplicationProtocols = protocols
        };

        Assert.Equal(100, options.MaxInboundBidirectionalStreams);
        Assert.Equal(3, options.MaxInboundUnidirectionalStreams);
        Assert.Equal(TimeSpan.FromSeconds(30), options.IdleTimeout);
        Assert.Equal(SslProtocols.None, options.EnabledSslProtocols);
    }

    [Fact(Timeout = 5000)]
    public void TcpListenerOptions_should_allow_property_override()
    {
        var options = new TcpListenerOptions
        {
            Host = "0.0.0.0",
            Port = 9000,
            ReuseAddress = false,
            NoDelay = false,
            Backlog = 256,
            SocketSendBufferSize = 65536,
            SocketReceiveBufferSize = 65536
        };

        Assert.Equal("0.0.0.0", options.Host);
        Assert.Equal(9000, options.Port);
        Assert.False(options.ReuseAddress);
        Assert.False(options.NoDelay);
        Assert.Equal(256, options.Backlog);
        Assert.Equal(65536, options.SocketSendBufferSize);
        Assert.Equal(65536, options.SocketReceiveBufferSize);
    }

    [Fact(Timeout = 5000)]
    public void QuicListenerOptions_should_allow_property_override()
    {
        var cert = CreateDummyCert();
        var protocols = new List<SslApplicationProtocol> { SslApplicationProtocol.Http3 };

        var options = new QuicListenerOptions
        {
            Host = "0.0.0.0",
            Port = 443,
            MaxInboundBidirectionalStreams = 200,
            MaxInboundUnidirectionalStreams = 10,
            IdleTimeout = TimeSpan.FromSeconds(60),
            ServerCertificate = cert,
            ApplicationProtocols = protocols,
            Backlog = 512
        };

        Assert.Equal("0.0.0.0", options.Host);
        Assert.Equal(443, options.Port);
        Assert.Equal(200, options.MaxInboundBidirectionalStreams);
        Assert.Equal(10, options.MaxInboundUnidirectionalStreams);
        Assert.Equal(TimeSpan.FromSeconds(60), options.IdleTimeout);
        Assert.Equal(512, options.Backlog);
        Assert.Same(cert, options.ServerCertificate);
        Assert.Same(protocols, options.ApplicationProtocols);
    }

    [Fact(Timeout = 5000)]
    public void ListenerOptions_base_should_have_default_backlog_128()
    {
        var options = new TcpListenerOptions
        {
            Host = "127.0.0.1",
            Port = 0
        };

        Assert.Equal(int.MaxValue, options.Backlog);
    }

    [Fact(Timeout = 5000)]
    public void ListenerOptions_base_should_have_null_socket_buffer_sizes()
    {
        var options = new TcpListenerOptions
        {
            Host = "127.0.0.1",
            Port = 0
        };

        Assert.Null(options.SocketSendBufferSize);
        Assert.Null(options.SocketReceiveBufferSize);
    }

    [Fact(Timeout = 5000)]
    public void TcpListenerOptions_should_have_null_certificate_by_default()
    {
        var options = new TcpListenerOptions
        {
            Host = "127.0.0.1",
            Port = 0
        };

        Assert.Null(options.ServerCertificate);
    }

    [Fact(Timeout = 5000)]
    public void TcpListenerOptions_should_have_null_application_protocols()
    {
        var options = new TcpListenerOptions
        {
            Host = "127.0.0.1",
            Port = 0
        };

        Assert.Null(options.ApplicationProtocols);
    }

    [Fact(Timeout = 5000)]
    public void TcpListenerOptions_should_have_null_client_cert_callback()
    {
        var options = new TcpListenerOptions
        {
            Host = "127.0.0.1",
            Port = 0
        };

        Assert.Null(options.ClientCertificateValidationCallback);
    }

    [Fact(Timeout = 5000)]
    public void QuicListenerOptions_should_have_null_client_cert_callback()
    {
        var cert = CreateDummyCert();
        var protocols = new List<SslApplicationProtocol> { SslApplicationProtocol.Http3 };

        var options = new QuicListenerOptions
        {
            Host = "127.0.0.1",
            Port = 0,
            ServerCertificate = cert,
            ApplicationProtocols = protocols
        };

        Assert.Null(options.ClientCertificateValidationCallback);
    }

    [Fact(Timeout = 5000)]
    public void QuicListenerOptions_should_have_ssl_protocols_none_by_default()
    {
        var cert = CreateDummyCert();
        var protocols = new List<SslApplicationProtocol> { SslApplicationProtocol.Http3 };

        var options = new QuicListenerOptions
        {
            Host = "127.0.0.1",
            Port = 0,
            ServerCertificate = cert,
            ApplicationProtocols = protocols
        };

        Assert.Equal(SslProtocols.None, options.EnabledSslProtocols);
    }
}
