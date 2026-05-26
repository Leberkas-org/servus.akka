using System.Net;
using System.Net.Security;
using System.Security.Authentication;
using Servus.Akka.Transport;

namespace Servus.Akka.Tests.Transport;

public sealed class TransportOptionsSpec
{
    [Fact(Timeout = 5000)]
    public void TcpTransportOptions_should_have_default_connect_timeout()
    {
        var opts = new TcpTransportOptions
        {
            Host = "localhost",
            Port = 80
        };

        Assert.Equal(TimeSpan.FromSeconds(10), opts.ConnectTimeout);
    }

    [Fact(Timeout = 5000)]
    public void TcpTransportOptions_should_be_assignable_to_TransportOptions()
    {
        TransportOptions opts = new TcpTransportOptions
        {
            Host = "localhost",
            Port = 80
        };

        Assert.IsType<TcpTransportOptions>(opts);
    }

    [Fact(Timeout = 5000)]
    public void TlsTransportOptions_should_be_assignable_to_TransportOptions()
    {
        TransportOptions opts = new TlsTransportOptions
        {
            Host = "localhost",
            Port = 443
        };

        Assert.IsType<TlsTransportOptions>(opts);
    }

    [Fact(Timeout = 5000)]
    public void QuicTransportOptions_should_be_assignable_to_TransportOptions()
    {
        TransportOptions opts = new QuicTransportOptions
        {
            Host = "localhost",
            Port = 443
        };

        Assert.IsType<QuicTransportOptions>(opts);
    }

    [Fact(Timeout = 5000)]
    public void TcpTransportOptions_should_expose_proxy_settings()
    {
        var proxy = new WebProxy("http://proxy:8080");
        var opts = new TcpTransportOptions
        {
            Host = "localhost",
            Port = 80,
            UseProxy = true,
            Proxy = proxy,
            DefaultProxyCredentials = CredentialCache.DefaultCredentials
        };

        Assert.True(opts.UseProxy);
        Assert.Same(proxy, opts.Proxy);
        Assert.Same(CredentialCache.DefaultCredentials, opts.DefaultProxyCredentials);
    }

    [Fact(Timeout = 5000)]
    public void TlsTransportOptions_should_expose_tls_settings()
    {
        var opts = new TlsTransportOptions
        {
            Host = "example.com",
            Port = 443,
            TargetHost = "example.com",
            EnabledSslProtocols = SslProtocols.Tls13,
            ApplicationProtocols = [SslApplicationProtocol.Http2]
        };

        Assert.Equal("example.com", opts.TargetHost);
        Assert.Equal(SslProtocols.Tls13, opts.EnabledSslProtocols);
        Assert.Single(opts.ApplicationProtocols);
    }

    [Fact(Timeout = 5000)]
    public void TlsTransportOptions_should_default_ssl_protocols_to_none()
    {
        var opts = new TlsTransportOptions
        {
            Host = "example.com",
            Port = 443
        };

        Assert.Equal(SslProtocols.None, opts.EnabledSslProtocols);
    }

    [Fact(Timeout = 5000)]
    public void QuicTransportOptions_should_have_correct_defaults()
    {
        var opts = new QuicTransportOptions
        {
            Host = "example.com",
            Port = 443
        };

        Assert.Equal(TimeSpan.FromSeconds(30), opts.IdleTimeout);
        Assert.Equal(100, opts.MaxBidirectionalStreams);
        Assert.Equal(3, opts.MaxUnidirectionalStreams);
        Assert.True(opts.AllowConnectionMigration);
    }

    [Fact(Timeout = 5000)]
    public void Equality_should_be_case_insensitive_for_host()
    {
        var a = new TcpTransportOptions { Host = "EXAMPLE.COM", Port = 80 };
        var b = new TcpTransportOptions { Host = "example.com", Port = 80 };

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact(Timeout = 5000)]
    public void Equality_should_differ_for_different_ports()
    {
        var a = new TcpTransportOptions { Host = "example.com", Port = 80 };
        var b = new TcpTransportOptions { Host = "example.com", Port = 8080 };

        Assert.NotEqual(a, b);
    }

    [Fact(Timeout = 5000)]
    public void Equality_should_differ_across_transport_types()
    {
        var tcp = new TcpTransportOptions { Host = "example.com", Port = 443 };
        var tls = new TlsTransportOptions { Host = "example.com", Port = 443 };

        Assert.False(tcp.Equals(tls));
    }

    [Fact(Timeout = 5000)]
    public void Equality_should_match_identical_tcp_options()
    {
        var a = new TcpTransportOptions { Host = "example.com", Port = 80 };
        var b = new TcpTransportOptions { Host = "example.com", Port = 80 };

        Assert.Equal(a, b);
        Assert.True(a == b);
    }

    [Fact(Timeout = 5000)]
    public void Equality_should_match_identical_quic_options()
    {
        var a = new QuicTransportOptions { Host = "example.com", Port = 443 };
        var b = new QuicTransportOptions { Host = "example.com", Port = 443 };

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact(Timeout = 5000)]
    public void GetHashCode_should_be_case_insensitive_for_host()
    {
        var a = new TlsTransportOptions { Host = "EXAMPLE.COM", Port = 443 };
        var b = new TlsTransportOptions { Host = "example.com", Port = 443 };

        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact(Timeout = 5000)]
    public void TransportOptions_should_work_as_dictionary_key()
    {
        var dict = new Dictionary<TransportOptions, string>();
        var key = new TcpTransportOptions { Host = "example.com", Port = 80 };
        var sameCaseDifferent = new TcpTransportOptions { Host = "EXAMPLE.COM", Port = 80 };

        dict[key] = "pooled";

        Assert.True(dict.ContainsKey(sameCaseDifferent));
        Assert.Equal("pooled", dict[sameCaseDifferent]);
    }

    [Fact(Timeout = 5000)]
    public void SocketBufferSizes_should_default_to_null()
    {
        var opts = new TcpTransportOptions { Host = "localhost", Port = 80 };

        Assert.Null(opts.SocketSendBufferSize);
        Assert.Null(opts.SocketReceiveBufferSize);
    }

    [Fact(Timeout = 5000)]
    public void SocketBufferSizes_should_be_settable()
    {
        var opts = new TcpTransportOptions
        {
            Host = "localhost",
            Port = 80,
            SocketSendBufferSize = 65536,
            SocketReceiveBufferSize = 131072
        };

        Assert.Equal(65536, opts.SocketSendBufferSize);
        Assert.Equal(131072, opts.SocketReceiveBufferSize);
    }

    [Fact(Timeout = 5000)]
    public void Equals_should_return_false_for_null()
    {
        var opts = new TcpTransportOptions { Host = "localhost", Port = 80 };

        Assert.False(opts.Equals(null));
        Assert.False(opts == null);
        Assert.True(opts != null);
    }

    [Fact(Timeout = 5000)]
    public void Equals_should_return_true_for_same_reference()
    {
        var opts = new TcpTransportOptions { Host = "localhost", Port = 80 };

        Assert.True(opts.Equals(opts));
        Assert.True(ReferenceEquals(opts, opts));
    }

    [Fact(Timeout = 5000)]
    public void Equals_should_return_true_for_same_values_different_instances()
    {
        var a = new TcpTransportOptions { Host = "example.com", Port = 8080 };
        var b = new TcpTransportOptions { Host = "example.com", Port = 8080 };

        Assert.True(a.Equals(b));
        Assert.False(ReferenceEquals(a, b));
    }

    [Fact(Timeout = 5000)]
    public void Equals_should_return_false_for_different_host()
    {
        var a = new TcpTransportOptions { Host = "example.com", Port = 80 };
        var b = new TcpTransportOptions { Host = "different.com", Port = 80 };

        Assert.False(a.Equals(b));
        Assert.False(a == b);
    }

    [Fact(Timeout = 5000)]
    public void Equals_should_handle_tls_options_with_same_host_port()
    {
        var a = new TlsTransportOptions { Host = "example.com", Port = 443 };
        var b = new TlsTransportOptions { Host = "example.com", Port = 443 };

        Assert.True(a.Equals(b));
        Assert.True(a == b);
    }

    [Fact(Timeout = 5000)]
    public void Equals_should_handle_quic_options_null_check()
    {
        var opts = new QuicTransportOptions { Host = "example.com", Port = 443 };

        Assert.NotNull(opts);
        Assert.False(opts.Equals(null));
    }
}
