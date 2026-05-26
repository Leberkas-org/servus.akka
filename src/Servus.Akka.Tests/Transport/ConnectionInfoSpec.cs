using System.Net;
using System.Net.Security;
using System.Security.Authentication;
using Servus.Akka.Transport;

namespace Servus.Akka.Tests.Transport;

public sealed class ConnectionInfoSpec
{
    [Fact(Timeout = 5000)]
    public void Should_store_endpoints_and_protocol()
    {
        var local = new IPEndPoint(IPAddress.Loopback, 5000);
        var remote = new IPEndPoint(IPAddress.Parse("192.168.1.1"), 443);

        var info = new ConnectionInfo(local, remote, TransportProtocol.Tcp);

        Assert.Equal(local, info.Local);
        Assert.Equal(remote, info.Remote);
        Assert.Equal(TransportProtocol.Tcp, info.Protocol);
        Assert.Null(info.Security);
    }

    [Fact(Timeout = 5000)]
    public void Should_store_security_info_when_provided()
    {
        var local = new IPEndPoint(IPAddress.Loopback, 5000);
        var remote = new IPEndPoint(IPAddress.Parse("192.168.1.1"), 443);
        var security = new SecurityInfo(SslProtocols.Tls13, SslApplicationProtocol.Http2);

        var info = new ConnectionInfo(local, remote, TransportProtocol.Tls, security);

        Assert.Equal(TransportProtocol.Tls, info.Protocol);
        Assert.NotNull(info.Security);
        Assert.Equal(SslProtocols.Tls13, info.Security.Protocol);
        Assert.Equal(SslApplicationProtocol.Http2, info.Security.ApplicationProtocol);
    }

    [Fact(Timeout = 5000)]
    public void Equality_should_work_for_records()
    {
        var local = new IPEndPoint(IPAddress.Loopback, 5000);
        var remote = new IPEndPoint(IPAddress.Parse("192.168.1.1"), 443);
        var security = new SecurityInfo(SslProtocols.Tls13, SslApplicationProtocol.Http2);

        var info1 = new ConnectionInfo(local, remote, TransportProtocol.Tls, security);
        var info2 = new ConnectionInfo(local, remote, TransportProtocol.Tls, security);

        Assert.Equal(info1, info2);
        Assert.Equal(info1.GetHashCode(), info2.GetHashCode());
    }

    [Fact(Timeout = 5000)]
    public void Inequality_should_work_for_different_local_endpoint()
    {
        var local1 = new IPEndPoint(IPAddress.Loopback, 5000);
        var local2 = new IPEndPoint(IPAddress.Loopback, 5001);
        var remote = new IPEndPoint(IPAddress.Parse("192.168.1.1"), 443);

        var info1 = new ConnectionInfo(local1, remote, TransportProtocol.Tcp);
        var info2 = new ConnectionInfo(local2, remote, TransportProtocol.Tcp);

        Assert.NotEqual(info1, info2);
    }

    [Fact(Timeout = 5000)]
    public void Inequality_should_work_for_different_remote_endpoint()
    {
        var local = new IPEndPoint(IPAddress.Loopback, 5000);
        var remote1 = new IPEndPoint(IPAddress.Parse("192.168.1.1"), 443);
        var remote2 = new IPEndPoint(IPAddress.Parse("192.168.1.2"), 443);

        var info1 = new ConnectionInfo(local, remote1, TransportProtocol.Tcp);
        var info2 = new ConnectionInfo(local, remote2, TransportProtocol.Tcp);

        Assert.NotEqual(info1, info2);
    }

    [Fact(Timeout = 5000)]
    public void Inequality_should_work_for_different_protocol()
    {
        var local = new IPEndPoint(IPAddress.Loopback, 5000);
        var remote = new IPEndPoint(IPAddress.Parse("192.168.1.1"), 443);

        var info1 = new ConnectionInfo(local, remote, TransportProtocol.Tcp);
        var info2 = new ConnectionInfo(local, remote, TransportProtocol.Quic);

        Assert.NotEqual(info1, info2);
    }

    [Fact(Timeout = 5000)]
    public void Inequality_should_work_for_different_security()
    {
        var local = new IPEndPoint(IPAddress.Loopback, 5000);
        var remote = new IPEndPoint(IPAddress.Parse("192.168.1.1"), 443);

        var info1 = new ConnectionInfo(local, remote, TransportProtocol.Tls,
            new SecurityInfo(SslProtocols.Tls13, SslApplicationProtocol.Http2));
        var info2 = new ConnectionInfo(local, remote, TransportProtocol.Tls,
            new SecurityInfo(SslProtocols.Tls12, SslApplicationProtocol.Http2));

        Assert.NotEqual(info1, info2);
    }

    [Fact(Timeout = 5000)]
    public void None_should_have_sensible_defaults()
    {
        var none = ConnectionInfo.None;

        Assert.Equal(TransportProtocol.None, none.Protocol);
        Assert.Null(none.Security);
    }

    [Fact(Timeout = 5000)]
    public void Should_work_as_dictionary_key()
    {
        var local = new IPEndPoint(IPAddress.Loopback, 5000);
        var remote = new IPEndPoint(IPAddress.Parse("192.168.1.1"), 443);

        var info1 = new ConnectionInfo(local, remote, TransportProtocol.Tls,
            new SecurityInfo(SslProtocols.Tls13, SslApplicationProtocol.Http2));
        var info2 = new ConnectionInfo(local, remote, TransportProtocol.Tls,
            new SecurityInfo(SslProtocols.Tls13, SslApplicationProtocol.Http2));

        var dict = new Dictionary<ConnectionInfo, string> { { info1, "pooled" } };

        Assert.True(dict.ContainsKey(info2));
        Assert.Equal("pooled", dict[info2]);
    }
}
