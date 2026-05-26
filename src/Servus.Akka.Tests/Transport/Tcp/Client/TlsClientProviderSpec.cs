using System.Net;
using System.Security.Authentication;
using System.Text;
using Servus.Akka.Tests.Utils;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Tcp.Client;

namespace Servus.Akka.Tests.Transport.Tcp.Client;

[Collection("ClientProvider")]
public sealed class TlsClientProviderSpec
{
    [Fact(Timeout = 5000)]
    public void TlsClientProvider_should_initialize_with_options()
    {
        var options = new TlsTransportOptions
        {
            Host = "example.com",
            Port = 443,
            EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13
        };

        var provider = new TlsClientProvider(options);

        Assert.NotNull(provider);
    }

    [Fact(Timeout = 5000)]
    public async Task TlsClientProvider_should_dispose_without_connection()
    {
        var options = new TlsTransportOptions
        {
            Host = "example.com",
            Port = 443
        };

        var provider = new TlsClientProvider(options);

        await provider.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task TlsClientProvider_should_handle_double_dispose()
    {
        var options = new TlsTransportOptions
        {
            Host = "example.com",
            Port = 443
        };

        var provider = new TlsClientProvider(options);

        await provider.DisposeAsync();
        await provider.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task ConnectTunnel_should_send_correct_request()
    {
        var targetHost = "example.com";
        var targetPort = 443;
        var proxyStream = new MockProxyStream("HTTP/1.1 200 Connection Established\r\n\r\n");

        await TlsClientProvider.EstablishConnectTunnelAsync(
            proxyStream,
            targetHost,
            targetPort,
            new TestProxy(new Uri("http://proxy.local:8080")),
            defaultProxyCredentials: null,
            CancellationToken.None
        );

        var requestContent = proxyStream.GetRequestContent();
        Assert.NotNull(requestContent);
        Assert.Contains($"CONNECT {targetHost}:{targetPort} HTTP/1.1", requestContent);
        Assert.Contains($"Host: {targetHost}:{targetPort}", requestContent);
    }

    [Fact(Timeout = 5000)]
    public async Task ConnectTunnel_should_succeed_on_200_response()
    {
        var proxyStream = new MockProxyStream("HTTP/1.1 200 Connection Established\r\n\r\n");

        await TlsClientProvider.EstablishConnectTunnelAsync(
            proxyStream,
            "example.com",
            443,
            new TestProxy(new Uri("http://proxy.local:8080")),
            defaultProxyCredentials: null,
            CancellationToken.None
        );
    }

    [Fact(Timeout = 5000)]
    public async Task ConnectTunnel_should_succeed_on_HTTP10_200()
    {
        var proxyStream = new MockProxyStream("HTTP/1.0 200 OK\r\n\r\n");

        await TlsClientProvider.EstablishConnectTunnelAsync(
            proxyStream,
            "example.com",
            443,
            new TestProxy(new Uri("http://proxy.local:8080")),
            defaultProxyCredentials: null,
            CancellationToken.None
        );
    }

    [Fact(Timeout = 5000)]
    public async Task ConnectTunnel_should_throw_on_407_response()
    {
        var proxyStream = new MockProxyStream("HTTP/1.1 407 Proxy Authentication Required\r\n\r\n");

        var ex = await Assert.ThrowsAsync<HttpRequestException>(() => TlsClientProvider.EstablishConnectTunnelAsync(
                proxyStream,
                "example.com",
                443,
                new TestProxy(new Uri("http://proxy.local:8080")),
                defaultProxyCredentials: null,
                CancellationToken.None
            )
        );

        Assert.Contains("407 Proxy Authentication Required", ex.Message);
    }

    [Fact(Timeout = 5000)]
    public async Task ConnectTunnel_should_throw_on_non_200()
    {
        var proxyStream = new MockProxyStream("HTTP/1.1 503 Service Unavailable\r\n\r\n");

        var ex = await Assert.ThrowsAsync<HttpRequestException>(() => TlsClientProvider.EstablishConnectTunnelAsync(
                proxyStream,
                "example.com",
                443,
                new TestProxy(new Uri("http://proxy.local:8080")),
                defaultProxyCredentials: null,
                CancellationToken.None
            )
        );

        Assert.Contains("503 Service Unavailable", ex.Message);
    }

    [Fact(Timeout = 5000)]
    public async Task ConnectTunnel_should_include_proxy_auth_header()
    {
        var credentials = new NetworkCredential("testuser", "testpass");
        var proxyStream = new MockProxyStream("HTTP/1.1 200 Connection Established\r\n\r\n");

        await TlsClientProvider.EstablishConnectTunnelAsync(
            proxyStream,
            "example.com",
            443,
            new TestProxy(new Uri("http://proxy.local:8080"), credentials: credentials),
            defaultProxyCredentials: null,
            CancellationToken.None
        );

        var requestContent = proxyStream.GetRequestContent();
        Assert.NotNull(requestContent);

        var expectedEncoded = Convert.ToBase64String(Encoding.UTF8.GetBytes("testuser:testpass"));
        Assert.Contains($"Proxy-Authorization: Basic {expectedEncoded}", requestContent);
    }

    [Fact(Timeout = 5000)]
    public async Task ConnectTunnel_should_not_include_auth_when_no_credentials()
    {
        var proxyStream = new MockProxyStream("HTTP/1.1 200 Connection Established\r\n\r\n");

        await TlsClientProvider.EstablishConnectTunnelAsync(
            proxyStream,
            "example.com",
            443,
            new TestProxy(new Uri("http://proxy.local:8080")),
            defaultProxyCredentials: null,
            CancellationToken.None
        );

        var requestContent = proxyStream.GetRequestContent();
        Assert.NotNull(requestContent);
        Assert.DoesNotContain("Proxy-Authorization", requestContent);
    }

    [Fact(Timeout = 5000)]
    public async Task ConnectTunnel_should_use_default_proxy_credentials()
    {
        var defaultCredentials = new NetworkCredential("defaultuser", "defaultpass");
        var proxyStream = new MockProxyStream("HTTP/1.1 200 Connection Established\r\n\r\n");

        await TlsClientProvider.EstablishConnectTunnelAsync(
            proxyStream,
            "example.com",
            443,
            new TestProxy(new Uri("http://proxy.local:8080")),
            defaultProxyCredentials: defaultCredentials,
            CancellationToken.None
        );

        var requestContent = proxyStream.GetRequestContent();
        Assert.NotNull(requestContent);

        var expectedEncoded = Convert.ToBase64String(Encoding.UTF8.GetBytes("defaultuser:defaultpass"));
        Assert.Contains($"Proxy-Authorization: Basic {expectedEncoded}", requestContent);
    }

    [Fact(Timeout = 5000)]
    public async Task ConnectTunnel_should_prefer_proxy_credentials_over_defaults()
    {
        var proxyCredentials = new NetworkCredential("proxyuser", "proxypass");
        var defaultCredentials = new NetworkCredential("defaultuser", "defaultpass");
        var proxyStream = new MockProxyStream("HTTP/1.1 200 Connection Established\r\n\r\n");

        await TlsClientProvider.EstablishConnectTunnelAsync(
            proxyStream,
            "example.com",
            443,
            new TestProxy(new Uri("http://proxy.local:8080"), credentials: proxyCredentials),
            defaultProxyCredentials: defaultCredentials,
            CancellationToken.None
        );

        var requestContent = proxyStream.GetRequestContent();
        Assert.NotNull(requestContent);

        var proxyEncoded = Convert.ToBase64String(Encoding.UTF8.GetBytes("proxyuser:proxypass"));
        var defaultEncoded = Convert.ToBase64String(Encoding.UTF8.GetBytes("defaultuser:defaultpass"));

        Assert.Contains($"Proxy-Authorization: Basic {proxyEncoded}", requestContent);
        Assert.DoesNotContain($"Proxy-Authorization: Basic {defaultEncoded}", requestContent);
    }

    [Fact(Timeout = 5000)]
    public async Task ConnectTunnel_should_throw_on_empty_response()
    {
        var proxyStream = new MockProxyStream("");

        var ex = await Assert.ThrowsAsync<HttpRequestException>(() => TlsClientProvider.EstablishConnectTunnelAsync(
                proxyStream,
                "example.com",
                443,
                new TestProxy(new Uri("http://proxy.local:8080")),
                defaultProxyCredentials: null,
                CancellationToken.None
            )
        );

        Assert.Contains("Proxy closed connection", ex.Message);
    }

    [Fact(Timeout = 5000)]
    public async Task ConnectTunnel_should_handle_large_response_buffer()
    {
        var largeHeaders = string.Concat(Enumerable.Range(0, 10).Select(i => $"X-Custom-Header-{i}: value-{i}\r\n"));
        var response = $"HTTP/1.1 200 Connection Established\r\n{largeHeaders}\r\n";
        var proxyStream = new MockProxyStream(response);

        await TlsClientProvider.EstablishConnectTunnelAsync(
            proxyStream,
            "example.com",
            443,
            new TestProxy(new Uri("http://proxy.local:8080")),
            defaultProxyCredentials: null,
            CancellationToken.None
        );
    }

    [Fact(Timeout = 5000)]
    public async Task ConnectTunnel_should_respect_cancellation_token()
    {
        var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var proxyStream = new MockProxyStream("HTTP/1.1 200 Connection Established\r\n\r\n");

        await Assert.ThrowsAsync<OperationCanceledException>(() => TlsClientProvider.EstablishConnectTunnelAsync(
                proxyStream,
                "example.com",
                443,
                new TestProxy(new Uri("http://proxy.local:8080")),
                defaultProxyCredentials: null,
                cts.Token
            )
        );
    }

    [Fact(Timeout = 5000)]
    public async Task GetStreamAsync_should_throw_on_connection_refused()
    {
        var options = new TlsTransportOptions
        {
            Host = "localhost",
            Port = (ushort)1,
            ConnectTimeout = TimeSpan.FromSeconds(2),
            ServerCertificateValidationCallback = (_, _, _, _) => true
        };

        var provider = new TlsClientProvider(options);

        await Assert.ThrowsAnyAsync<Exception>(() =>
            provider.GetStreamAsync(TestContext.Current.CancellationToken));

        await provider.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task ConnectTunnel_should_throw_on_exceeding_buffer_size()
    {
        var largeResponse = "HTTP/1.1 200 OK\r\n" + string.Concat(Enumerable.Range(0, 1000).Select(_ => "X-Large-Header: value\r\n"));
        var proxyStream = new MockProxyStream(largeResponse);

        var ex = await Assert.ThrowsAsync<HttpRequestException>(() => TlsClientProvider.EstablishConnectTunnelAsync(
                proxyStream,
                "example.com",
                443,
                new TestProxy(new Uri("http://proxy.local:8080")),
                defaultProxyCredentials: null,
                CancellationToken.None
            )
        );

        Assert.Contains("exceeded buffer size", ex.Message);
    }

    [Fact(Timeout = 5000)]
    public async Task ConnectTunnel_should_handle_chunked_response_reads()
    {
        var proxyStream = new ChunkedMockProxyStream("HTTP/1.1 200 OK\r\n\r\n", chunkSize: 5);

        await TlsClientProvider.EstablishConnectTunnelAsync(
            proxyStream,
            "example.com",
            443,
            new TestProxy(new Uri("http://proxy.local:8080")),
            defaultProxyCredentials: null,
            CancellationToken.None
        );
    }

    [Fact(Timeout = 5000)]
    public async Task ConnectTunnel_should_work_with_proxy_returning_null()
    {
        var proxyStream = new MockProxyStream("HTTP/1.1 200 OK\r\n\r\n");
        var bypassedProxy = new TestProxy(null);

        await TlsClientProvider.EstablishConnectTunnelAsync(
            proxyStream,
            "example.com",
            443,
            bypassedProxy,
            defaultProxyCredentials: null,
            CancellationToken.None
        );

        var requestContent = proxyStream.GetRequestContent();
        Assert.NotNull(requestContent);
        Assert.Contains("CONNECT", requestContent);
    }

    [Fact(Timeout = 5000)]
    public async Task ConnectTunnel_should_handle_status_codes_without_reason()
    {
        var proxyStream = new MockProxyStream("HTTP/1.1 500\r\n\r\n");

        var ex = await Assert.ThrowsAsync<HttpRequestException>(() => TlsClientProvider.EstablishConnectTunnelAsync(
                proxyStream,
                "example.com",
                443,
                new TestProxy(new Uri("http://proxy.local:8080")),
                defaultProxyCredentials: null,
                CancellationToken.None
            )
        );

        Assert.Contains("500", ex.Message);
    }

    [Fact(Timeout = 5000)]
    public async Task ConnectTunnel_should_handle_response_with_headers()
    {
        var response = "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 0\r\n\r\n";
        var proxyStream = new MockProxyStream(response);

        await TlsClientProvider.EstablishConnectTunnelAsync(
            proxyStream,
            "example.com",
            443,
            new TestProxy(new Uri("http://proxy.local:8080")),
            defaultProxyCredentials: null,
            CancellationToken.None
        );
    }

    [Fact(Timeout = 5000)]
    public async Task ConnectTunnel_should_ignore_response_body()
    {
        var response = "HTTP/1.1 200 OK\r\n\r\nExtra data in response body that should be ignored";
        var proxyStream = new MockProxyStream(response);

        await TlsClientProvider.EstablishConnectTunnelAsync(
            proxyStream,
            "example.com",
            443,
            new TestProxy(new Uri("http://proxy.local:8080")),
            defaultProxyCredentials: null,
            CancellationToken.None
        );
    }

    [Fact(Timeout = 5000)]
    public async Task ConnectTunnel_should_handle_404_response()
    {
        var proxyStream = new MockProxyStream("HTTP/1.1 404 Not Found\r\n\r\n");

        var ex = await Assert.ThrowsAsync<HttpRequestException>(() => TlsClientProvider.EstablishConnectTunnelAsync(
                proxyStream,
                "example.com",
                443,
                new TestProxy(new Uri("http://proxy.local:8080")),
                defaultProxyCredentials: null,
                CancellationToken.None
            )
        );

        Assert.Contains("404 Not Found", ex.Message);
    }

    [Fact(Timeout = 5000)]
    public async Task ConnectTunnel_should_format_credentials_correctly()
    {
        var credentials = new NetworkCredential("user@domain", "pass:word!");
        var proxyStream = new MockProxyStream("HTTP/1.1 200 OK\r\n\r\n");

        await TlsClientProvider.EstablishConnectTunnelAsync(
            proxyStream,
            "example.com",
            443,
            new TestProxy(new Uri("http://proxy.local:8080"), credentials: credentials),
            defaultProxyCredentials: null,
            CancellationToken.None
        );

        var requestContent = proxyStream.GetRequestContent();
        var expectedEncoded = Convert.ToBase64String(Encoding.UTF8.GetBytes("user@domain:pass:word!"));
        Assert.Contains($"Proxy-Authorization: Basic {expectedEncoded}", requestContent);
    }
}