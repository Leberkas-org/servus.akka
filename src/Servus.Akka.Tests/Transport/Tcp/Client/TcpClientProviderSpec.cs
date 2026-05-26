using System.Net;
using System.Net.Sockets;
using Servus.Akka.Tests.Utils;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Tcp.Client;

namespace Servus.Akka.Tests.Transport.Tcp.Client;

[CollectionDefinition("ClientProvider", DisableParallelization = true)]
public class ClientProviderCollection;

[Collection("ClientProvider")]
public sealed class TcpClientProviderSpec
{
    [Fact(Timeout = 5000)]
    public void TcpClientProvider_should_initialize_with_options()
    {
        var options = new TcpTransportOptions
        {
            Host = "localhost",
            Port = 8080
        };

        var provider = new TcpClientProvider(options);

        Assert.Null(provider.RemoteEndPoint);
    }

    [Fact(Timeout = 5000)]
    public async Task TcpClientProvider_should_dispose_without_socket()
    {
        var options = new TcpTransportOptions { Host = "localhost", Port = 8080 };
        var provider = new TcpClientProvider(options);

        await provider.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task TcpClientProvider_should_complete_disposal_on_double_dispose()
    {
        var options = new TcpTransportOptions { Host = "localhost", Port = 8080 };
        var provider = new TcpClientProvider(options);

        await provider.DisposeAsync();
        await provider.DisposeAsync();
    }

    [Fact(Timeout = 10_000)]
    public async Task TcpClientProvider_should_resolve_proxy_when_configured()
    {
        var proxyUri = new Uri("http://proxy.local:8080");
        var proxy = new TestProxy(proxyUri);

        var options = new TcpTransportOptions
        {
            Host = "example.com",
            Port = 443,
            UseProxy = true,
            Proxy = proxy
        };

        var provider = new TcpClientProvider(options);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await provider.GetStreamAsync(cts.Token));

        await provider.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task TcpClientProvider_should_bypass_proxy_when_bypassed()
    {
        var proxy = new TestProxy(null, bypassedHost: "example.com");

        var options = new TcpTransportOptions
        {
            Host = "localhost",
            Port = 1,
            UseProxy = true,
            Proxy = proxy
        };

        var provider = new TcpClientProvider(options);

        await Assert.ThrowsAsync<SocketException>(async () =>
            await provider.GetStreamAsync(CancellationToken.None));

        await provider.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task TcpClientProvider_should_not_use_proxy_when_disabled()
    {
        var proxy = new TestProxy(new Uri("http://proxy.local:8080"));

        var options = new TcpTransportOptions
        {
            Host = "localhost",
            Port = 1,
            UseProxy = false,
            Proxy = proxy
        };

        var provider = new TcpClientProvider(options);

        await Assert.ThrowsAsync<SocketException>(async () =>
            await provider.GetStreamAsync(CancellationToken.None));

        await provider.DisposeAsync();
    }

    [Fact(Timeout = 10_000)]
    public async Task TcpClientProvider_should_apply_default_proxy_credentials()
    {
        var credentials = new NetworkCredential("user", "pass");
        var proxy = new TestProxy(new Uri("http://proxy.local:8080"));

        var options = new TcpTransportOptions
        {
            Host = "example.com",
            Port = 443,
            UseProxy = true,
            Proxy = proxy,
            DefaultProxyCredentials = credentials
        };

        var provider = new TcpClientProvider(options);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await provider.GetStreamAsync(cts.Token));

        Assert.NotNull(proxy.Credentials);
        await provider.DisposeAsync();
    }

    [Fact(Timeout = 10_000)]
    public async Task TcpClientProvider_should_not_override_existing_proxy_credentials()
    {
        var existingCredentials = new NetworkCredential("existing", "existing");
        var defaultCredentials = new NetworkCredential("default", "default");
        var proxy = new TestProxy(new Uri("http://proxy.local:8080"), credentials: existingCredentials);

        var options = new TcpTransportOptions
        {
            Host = "example.com",
            Port = 443,
            UseProxy = true,
            Proxy = proxy,
            DefaultProxyCredentials = defaultCredentials
        };

        var provider = new TcpClientProvider(options);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await provider.GetStreamAsync(cts.Token));

        Assert.Equal("existing", ((NetworkCredential)proxy.Credentials!).UserName);
        await provider.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task TcpClientProvider_should_set_socket_options()
    {
        var options = new TcpTransportOptions
        {
            Host = "localhost",
            Port = 1,
            SocketSendBufferSize = 65536,
            SocketReceiveBufferSize = 65536
        };

        var provider = new TcpClientProvider(options);

        await Assert.ThrowsAsync<SocketException>(async () =>
            await provider.GetStreamAsync(CancellationToken.None));

        await provider.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task TcpClientProvider_should_handle_null_buffer_sizes()
    {
        var options = new TcpTransportOptions
        {
            Host = "localhost",
            Port = 1,
            SocketSendBufferSize = null,
            SocketReceiveBufferSize = null
        };

        var provider = new TcpClientProvider(options);

        await Assert.ThrowsAsync<SocketException>(async () =>
            await provider.GetStreamAsync(CancellationToken.None));

        await provider.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task TcpClientProvider_should_throw_OperationCanceledException_on_timeout()
    {
        var options = new TcpTransportOptions
        {
            Host = "192.0.2.1",
            Port = 443
        };

        var provider = new TcpClientProvider(options);
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await provider.GetStreamAsync(cts.Token));

        await provider.DisposeAsync();
    }

    [Fact(Timeout = 10_000)]
    public async Task GetStreamAsync_should_throw_socket_exception_for_unreachable_host()
    {
        var options = new TcpTransportOptions
        {
            Host = "invalid-host-that-does-not-exist-12345.local",
            Port = 80
        };

        var provider = new TcpClientProvider(options);

        await Assert.ThrowsAsync<SocketException>(async () =>
        {
            await provider.GetStreamAsync(CancellationToken.None);
        });

        await provider.DisposeAsync();
    }

    [Fact(Timeout = 10_000)]
    public async Task GetStreamAsync_should_respect_cancellation_token()
    {
        var options = new TcpTransportOptions
        {
            Host = "192.0.2.1",
            Port = 443
        };

        var provider = new TcpClientProvider(options);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var exception =
            await Assert.ThrowsAnyAsync<Exception>(async () => { await provider.GetStreamAsync(cts.Token); });

        Assert.True(
            exception is OperationCanceledException,
            $"Expected OperationCanceledException or derived type, got {exception.GetType().Name}"
        );

        await provider.DisposeAsync();
    }

    [Fact(Timeout = 5000)]
    public async Task RemoteEndPoint_should_be_null_before_connect()
    {
        var options = new TcpTransportOptions
        {
            Host = "localhost",
            Port = 8080
        };

        var provider = new TcpClientProvider(options);

        Assert.Null(provider.RemoteEndPoint);

        await provider.DisposeAsync();
    }

    [Fact(Timeout = 10_000)]
    public async Task Disposal_should_be_safe_after_failed_connect()
    {
        var options = new TcpTransportOptions
        {
            Host = "invalid-host-that-does-not-exist-xyz.local",
            Port = 443
        };

        var provider = new TcpClientProvider(options);

        await Assert.ThrowsAsync<SocketException>(async () =>
            await provider.GetStreamAsync(CancellationToken.None));

        await provider.DisposeAsync();
    }

    [Fact(Timeout = 30_000)]
    public async Task GetStreamAsync_with_custom_buffer_sizes_should_not_throw_on_configuration()
    {
        var options = new TcpTransportOptions
        {
            Host = "invalid-host-that-does-not-exist-abc.local",
            Port = 443,
            SocketSendBufferSize = 131072,
            SocketReceiveBufferSize = 131072
        };

        var provider = new TcpClientProvider(options);

        var exception = await Assert.ThrowsAsync<SocketException>(async () =>
        {
            await provider.GetStreamAsync(CancellationToken.None);
        });

        Assert.NotNull(exception);

        await provider.DisposeAsync();
    }

    [Fact(Timeout = 30_000)]
    public async Task GetStreamAsync_with_zero_buffer_sizes_should_not_throw_on_configuration()
    {
        var options = new TcpTransportOptions
        {
            Host = "invalid-host-that-does-not-exist-def.local",
            Port = 443,
            SocketSendBufferSize = 0,
            SocketReceiveBufferSize = 0
        };

        var provider = new TcpClientProvider(options);

        var exception = await Assert.ThrowsAsync<SocketException>(async () =>
        {
            await provider.GetStreamAsync(CancellationToken.None);
        });

        Assert.NotNull(exception);

        await provider.DisposeAsync();
    }
}