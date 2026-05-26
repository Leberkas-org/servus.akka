using System.Net;
using Servus.Akka.Transport.Tcp.Client;

namespace Servus.Akka.Tests.Transport.Tcp.Client;

[Collection("DnsCache")]
public sealed class DnsCacheSpec : IDisposable
{
    public DnsCacheSpec()
    {
        DnsCache.Clear();
    }

    public void Dispose()
    {
        DnsCache.Clear();
        DnsCache.Ttl = TimeSpan.FromSeconds(120);
    }

    [Fact(Timeout = 5000)]
    public async Task ResolveAsync_should_return_literal_ip_without_dns_lookup()
    {
        var addresses = await DnsCache.ResolveAsync("127.0.0.1", CancellationToken.None);

        Assert.Single(addresses);
        Assert.Equal(IPAddress.Loopback, addresses[0]);
    }

    [Fact(Timeout = 5000)]
    public async Task ResolveAsync_should_return_ipv6_literal()
    {
        var addresses = await DnsCache.ResolveAsync("::1", CancellationToken.None);

        Assert.Single(addresses);
        Assert.Equal(IPAddress.IPv6Loopback, addresses[0]);
    }

    [Fact(Timeout = 10000)]
    public async Task ResolveAsync_should_resolve_localhost()
    {
        var addresses = await DnsCache.ResolveAsync("localhost", CancellationToken.None);

        Assert.NotEmpty(addresses);
    }

    [Fact(Timeout = 10000)]
    public async Task ResolveAsync_should_cache_results()
    {
        var first = await DnsCache.ResolveAsync("localhost", CancellationToken.None);
        var second = await DnsCache.ResolveAsync("localhost", CancellationToken.None);

        Assert.Same(first, second);
    }

    [Fact(Timeout = 10000)]
    public async Task ResolveAsync_should_expire_after_ttl()
    {
        DnsCache.Ttl = TimeSpan.FromMilliseconds(1);

        var first = await DnsCache.ResolveAsync("localhost", CancellationToken.None);
        await Task.Delay(100, TestContext.Current.CancellationToken);
        var second = await DnsCache.ResolveAsync("localhost", CancellationToken.None);

        Assert.NotSame(first, second);
    }

    [Fact(Timeout = 5000)]
    public async Task Clear_should_remove_all_entries()
    {
        await DnsCache.ResolveAsync("127.0.0.1", CancellationToken.None);
        DnsCache.Clear();

        var addresses = await DnsCache.ResolveAsync("127.0.0.1", CancellationToken.None);
        Assert.NotNull(addresses);
    }
}