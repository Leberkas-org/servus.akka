using Servus.Akka.Transport;

namespace Servus.Akka.Tests.Transport;

public sealed class TcpPoolConfigSpec
{
    [Fact(Timeout = 5000)]
    public void Should_store_all_properties()
    {
        var config = new TcpPoolConfig(
            MaxConnectionsPerHost: 10,
            IdleTimeout: TimeSpan.FromSeconds(30),
            ConnectionLifetime: TimeSpan.FromMinutes(5),
            ReuseOnUpstreamFinish: true);

        Assert.Equal(10, config.MaxConnectionsPerHost);
        Assert.Equal(TimeSpan.FromSeconds(30), config.IdleTimeout);
        Assert.Equal(TimeSpan.FromMinutes(5), config.ConnectionLifetime);
        Assert.True(config.ReuseOnUpstreamFinish);
    }

    [Fact(Timeout = 5000)]
    public void Default_values_should_be_reasonable()
    {
        var config = new TcpPoolConfig(
            MaxConnectionsPerHost: 5,
            IdleTimeout: TimeSpan.FromSeconds(60),
            ConnectionLifetime: TimeSpan.FromMinutes(10),
            ReuseOnUpstreamFinish: false);

        Assert.True(config.MaxConnectionsPerHost > 0);
        Assert.True(config.IdleTimeout > TimeSpan.Zero);
        Assert.True(config.ConnectionLifetime > TimeSpan.Zero);
    }

    [Fact(Timeout = 5000)]
    public void Equality_should_work()
    {
        var config1 = new TcpPoolConfig(
            MaxConnectionsPerHost: 10,
            IdleTimeout: TimeSpan.FromSeconds(30),
            ConnectionLifetime: TimeSpan.FromMinutes(5),
            ReuseOnUpstreamFinish: true);

        var config2 = new TcpPoolConfig(
            MaxConnectionsPerHost: 10,
            IdleTimeout: TimeSpan.FromSeconds(30),
            ConnectionLifetime: TimeSpan.FromMinutes(5),
            ReuseOnUpstreamFinish: true);

        Assert.Equal(config1, config2);
        Assert.Equal(config1.GetHashCode(), config2.GetHashCode());
    }

    [Fact(Timeout = 5000)]
    public void Inequality_should_work_for_different_max_connections()
    {
        var config1 = new TcpPoolConfig(
            MaxConnectionsPerHost: 10,
            IdleTimeout: TimeSpan.FromSeconds(30),
            ConnectionLifetime: TimeSpan.FromMinutes(5),
            ReuseOnUpstreamFinish: true);

        var config2 = new TcpPoolConfig(
            MaxConnectionsPerHost: 20,
            IdleTimeout: TimeSpan.FromSeconds(30),
            ConnectionLifetime: TimeSpan.FromMinutes(5),
            ReuseOnUpstreamFinish: true);

        Assert.NotEqual(config1, config2);
    }

    [Fact(Timeout = 5000)]
    public void Inequality_should_work_for_different_idle_timeout()
    {
        var config1 = new TcpPoolConfig(
            MaxConnectionsPerHost: 10,
            IdleTimeout: TimeSpan.FromSeconds(30),
            ConnectionLifetime: TimeSpan.FromMinutes(5),
            ReuseOnUpstreamFinish: true);

        var config2 = new TcpPoolConfig(
            MaxConnectionsPerHost: 10,
            IdleTimeout: TimeSpan.FromSeconds(60),
            ConnectionLifetime: TimeSpan.FromMinutes(5),
            ReuseOnUpstreamFinish: true);

        Assert.NotEqual(config1, config2);
    }

    [Fact(Timeout = 5000)]
    public void Inequality_should_work_for_different_connection_lifetime()
    {
        var config1 = new TcpPoolConfig(
            MaxConnectionsPerHost: 10,
            IdleTimeout: TimeSpan.FromSeconds(30),
            ConnectionLifetime: TimeSpan.FromMinutes(5),
            ReuseOnUpstreamFinish: true);

        var config2 = new TcpPoolConfig(
            MaxConnectionsPerHost: 10,
            IdleTimeout: TimeSpan.FromSeconds(30),
            ConnectionLifetime: TimeSpan.FromMinutes(10),
            ReuseOnUpstreamFinish: true);

        Assert.NotEqual(config1, config2);
    }

    [Fact(Timeout = 5000)]
    public void Inequality_should_work_for_different_reuse_flag()
    {
        var config1 = new TcpPoolConfig(
            MaxConnectionsPerHost: 10,
            IdleTimeout: TimeSpan.FromSeconds(30),
            ConnectionLifetime: TimeSpan.FromMinutes(5),
            ReuseOnUpstreamFinish: true);

        var config2 = new TcpPoolConfig(
            MaxConnectionsPerHost: 10,
            IdleTimeout: TimeSpan.FromSeconds(30),
            ConnectionLifetime: TimeSpan.FromMinutes(5),
            ReuseOnUpstreamFinish: false);

        Assert.NotEqual(config1, config2);
    }

    [Fact(Timeout = 5000)]
    public void Should_work_as_dictionary_key()
    {
        var config1 = new TcpPoolConfig(
            MaxConnectionsPerHost: 10,
            IdleTimeout: TimeSpan.FromSeconds(30),
            ConnectionLifetime: TimeSpan.FromMinutes(5),
            ReuseOnUpstreamFinish: true);

        var config2 = new TcpPoolConfig(
            MaxConnectionsPerHost: 10,
            IdleTimeout: TimeSpan.FromSeconds(30),
            ConnectionLifetime: TimeSpan.FromMinutes(5),
            ReuseOnUpstreamFinish: true);

        var dict = new Dictionary<TcpPoolConfig, string> { { config1, "pooled" } };

        Assert.True(dict.ContainsKey(config2));
        Assert.Equal("pooled", dict[config2]);
    }

    [Fact(Timeout = 5000)]
    public void Should_support_zero_or_negative_infinite_timespan_for_lifetime()
    {
        var config1 = new TcpPoolConfig(
            MaxConnectionsPerHost: 10,
            IdleTimeout: TimeSpan.Zero,
            ConnectionLifetime: Timeout.InfiniteTimeSpan,
            ReuseOnUpstreamFinish: false);

        Assert.Equal(TimeSpan.Zero, config1.IdleTimeout);
        Assert.Equal(Timeout.InfiniteTimeSpan, config1.ConnectionLifetime);
    }
}
