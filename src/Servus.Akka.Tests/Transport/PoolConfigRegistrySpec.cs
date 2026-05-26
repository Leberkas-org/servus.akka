using Servus.Akka.Transport;

namespace Servus.Akka.Tests.Transport;

public sealed class PoolConfigRegistrySpec
{
    [Fact(Timeout = 5000)]
    public void Constructor_should_set_default_config()
    {
        var defaultConfig = new TcpPoolConfig(
            MaxConnectionsPerHost: 10,
            IdleTimeout: TimeSpan.FromSeconds(30),
            ConnectionLifetime: TimeSpan.FromMinutes(5),
            ReuseOnUpstreamFinish: true);

        var registry = new PoolConfigRegistry(defaultConfig);

        var resolved = registry.Resolve(null);
        Assert.Equal(defaultConfig, resolved);
    }

    [Fact(Timeout = 5000)]
    public void Resolve_should_return_default_when_key_is_null()
    {
        var defaultConfig = new TcpPoolConfig(
            MaxConnectionsPerHost: 5,
            IdleTimeout: TimeSpan.FromSeconds(60),
            ConnectionLifetime: TimeSpan.FromMinutes(10),
            ReuseOnUpstreamFinish: false);

        var registry = new PoolConfigRegistry(defaultConfig);

        var resolved = registry.Resolve(null);
        Assert.Equal(defaultConfig, resolved);
    }

    [Fact(Timeout = 5000)]
    public void Resolve_should_return_default_when_key_not_registered()
    {
        var defaultConfig = new TcpPoolConfig(
            MaxConnectionsPerHost: 8,
            IdleTimeout: TimeSpan.FromSeconds(45),
            ConnectionLifetime: TimeSpan.FromMinutes(3),
            ReuseOnUpstreamFinish: true);

        var registry = new PoolConfigRegistry(defaultConfig);

        var resolved = registry.Resolve("nonexistent-pool");
        Assert.Equal(defaultConfig, resolved);
    }

    [Fact(Timeout = 5000)]
    public void Register_should_store_config_for_key()
    {
        var defaultConfig = new TcpPoolConfig(
            MaxConnectionsPerHost: 10,
            IdleTimeout: TimeSpan.FromSeconds(30),
            ConnectionLifetime: TimeSpan.FromMinutes(5),
            ReuseOnUpstreamFinish: true);

        var customConfig = new TcpPoolConfig(
            MaxConnectionsPerHost: 20,
            IdleTimeout: TimeSpan.FromSeconds(15),
            ConnectionLifetime: TimeSpan.FromMinutes(2),
            ReuseOnUpstreamFinish: false);

        var registry = new PoolConfigRegistry(defaultConfig);
        registry.Register("custom-pool", customConfig);

        var resolved = registry.Resolve("custom-pool");
        Assert.Equal(customConfig, resolved);
    }

    [Fact(Timeout = 5000)]
    public void Resolve_should_return_registered_config()
    {
        var defaultConfig = new TcpPoolConfig(
            MaxConnectionsPerHost: 10,
            IdleTimeout: TimeSpan.FromSeconds(30),
            ConnectionLifetime: TimeSpan.FromMinutes(5),
            ReuseOnUpstreamFinish: true);

        var poolAConfig = new TcpPoolConfig(
            MaxConnectionsPerHost: 5,
            IdleTimeout: TimeSpan.FromSeconds(20),
            ConnectionLifetime: TimeSpan.FromMinutes(1),
            ReuseOnUpstreamFinish: false);

        var poolBConfig = new TcpPoolConfig(
            MaxConnectionsPerHost: 50,
            IdleTimeout: TimeSpan.FromSeconds(60),
            ConnectionLifetime: TimeSpan.FromMinutes(10),
            ReuseOnUpstreamFinish: true);

        var registry = new PoolConfigRegistry(defaultConfig);
        registry.Register("pool-a", poolAConfig);
        registry.Register("pool-b", poolBConfig);

        Assert.Equal(poolAConfig, registry.Resolve("pool-a"));
        Assert.Equal(poolBConfig, registry.Resolve("pool-b"));
        Assert.Equal(defaultConfig, registry.Resolve("pool-c"));
    }

    [Fact(Timeout = 5000)]
    public void Register_should_overwrite_existing_key()
    {
        var defaultConfig = new TcpPoolConfig(
            MaxConnectionsPerHost: 10,
            IdleTimeout: TimeSpan.FromSeconds(30),
            ConnectionLifetime: TimeSpan.FromMinutes(5),
            ReuseOnUpstreamFinish: true);

        var initialConfig = new TcpPoolConfig(
            MaxConnectionsPerHost: 15,
            IdleTimeout: TimeSpan.FromSeconds(25),
            ConnectionLifetime: TimeSpan.FromMinutes(3),
            ReuseOnUpstreamFinish: false);

        var overwriteConfig = new TcpPoolConfig(
            MaxConnectionsPerHost: 25,
            IdleTimeout: TimeSpan.FromSeconds(40),
            ConnectionLifetime: TimeSpan.FromMinutes(7),
            ReuseOnUpstreamFinish: true);

        var registry = new PoolConfigRegistry(defaultConfig);
        registry.Register("pool", initialConfig);

        var resolved1 = registry.Resolve("pool");
        Assert.Equal(initialConfig, resolved1);

        registry.Register("pool", overwriteConfig);

        var resolved2 = registry.Resolve("pool");
        Assert.Equal(overwriteConfig, resolved2);
    }

    [Fact(Timeout = 5000)]
    public void Register_should_throw_if_config_is_null()
    {
        var defaultConfig = new TcpPoolConfig(
            MaxConnectionsPerHost: 10,
            IdleTimeout: TimeSpan.FromSeconds(30),
            ConnectionLifetime: TimeSpan.FromMinutes(5),
            ReuseOnUpstreamFinish: true);

        var registry = new PoolConfigRegistry(defaultConfig);

        Assert.Throws<ArgumentNullException>(() => registry.Register("pool", null!));
    }

    [Fact(Timeout = 5000)]
    public void Constructor_should_throw_if_default_config_is_null()
    {
        Assert.Throws<ArgumentNullException>(() => new PoolConfigRegistry(null!));
    }

    [Fact(Timeout = 5000)]
    public void Register_should_support_case_insensitive_keys()
    {
        var defaultConfig = new TcpPoolConfig(
            MaxConnectionsPerHost: 10,
            IdleTimeout: TimeSpan.FromSeconds(30),
            ConnectionLifetime: TimeSpan.FromMinutes(5),
            ReuseOnUpstreamFinish: true);

        var customConfig = new TcpPoolConfig(
            MaxConnectionsPerHost: 20,
            IdleTimeout: TimeSpan.FromSeconds(15),
            ConnectionLifetime: TimeSpan.FromMinutes(2),
            ReuseOnUpstreamFinish: false);

        var registry = new PoolConfigRegistry(defaultConfig);
        registry.Register("MyPool", customConfig);

        var resolved1 = registry.Resolve("mypool");
        var resolved2 = registry.Resolve("MYPOOL");

        Assert.Equal(customConfig, resolved1);
        Assert.Equal(customConfig, resolved2);
    }

    [Fact(Timeout = 5000)]
    public void Register_should_return_self_for_fluent_chaining()
    {
        var defaultConfig = new TcpPoolConfig(
            MaxConnectionsPerHost: 10,
            IdleTimeout: TimeSpan.FromSeconds(30),
            ConnectionLifetime: TimeSpan.FromMinutes(5),
            ReuseOnUpstreamFinish: true);

        var config1 = new TcpPoolConfig(
            MaxConnectionsPerHost: 5,
            IdleTimeout: TimeSpan.FromSeconds(20),
            ConnectionLifetime: TimeSpan.FromMinutes(1),
            ReuseOnUpstreamFinish: false);

        var config2 = new TcpPoolConfig(
            MaxConnectionsPerHost: 15,
            IdleTimeout: TimeSpan.FromSeconds(40),
            ConnectionLifetime: TimeSpan.FromMinutes(4),
            ReuseOnUpstreamFinish: true);

        var registry = new PoolConfigRegistry(defaultConfig);
        var result = registry
            .Register("pool1", config1)
            .Register("pool2", config2);

        Assert.Same(registry, result);
        Assert.Equal(config1, registry.Resolve("pool1"));
        Assert.Equal(config2, registry.Resolve("pool2"));
    }
}
