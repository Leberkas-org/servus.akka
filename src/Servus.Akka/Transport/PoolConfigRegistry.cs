namespace Servus.Akka.Transport;

public sealed class PoolConfigRegistry(TcpPoolConfig defaultConfig)
{
    private readonly Dictionary<string, TcpPoolConfig> _configs = new(StringComparer.OrdinalIgnoreCase);
    private readonly TcpPoolConfig _default = defaultConfig ?? throw new ArgumentNullException(nameof(defaultConfig));

    public PoolConfigRegistry Register(string poolKey, TcpPoolConfig config)
    {
        _configs[poolKey] = config ?? throw new ArgumentNullException(nameof(config));
        return this;
    }

    public TcpPoolConfig Resolve(string? poolKey)
    {
        if (poolKey is not null && _configs.TryGetValue(poolKey, out var config))
        {
            return config;
        }

        return _default;
    }
}
