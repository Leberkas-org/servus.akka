namespace Servus.Akka.Transport;

public sealed record TcpPoolConfig(
    int MaxConnectionsPerHost,
    TimeSpan IdleTimeout,
    TimeSpan ConnectionLifetime,
    bool ReuseOnUpstreamFinish);
