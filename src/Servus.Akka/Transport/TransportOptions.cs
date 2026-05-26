namespace Servus.Akka.Transport;

public abstract record TransportOptions
{
    public required string Host { get; init; }
    public required ushort Port { get; init; }
    public string? PoolKey { get; init; }
    public TimeSpan ConnectTimeout { get; init; } = TimeSpan.FromSeconds(10);
    public int? SocketSendBufferSize { get; init; }
    public int? SocketReceiveBufferSize { get; init; }

    public virtual bool Equals(TransportOptions? other)
    {
        if (other is null)
        {
            return false;
        }

        if (ReferenceEquals(this, other))
        {
            return true;
        }

        return GetType() == other.GetType()
               && string.Equals(Host, other.Host, StringComparison.OrdinalIgnoreCase)
               && Port == other.Port;
    }

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(GetType());
        hash.Add(Host, StringComparer.OrdinalIgnoreCase);
        hash.Add(Port);
        return hash.ToHashCode();
    }
}
