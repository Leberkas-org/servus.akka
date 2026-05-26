using System.Net;

namespace Servus.Akka.Transport;

public sealed record TcpTransportOptions : TransportOptions
{
    public bool UseProxy { get; init; }
    public IWebProxy? Proxy { get; init; }
    public ICredentials? DefaultProxyCredentials { get; init; }
    public bool AutoReconnect { get; init; }
}
