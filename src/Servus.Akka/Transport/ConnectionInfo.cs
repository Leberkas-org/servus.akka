using System.Net;

namespace Servus.Akka.Transport;

public sealed record ConnectionInfo(
    EndPoint Local,
    EndPoint Remote,
    TransportProtocol Protocol,
    SecurityInfo? Security = null)
{
    public static readonly ConnectionInfo None = new(
        new IPEndPoint(IPAddress.None, 0),
        new IPEndPoint(IPAddress.None, 0),
        TransportProtocol.None);
}
