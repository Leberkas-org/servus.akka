using System.Net.Security;
using System.Security.Authentication;

namespace Servus.Akka.Transport;

public sealed record SecurityInfo(
    SslProtocols Protocol,
    SslApplicationProtocol ApplicationProtocol,
    TlsCipherSuite? NegotiatedCipherSuite = null,
    string? HostName = null,
    SslStream? SslStream = null,
    bool AllowDelayedNegotiation = false);
