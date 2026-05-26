using System.Net.Security;

namespace Servus.Akka.Transport;

internal sealed record TlsConnectionResult(
    Stream Stream,
    SecurityInfo? Security,
    SslStream? SslStream,
    bool AllowDelayedNegotiation);
