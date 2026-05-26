using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;

namespace Servus.Akka.Transport;

public sealed record QuicListenerOptions : ListenerOptions
{
    public int MaxInboundBidirectionalStreams { get; init; } = 100;
    public int MaxInboundUnidirectionalStreams { get; init; } = 3;
    public TimeSpan IdleTimeout { get; init; } = TimeSpan.FromSeconds(30);
    public required X509Certificate2 ServerCertificate { get; init; }
    public required List<SslApplicationProtocol> ApplicationProtocols { get; init; }
    public SslProtocols EnabledSslProtocols { get; init; } = SslProtocols.None;
    public RemoteCertificateValidationCallback? ClientCertificateValidationCallback { get; init; }
}
