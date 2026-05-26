using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;

namespace Servus.Akka.Transport;

public sealed record QuicTransportOptions : TransportOptions
{
    public string? TargetHost { get; init; }
    public TimeSpan IdleTimeout { get; init; } = TimeSpan.FromSeconds(30);
    public int MaxBidirectionalStreams { get; init; } = 100;
    public int MaxUnidirectionalStreams { get; init; } = 3;
    public X509CertificateCollection? ClientCertificates { get; init; }
    public RemoteCertificateValidationCallback? ServerCertificateValidationCallback { get; init; }
    public SslProtocols EnabledSslProtocols { get; init; } = SslProtocols.None;
    public List<SslApplicationProtocol>? ApplicationProtocols { get; init; }
    public bool AutoReconnect { get; init; }
    public int MaxConnectionsPerHost { get; init; } = 1;
    public TimeSpan ConnectionLifetime { get; init; } = TimeSpan.FromMinutes(10);
}