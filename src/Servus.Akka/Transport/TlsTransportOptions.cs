using System.Net;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;

namespace Servus.Akka.Transport;

public sealed record TlsTransportOptions : TransportOptions
{
    public string? TargetHost { get; init; }
    public bool UseProxy { get; init; }
    public IWebProxy? Proxy { get; init; }
    public ICredentials? DefaultProxyCredentials { get; init; }
    public X509CertificateCollection? ClientCertificates { get; init; }
    public RemoteCertificateValidationCallback? ServerCertificateValidationCallback { get; init; }
    public SslProtocols EnabledSslProtocols { get; init; } = SslProtocols.None;
    public List<SslApplicationProtocol>? ApplicationProtocols { get; init; }
}
