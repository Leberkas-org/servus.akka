using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;

namespace Servus.Akka.Transport;

public sealed record TcpListenerOptions : ListenerOptions
{
    public bool ReuseAddress { get; init; } = true;
    public bool NoDelay { get; init; } = true;
    public X509Certificate2? ServerCertificate { get; init; }
    public SslProtocols EnabledSslProtocols { get; init; } = SslProtocols.None;
    public List<SslApplicationProtocol>? ApplicationProtocols { get; init; }
    public RemoteCertificateValidationCallback? ClientCertificateValidationCallback { get; init; }
    public TimeSpan HandshakeTimeout { get; init; } = TimeSpan.FromSeconds(10);
    public ClientCertificateMode ClientCertificateMode { get; init; } = ClientCertificateMode.NoCertificate;
    public Func<string?, X509Certificate2?>? ServerCertificateSelector { get; init; }
}
