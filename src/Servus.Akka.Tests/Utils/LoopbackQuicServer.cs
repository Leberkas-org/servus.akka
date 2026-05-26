using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Servus.Akka.Tests.Utils;

public sealed class LoopbackQuicServer : IAsyncDisposable
{
    public static SslApplicationProtocol Alpn => new("h3");
    private readonly QuicListener _listener;
    private readonly X509Certificate2 _cert;
    public int Port { get; }

    private LoopbackQuicServer(QuicListener listener, X509Certificate2 cert, int port)
    {
        _listener = listener;
        _cert = cert;
        Port = port;
    }

    public static async Task<LoopbackQuicServer> CreateAsync()
    {
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest("CN=localhost", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var san = new SubjectAlternativeNameBuilder();
        san.AddDnsName("localhost");
        san.AddIpAddress(IPAddress.Loopback);
        req.CertificateExtensions.Add(san.Build());
        req.CertificateExtensions.Add(
            new X509EnhancedKeyUsageExtension(
                new OidCollection { new Oid("1.3.6.1.5.5.7.3.1") }, false));
        var ephemeral = req.CreateSelfSigned(DateTimeOffset.Now.AddDays(-1), DateTimeOffset.Now.AddHours(1));
        var pfx = ephemeral.Export(X509ContentType.Pfx, "");
        var cert = X509CertificateLoader.LoadPkcs12(pfx, "", X509KeyStorageFlags.Exportable);
        ephemeral.Dispose();

        var certContext = SslStreamCertificateContext.Create(cert, null);
        var protocols = new List<SslApplicationProtocol> { Alpn };

        var listener = await QuicListener.ListenAsync(new QuicListenerOptions
        {
            ListenEndPoint = new IPEndPoint(IPAddress.IPv6Loopback, 0),
            ApplicationProtocols = protocols,
            ConnectionOptionsCallback = (_, _, _) => ValueTask.FromResult(new QuicServerConnectionOptions
            {
                DefaultStreamErrorCode = 0x0100,
                DefaultCloseErrorCode = 0x0100,
                ServerAuthenticationOptions = new SslServerAuthenticationOptions
                {
                    ServerCertificateContext = certContext,
                    ApplicationProtocols = protocols
                }
            })
        });

        var port = listener.LocalEndPoint.Port;
        return new LoopbackQuicServer(listener, cert, port);
    }

    public async Task<QuicConnection> AcceptConnectionAsync(CancellationToken ct = default)
    {
        return await _listener.AcceptConnectionAsync(ct);
    }

    public async ValueTask DisposeAsync()
    {
        await _listener.DisposeAsync();
        _cert.Dispose();
    }
}