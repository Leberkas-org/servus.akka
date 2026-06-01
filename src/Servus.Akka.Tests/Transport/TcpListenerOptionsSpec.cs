using Servus.Akka.Transport;

namespace Servus.Akka.Tests.Transport;

public sealed class TcpListenerOptionsSpec
{
    [Fact(Timeout = 5000)]
    public void TcpListenerOptions_should_default_client_certificate_mode_to_no_certificate()
    {
        var options = new TcpListenerOptions { Host = "localhost", Port = 443 };

        Assert.Equal(ClientCertificateMode.NoCertificate, options.ClientCertificateMode);
    }

    [Fact(Timeout = 5000)]
    public void TcpListenerOptions_should_default_server_certificate_selector_to_null()
    {
        var options = new TcpListenerOptions { Host = "localhost", Port = 443 };

        Assert.Null(options.ServerCertificateSelector);
    }

    [Fact(Timeout = 5000)]
    public void TcpListenerOptions_should_allow_setting_client_certificate_mode()
    {
        var options = new TcpListenerOptions
        {
            Host = "localhost",
            Port = 443,
            ClientCertificateMode = ClientCertificateMode.RequireCertificate
        };

        Assert.Equal(ClientCertificateMode.RequireCertificate, options.ClientCertificateMode);
    }
}