namespace Servus.Akka.Transport;

public enum ClientCertificateMode
{
    NoCertificate = 0,
    AllowCertificate = 1,
    RequireCertificate = 2,
    DelayCertificate = 3
}
