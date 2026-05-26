using System.Net;

namespace Servus.Akka.Tests.Utils;

public sealed class TestProxy(Uri? proxyUri, string? bypassedHost = null, ICredentials? credentials = null)
    : IWebProxy
{
    public ICredentials? Credentials { get; set; } = credentials;

    public Uri? GetProxy(Uri destination) => proxyUri;

    public bool IsBypassed(Uri host)
    {
        if (bypassedHost is null)
        {
            return false;
        }

        return host.Host == bypassedHost;
    }
}