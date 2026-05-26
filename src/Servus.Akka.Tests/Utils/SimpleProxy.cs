using System.Net;

namespace Servus.Akka.Tests.Utils;

public sealed class SimpleProxy(ICredentials? credentials = null) : IWebProxy
{
    public ICredentials? Credentials
    {
        get => credentials;
        set { }
    }

    public Uri GetProxy(Uri destination) => new($"http://proxy.local:8080/");

    public bool IsBypassed(Uri host) => false;
}