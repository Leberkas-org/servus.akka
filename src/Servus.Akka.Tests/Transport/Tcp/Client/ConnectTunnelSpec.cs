using System.IO.Pipelines;
using System.Net;
using System.Text;
using Servus.Akka.Tests.Utils;
using Servus.Akka.Transport.Tcp.Client;

namespace Servus.Akka.Tests.Transport.Tcp.Client;

public sealed class ConnectTunnelSpec
{
    private const string TargetHost = "example.com";
    private const int TargetPort = 443;

    [Fact(Timeout = 10_000)]
    public async Task Tunnel_should_send_correct_CONNECT_request()
    {
        var (clientStream, serverStream) = CreateDuplexPipe();

        var tunnelTask = TlsClientProvider.EstablishConnectTunnelAsync(
            clientStream, TargetHost, TargetPort,
            new SimpleProxy(), null, TestContext.Current.CancellationToken);

        var request = await ReadRequestAsync(serverStream);
        await WriteResponseAsync(serverStream, "HTTP/1.1 200 Connection Established\r\n\r\n");
        await tunnelTask;

        Assert.StartsWith($"CONNECT {TargetHost}:{TargetPort} HTTP/1.1\r\n", request);
        Assert.Contains($"Host: {TargetHost}:{TargetPort}\r\n", request);
        Assert.EndsWith("\r\n\r\n", request);
    }

    [Fact(Timeout = 5000)]
    public async Task Tunnel_should_succeed_on_200_response()
    {
        var (clientStream, serverStream) = CreateDuplexPipe();

        var tunnelTask = TlsClientProvider.EstablishConnectTunnelAsync(
            clientStream, TargetHost, TargetPort,
            new SimpleProxy(), null, TestContext.Current.CancellationToken);

        await ReadRequestAsync(serverStream);
        await WriteResponseAsync(serverStream, "HTTP/1.1 200 Connection Established\r\n\r\n");

        await tunnelTask;
    }

    [Fact(Timeout = 5000)]
    public async Task Tunnel_should_throw_on_non_200_response()
    {
        var (clientStream, serverStream) = CreateDuplexPipe();

        var tunnelTask = TlsClientProvider.EstablishConnectTunnelAsync(
            clientStream, TargetHost, TargetPort,
            new SimpleProxy(), null, TestContext.Current.CancellationToken);

        await ReadRequestAsync(serverStream);
        await WriteResponseAsync(serverStream, "HTTP/1.1 407 Proxy Authentication Required\r\n\r\n");

        var ex = await Assert.ThrowsAsync<HttpRequestException>(() => tunnelTask);
        Assert.Contains("407", ex.Message);
    }

    [Fact(Timeout = 5000)]
    public async Task Tunnel_should_throw_on_proxy_close()
    {
        var (clientStream, serverStream) = CreateDuplexPipe();

        var tunnelTask = TlsClientProvider.EstablishConnectTunnelAsync(
            clientStream, TargetHost, TargetPort,
            new SimpleProxy(), null, TestContext.Current.CancellationToken);

        await ReadRequestAsync(serverStream);
        await serverStream.DisposeAsync();

        await Assert.ThrowsAsync<HttpRequestException>(() => tunnelTask);
    }

    [Fact(Timeout = 5000)]
    public async Task Tunnel_should_include_proxy_auth_header()
    {
        var (clientStream, serverStream) = CreateDuplexPipe();

        var credentials = new NetworkCredential("user", "pass");
        var proxy = new SimpleProxy(credentials);

        var tunnelTask = TlsClientProvider.EstablishConnectTunnelAsync(
            clientStream, TargetHost, TargetPort,
            proxy, null, TestContext.Current.CancellationToken);

        var request = await ReadRequestAsync(serverStream);
        await WriteResponseAsync(serverStream, "HTTP/1.1 200 OK\r\n\r\n");
        await tunnelTask;

        var expectedEncoded = Convert.ToBase64String("user:pass"u8.ToArray());
        Assert.Contains($"Proxy-Authorization: Basic {expectedEncoded}\r\n", request);
    }

    [Fact(Timeout = 5000)]
    public async Task Tunnel_should_accept_http10_200_response()
    {
        var (clientStream, serverStream) = CreateDuplexPipe();

        var tunnelTask = TlsClientProvider.EstablishConnectTunnelAsync(
            clientStream, TargetHost, TargetPort,
            new SimpleProxy(), null, TestContext.Current.CancellationToken);

        await ReadRequestAsync(serverStream);
        await WriteResponseAsync(serverStream, "HTTP/1.0 200 OK\r\n\r\n");

        await tunnelTask;
    }

    private static (Stream Client, Stream Server) CreateDuplexPipe()
    {
        var clientToServer = new Pipe();
        var serverToClient = new Pipe();

        var clientStream = new DuplexPipeStream(
            serverToClient.Reader, clientToServer.Writer);
        var serverStream = new DuplexPipeStream(
            clientToServer.Reader, serverToClient.Writer);

        return (clientStream, serverStream);
    }

    private static async Task<string> ReadRequestAsync(Stream serverStream)
    {
        var buffer = new byte[4096];
        var totalRead = 0;

        while (totalRead < buffer.Length)
        {
            var read = await serverStream.ReadAsync(buffer.AsMemory(totalRead));
            if (read == 0)
            {
                break;
            }

            totalRead += read;
            var text = Encoding.ASCII.GetString(buffer, 0, totalRead);
            if (text.Contains("\r\n\r\n"))
            {
                break;
            }
        }

        return Encoding.ASCII.GetString(buffer, 0, totalRead);
    }

    private static async Task WriteResponseAsync(Stream serverStream, string response)
    {
        await serverStream.WriteAsync(Encoding.ASCII.GetBytes(response));
        await serverStream.FlushAsync();
    }
}