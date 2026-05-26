using System.Buffers;
using System.Net;
using System.Net.Security;

namespace Servus.Akka.Transport.Tcp.Client;

internal class TlsClientProvider(TlsTransportOptions options) : IAsyncDisposable
{
    private readonly TcpClientProvider _tcpClientProvider = new(new TcpTransportOptions
    {
        Host = options.Host,
        Port = options.Port,
        ConnectTimeout = options.ConnectTimeout,
        SocketSendBufferSize = options.SocketSendBufferSize,
        SocketReceiveBufferSize = options.SocketReceiveBufferSize,
        UseProxy = options.UseProxy,
        Proxy = options.Proxy,
        DefaultProxyCredentials = options.DefaultProxyCredentials
    });

    private SslStream? _sslStream;

    public EndPoint? LocalEndPoint => _tcpClientProvider.LocalEndPoint;
    public EndPoint? RemoteEndPoint => _tcpClientProvider.RemoteEndPoint;
    public System.Security.Authentication.SslProtocols? NegotiatedSslProtocol => _sslStream?.SslProtocol;
    public SslApplicationProtocol? NegotiatedApplicationProtocol => _sslStream?.NegotiatedApplicationProtocol;

    public async Task<Stream> GetStreamAsync(CancellationToken ct = default)
    {
        var networkStream = await _tcpClientProvider.GetStreamAsync(ct).ConfigureAwait(false);

        if (options is { UseProxy: true, Proxy: not null })
        {
            var proxyUri = options.Proxy.GetProxy(new Uri($"https://{options.Host}:{options.Port}/"));
            if (proxyUri is not null)
            {
                await EstablishConnectTunnelAsync(networkStream, options.Host, options.Port,
                    options.Proxy, options.DefaultProxyCredentials, ct).ConfigureAwait(false);
            }
        }

        _sslStream = new SslStream(
            networkStream,
            leaveInnerStreamOpen: false,
            options.ServerCertificateValidationCallback
        );

        var targetHost = options.TargetHost ?? options.Host;
        var authOptions = new SslClientAuthenticationOptions
        {
            TargetHost = targetHost,
            EnabledSslProtocols = options.EnabledSslProtocols,
            ClientCertificates = options.ClientCertificates,
            ApplicationProtocols = options.ApplicationProtocols,
        };

        try
        {
            await _sslStream.AuthenticateAsClientAsync(authOptions, ct)
                .WaitAsync(options.ConnectTimeout, ct)
                .ConfigureAwait(false);
        }
        catch
        {
            throw;
        }

        return _sslStream;
    }

    public static async Task EstablishConnectTunnelAsync(
        Stream proxyStream,
        string targetHost,
        int targetPort,
        IWebProxy proxy,
        ICredentials? defaultProxyCredentials,
        CancellationToken ct)
    {
        var connectRequest = $"CONNECT {targetHost}:{targetPort} HTTP/1.1\r\nHost: {targetHost}:{targetPort}\r\n";

        var proxyUri = proxy.GetProxy(new Uri($"https://{targetHost}:{targetPort}/"));
        var credentials = proxy.Credentials ?? defaultProxyCredentials;
        if (credentials is not null && proxyUri is not null)
        {
            var credential = credentials.GetCredential(proxyUri, "Basic");
            if (credential is not null)
            {
                var encoded = Convert.ToBase64String(
                    System.Text.Encoding.UTF8.GetBytes($"{credential.UserName}:{credential.Password}"));
                connectRequest += $"Proxy-Authorization: Basic {encoded}\r\n";
            }
        }

        connectRequest += "\r\n";

        var requestBytes = System.Text.Encoding.ASCII.GetBytes(connectRequest);
        await proxyStream.WriteAsync(requestBytes, ct).ConfigureAwait(false);
        await proxyStream.FlushAsync(ct).ConfigureAwait(false);

        var responseBuffer = ArrayPool<byte>.Shared.Rent(4096);
        try
        {
            var totalRead = 0;
            while (totalRead < responseBuffer.Length)
            {
                var bytesRead = await proxyStream.ReadAsync(
                    responseBuffer.AsMemory(totalRead, responseBuffer.Length - totalRead), ct).ConfigureAwait(false);

                if (bytesRead == 0)
                {
                    throw new HttpRequestException("Proxy closed connection during CONNECT tunnel establishment.");
                }

                totalRead += bytesRead;

                var span = responseBuffer.AsSpan(0, totalRead);
                var headerEnd = span.IndexOf("\r\n\r\n"u8);
                if (headerEnd >= 0)
                {
                    if (!span.StartsWith("HTTP/1.1 200"u8) && !span.StartsWith("HTTP/1.0 200"u8))
                    {
                        var crIndex = span.IndexOf((byte)'\r');
                        var statusLine = System.Text.Encoding.ASCII.GetString(span[..crIndex]);
                        throw new HttpRequestException($"Proxy CONNECT tunnel failed: {statusLine}");
                    }

                    return;
                }
            }

            throw new HttpRequestException("Proxy CONNECT response exceeded buffer size.");
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(responseBuffer);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_sslStream is not null)
        {
            try
            {
                await _sslStream.DisposeAsync().ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
            }
            finally
            {
                _sslStream = null;
            }
        }

        await _tcpClientProvider.DisposeAsync().ConfigureAwait(false);
    }
}
