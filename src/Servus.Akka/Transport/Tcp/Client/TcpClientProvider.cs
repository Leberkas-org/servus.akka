using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using static Servus.Core.Servus;

namespace Servus.Akka.Transport.Tcp.Client;

internal class TcpClientProvider(TcpTransportOptions options) : IAsyncDisposable
{
    private Socket? _socket;

    public EndPoint? LocalEndPoint => _socket?.LocalEndPoint;
    public EndPoint? RemoteEndPoint => _socket?.RemoteEndPoint;

    public async Task<Stream> GetStreamAsync(CancellationToken ct = default)
    {
        var proxyUri = ResolveProxy(options);

        var connectHost = proxyUri?.Host ?? options.Host;
        var connectPort = proxyUri?.Port ?? options.Port;

        _socket = CreateSocket(options.SocketSendBufferSize, options.SocketReceiveBufferSize);

        var dnsActivity = Tracing.StartDnsLookup(connectHost);
        IPAddress[] addresses;
        try
        {
            var dnsStart = Stopwatch.GetTimestamp();
            addresses = await DnsCache.ResolveAsync(connectHost, ct).ConfigureAwait(false);
            var dnsDuration = Stopwatch.GetElapsedTime(dnsStart).TotalSeconds;

            if (addresses.Length == 0)
            {
                throw new InvalidOperationException($"Could not resolve any IP addresses for host '{connectHost}'.");
            }

            if (dnsActivity is not null)
            {
                Tracing.SetDnsAnswers(dnsActivity,
                    Array.ConvertAll(addresses, a => a.ToString()));
            }

            Metrics.DnsLookupDuration().Record(dnsDuration,
                new KeyValuePair<string, object?>("dns.question.name", connectHost));
            dnsActivity?.Stop();
            Tracing.For("Dns").Debug(this, "Resolved {0} → {1} address(es)", connectHost, addresses.Length);
        }
        catch (Exception ex)
        {
            if (dnsActivity is not null)
            {
                Tracing.SetError(dnsActivity, ex);
                dnsActivity.Stop();
            }

            Tracing.For("Dns").Warning(this, "DNS '{0}' failed: {1}", connectHost, ex.Message);
            throw;
        }

        var networkType = addresses[0].AddressFamily == AddressFamily.InterNetworkV6
            ? "ipv6"
            : "ipv4";
        var socketActivity = Tracing.StartSocketConnect(
            addresses[0].ToString(), connectPort, "tcp", networkType);
        try
        {
            await _socket.ConnectAsync(addresses, connectPort, ct).ConfigureAwait(false);
            socketActivity?.Stop();
            Tracing.For("Connection").Debug(this, "TCP connected to {0}:{1}", addresses[0], connectPort);
        }
        catch (Exception ex)
        {
            if (socketActivity is not null)
            {
                Tracing.SetError(socketActivity, ex);
                socketActivity.Stop();
            }

            Tracing.For("Connection").Warning(this, "TCP connect to {0}:{1} failed: {2}", addresses[0], connectPort, ex.Message);
            throw;
        }

        return new NetworkStream(_socket, ownsSocket: false);
    }

    private static Uri? ResolveProxy(TcpTransportOptions options)
    {
        if (!options.UseProxy || options.Proxy is null)
        {
            return null;
        }

        var targetUri = new Uri($"http://{options.Host}:{options.Port}/");

        if (options.Proxy.IsBypassed(targetUri))
        {
            return null;
        }

        if (options.DefaultProxyCredentials is not null && options.Proxy.Credentials is null)
        {
            options.Proxy.Credentials = options.DefaultProxyCredentials;
        }

        return options.Proxy.GetProxy(targetUri);
    }

    public ValueTask DisposeAsync()
    {
        if (_socket is null)
        {
            return ValueTask.CompletedTask;
        }

        try
        {
            _socket.Close();
            _socket.Dispose();
        }
        catch (ObjectDisposedException)
        {
        }
        finally
        {
            _socket = null;
        }

        return ValueTask.CompletedTask;
    }

    private static Socket CreateSocket(int? sendBufferSize, int? receiveBufferSize)
    {
        var result = new Socket(SocketType.Stream, ProtocolType.Tcp)
        {
            NoDelay = true,
            LingerState = new LingerOption(true, 0),
        };

        result.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);

        if (sendBufferSize.HasValue)
        {
            result.SendBufferSize = sendBufferSize.Value;
        }

        if (receiveBufferSize.HasValue)
        {
            result.ReceiveBufferSize = receiveBufferSize.Value;
        }

        return result;
    }
}
