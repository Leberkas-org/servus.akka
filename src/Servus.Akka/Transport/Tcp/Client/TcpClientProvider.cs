using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using static Servus.Senf;

namespace Servus.Akka.Transport.Tcp.Client;

internal class TcpClientProvider(TcpTransportOptions options) : IAsyncDisposable
{
    // Happy-Eyeballs-style stagger: a follow-up address attempt starts this long after the
    // previous one, instead of waiting for it to fail. Windows silently drops SYNs to a
    // non-listening ::1 (no RST, ~2s until failure), so a sequential fallback would pay that
    // penalty on every connect to an IPv4-only service resolved via "localhost".
    internal static readonly TimeSpan StaggerDelay = TimeSpan.FromMilliseconds(250);

    private Socket? _socket;

    public EndPoint? LocalEndPoint => _socket?.LocalEndPoint;
    public EndPoint? RemoteEndPoint => _socket?.RemoteEndPoint;
    public Socket? ConnectedSocket => _socket;

    public async Task<Stream> GetStreamAsync(CancellationToken ct = default)
    {
        var proxyUri = ResolveProxy(options);

        var connectHost = proxyUri?.Host ?? options.Host;
        var connectPort = proxyUri?.Port ?? options.Port;

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
            _socket = await ConnectWithFallbackAsync(addresses, connectPort, ct).ConfigureAwait(false);
            socketActivity?.Stop();
            Tracing.For("Connection").Debug(this, "TCP connected to {0}", _socket.RemoteEndPoint);
        }
        catch (Exception ex)
        {
            if (socketActivity is not null)
            {
                Tracing.SetError(socketActivity, ex);
                socketActivity.Stop();
            }

            Tracing.For("Connection").Warning(this, "TCP connect to {0} port {1} ({2} address(es)) failed: {3}",
                connectHost, connectPort, addresses.Length, ex.Message);
            throw;
        }

        return new NetworkStream(_socket, ownsSocket: false);
    }

    internal async Task<Socket> ConnectWithFallbackAsync(IPAddress[] addresses, int port, CancellationToken ct)
    {
        var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        attemptCts.CancelAfter(options.ConnectTimeout);
        var token = attemptCts.Token;

        var pending = new List<Task<Socket>>(addresses.Length);
        var nextAddress = 0;
        Exception lastFailure = new SocketException((int)SocketError.HostUnreachable);

        try
        {
            while (true)
            {
                if (pending.Count == 0)
                {
                    pending.Add(ConnectAttemptAsync(addresses[nextAddress++], port, token));
                }

                Task<Socket> finished;
                if (nextAddress < addresses.Length)
                {
                    var stagger = Task.Delay(StaggerDelay, token);
                    var completed = await Task.WhenAny(pending.Concat<Task>([stagger])).ConfigureAwait(false);
                    if (ReferenceEquals(completed, stagger))
                    {
                        pending.Add(ConnectAttemptAsync(addresses[nextAddress++], port, token));
                        continue;
                    }

                    finished = (Task<Socket>)completed;
                }
                else
                {
                    finished = await Task.WhenAny(pending).ConfigureAwait(false);
                }

                pending.Remove(finished);

                try
                {
                    return await finished.ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    lastFailure = ex;
                    if (pending.Count == 0 && nextAddress >= addresses.Length)
                    {
                        throw TranslateConnectFailure(lastFailure, ct);
                    }
                }
            }
        }
        finally
        {
            attemptCts.Cancel();
            if (pending.Count == 0)
            {
                attemptCts.Dispose();
            }
            else
            {
                _ = AbandonAttemptsAsync(pending, attemptCts);
            }
        }
    }

    private async Task<Socket> ConnectAttemptAsync(IPAddress address, int port, CancellationToken ct)
    {
        // Fresh socket per attempt: a socket whose connect failed is not reliably reusable on
        // Windows, and a family-specific socket avoids dual-mode quirks.
        var socket = CreateSocket(address.AddressFamily, options.SocketSendBufferSize, options.SocketReceiveBufferSize);
        try
        {
            await socket.ConnectAsync(address, port, ct).ConfigureAwait(false);
            return socket;
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    private async Task AbandonAttemptsAsync(List<Task<Socket>> pending, CancellationTokenSource attemptCts)
    {
        foreach (var attempt in pending)
        {
            try
            {
                var socket = await attempt.ConfigureAwait(false);
                socket.Dispose();
            }
            catch (Exception ex)
            {
                // Cancelled or failed attempts dispose their own socket.
                Tracing.For("Connection").Trace(this, "Abandoned connect attempt ended: {0}", ex.Message);
            }
        }

        attemptCts.Dispose();
    }

    private static Exception TranslateConnectFailure(Exception lastFailure, CancellationToken ct)
    {
        if (lastFailure is OperationCanceledException)
        {
            return ct.IsCancellationRequested
                ? new OperationCanceledException("TCP connect was cancelled.", lastFailure, ct)
                : new SocketException((int)SocketError.TimedOut);
        }

        return lastFailure;
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

    private static Socket CreateSocket(AddressFamily addressFamily, int? sendBufferSize, int? receiveBufferSize)
    {
        var result = new Socket(addressFamily, SocketType.Stream, ProtocolType.Tcp)
        {
            // Graceful close (FIN), not an abortive RST. An HTTP keep-alive / pooled connection must
            // flush in-flight bytes and let the peer observe a clean shutdown; SO_LINGER(true, 0) would
            // reset on every close, risk truncating the final bytes, and pollute the server with RSTs.
            NoDelay = true,
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
