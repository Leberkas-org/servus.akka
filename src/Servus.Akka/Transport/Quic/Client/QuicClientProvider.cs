using System.Net;
using System.Net.Quic;
using System.Net.Security;

namespace Servus.Akka.Transport.Quic.Client;

internal sealed class QuicClientProvider(QuicTransportOptions options) : IAsyncDisposable
{
    private QuicConnection? _connection;
    private readonly SemaphoreSlim _connectLock = new(1, 1);

    public EndPoint? LocalEndPoint => _connection?.LocalEndPoint;
    public EndPoint? RemoteEndPoint => _connection?.RemoteEndPoint;

    public async Task<Stream> GetStreamAsync(CancellationToken ct = default)
    {
        var connection = await EnsureConnectedAsync(ct).ConfigureAwait(false);
        return await connection.OpenOutboundStreamAsync(QuicStreamType.Bidirectional, ct).ConfigureAwait(false);
    }

    public async Task<Stream> GetUnidirectionalStreamAsync(CancellationToken ct = default)
    {
        var connection = await EnsureConnectedAsync(ct).ConfigureAwait(false);
        return await connection.OpenOutboundStreamAsync(QuicStreamType.Unidirectional, ct).ConfigureAwait(false);
    }

    public async Task<Stream> AcceptInboundStreamAsync(CancellationToken ct = default)
    {
        var connection = await EnsureConnectedAsync(ct).ConfigureAwait(false);
        return await connection.AcceptInboundStreamAsync(ct).ConfigureAwait(false);
    }

    internal Task ConnectAsync(CancellationToken ct) => EnsureConnectedAsync(ct);

    private async Task<QuicConnection> EnsureConnectedAsync(CancellationToken ct)
    {
        var existing = _connection;
        if (existing is not null)
        {
            return existing;
        }

        await _connectLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            existing = _connection;
            if (existing is not null)
            {
                return existing;
            }

            if (string.IsNullOrEmpty(options.Host))
            {
                throw new InvalidOperationException("QUIC connections require a non-empty hostname for TLS SNI.");
            }

            EndPoint remoteEndPoint = IPAddress.TryParse(options.Host, out var ip)
                ? new IPEndPoint(ip, options.Port)
                : new DnsEndPoint(options.Host, options.Port);

            var clientConnectionOptions = new QuicClientConnectionOptions
            {
                RemoteEndPoint = remoteEndPoint,
                DefaultStreamErrorCode = 0x0100,
                DefaultCloseErrorCode = 0x0100,
                MaxInboundBidirectionalStreams = options.MaxBidirectionalStreams,
                MaxInboundUnidirectionalStreams = options.MaxUnidirectionalStreams,
                IdleTimeout = options.IdleTimeout,
                ClientAuthenticationOptions = new SslClientAuthenticationOptions
                {
                    TargetHost = options.TargetHost ?? options.Host,
                    ApplicationProtocols = options.ApplicationProtocols,
                    RemoteCertificateValidationCallback = options.ServerCertificateValidationCallback,
                    EnabledSslProtocols = options.EnabledSslProtocols,
                    ClientCertificates = options.ClientCertificates
                }
            };

            var connection = await QuicConnection.ConnectAsync(clientConnectionOptions, ct).ConfigureAwait(false);
            _connection = connection;
            return connection;
        }
        finally
        {
            _connectLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        var connection = _connection;
        _connection = null;
        if (connection is not null)
        {
            await connection.DisposeAsync().ConfigureAwait(false);
        }

        _connectLock.Dispose();
    }
}