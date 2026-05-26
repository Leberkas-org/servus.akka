using System.Net;
using System.Net.Quic;
using System.Net.Security;

namespace Servus.Akka.Transport.Quic.Client;

internal sealed class QuicClientProvider : IAsyncDisposable
{
    private readonly QuicTransportOptions _options;
    private QuicConnection? _connection;
    private readonly SemaphoreSlim _connectLock = new(1, 1);

    public QuicClientProvider(QuicTransportOptions options)
    {
        _options = options;
    }

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

            if (string.IsNullOrEmpty(_options.Host))
            {
                throw new InvalidOperationException("QUIC connections require a non-empty hostname for TLS SNI.");
            }

            EndPoint remoteEndPoint = IPAddress.TryParse(_options.Host, out var ip)
                ? new IPEndPoint(ip, _options.Port)
                : new DnsEndPoint(_options.Host, _options.Port);

            var clientConnectionOptions = new QuicClientConnectionOptions
            {
                RemoteEndPoint = remoteEndPoint,
                DefaultStreamErrorCode = 0x0100,
                DefaultCloseErrorCode = 0x0100,
                MaxInboundBidirectionalStreams = _options.MaxBidirectionalStreams,
                MaxInboundUnidirectionalStreams = _options.MaxUnidirectionalStreams,
                IdleTimeout = _options.IdleTimeout,
                ClientAuthenticationOptions = new SslClientAuthenticationOptions
                {
                    TargetHost = _options.TargetHost ?? _options.Host,
                    ApplicationProtocols = _options.ApplicationProtocols,
                    RemoteCertificateValidationCallback = _options.ServerCertificateValidationCallback,
                    EnabledSslProtocols = _options.EnabledSslProtocols,
                    ClientCertificates = _options.ClientCertificates
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
        var connection = Interlocked.Exchange(ref _connection, null);
        if (connection is not null)
        {
            await connection.DisposeAsync().ConfigureAwait(false);
        }

        _connectLock.Dispose();
    }
}