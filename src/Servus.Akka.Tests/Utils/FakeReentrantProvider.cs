using System.Net;

namespace Servus.Akka.Tests.Utils;

public sealed class FakeReentrantProvider : IClientProvider
{
    private readonly TimeSpan _connectDelay;
    private readonly bool _failStreamOpen;
    private readonly SemaphoreSlim _connectLock = new(1, 1);
    private object? _connection; // simulates QuicConnection
    private int _connectionCount;
    private int _streamCount;

    public FakeReentrantProvider(int streamCount, TimeSpan connectDelay = default, bool failStreamOpen = false)
    {
        _ = streamCount; // reserved for future stream-limit tests
        _connectDelay = connectDelay;
        _failStreamOpen = failStreamOpen;
    }

    public EndPoint? RemoteEndPoint => _connection is not null ? new IPEndPoint(IPAddress.Loopback, 443) : null;
    public bool SupportsMultipleStreams => true;
    public int ConnectionCount => _connectionCount;
    public int StreamCount => _streamCount;

    public async Task<Stream> GetStreamAsync(CancellationToken ct = default)
    {
        await EnsureConnectedAsync(ct).ConfigureAwait(false);

        if (_failStreamOpen)
        {
            Interlocked.Exchange(ref _connection, null);
            throw new InvalidOperationException(
                "QUIC connection to 'fake:443' is no longer usable. "
                + "A new connection will be established on the next request.");
        }

        Interlocked.Increment(ref _streamCount);
        return new MemoryStream();
    }

    public void KillConnection()
    {
        Interlocked.Exchange(ref _connection, null);
    }

    public void Close()
    {
        Interlocked.Exchange(ref _connection, null);
    }

    public ValueTask DisposeAsync()
    {
        Close();
        return ValueTask.CompletedTask;
    }

    private async Task EnsureConnectedAsync(CancellationToken ct)
    {
        if (Volatile.Read(ref _connection) is not null)
        {
            return;
        }

        await _connectLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (Volatile.Read(ref _connection) is not null)
            {
                return;
            }

            if (_connectDelay > TimeSpan.Zero)
            {
                await Task.Delay(_connectDelay, ct).ConfigureAwait(false);
            }

            Volatile.Write(ref _connection, new object());
            Interlocked.Increment(ref _connectionCount);
        }
        finally
        {
            _connectLock.Release();
        }
    }
}

public sealed class MinimalClientProvider : IClientProvider
{
    public EndPoint? RemoteEndPoint => null;

    public Task<Stream> GetStreamAsync(CancellationToken ct = default) =>
        Task.FromResult<Stream>(new MemoryStream());

    public static void Close()
    {
    }

    public ValueTask DisposeAsync()
    {
        Close();
        return ValueTask.CompletedTask;
    }
}

public interface IClientProvider : IAsyncDisposable
{
    EndPoint? RemoteEndPoint { get; }
    bool SupportsMultipleStreams => false;
    Task<Stream> GetStreamAsync(CancellationToken ct = default);
}