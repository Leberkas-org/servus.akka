using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Tcp.Client;

namespace Servus.Akka.Tests.Transport.Tcp.Client;

[Collection("ClientProvider")]
public sealed class TcpClientProviderMultiAddressSpec
{
    private static TcpTransportOptions OptionsFor(int port) => new()
    {
        Host = "localhost",
        Port = (ushort)port
    };

    private static int GetUnusedPort()
    {
        using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        socket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        return ((IPEndPoint)socket.LocalEndPoint!).Port;
    }

    [Fact(Timeout = 10_000)]
    public async Task ConnectWithFallbackAsync_should_fall_back_to_next_address_when_first_is_not_listening()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;

        var provider = new TcpClientProvider(OptionsFor(port));
        try
        {
            var start = Stopwatch.GetTimestamp();
            var socket = await provider.ConnectWithFallbackAsync(
                [IPAddress.IPv6Loopback, IPAddress.Loopback], port, CancellationToken.None);
            var elapsed = Stopwatch.GetElapsedTime(start);

            Assert.True(socket.Connected);
            var remote = (IPEndPoint)socket.RemoteEndPoint!;
            Assert.Equal(IPAddress.Loopback, remote.Address);

            // Windows stealth-drops SYNs to a non-listening ::1 (~2050ms p50). The staggered
            // fallback must connect via 127.0.0.1 after ~250ms instead of waiting that out.
            Assert.True(elapsed < TimeSpan.FromMilliseconds(1500),
                $"Fallback connect took {elapsed.TotalMilliseconds:F0}ms, expected < 1500ms");

            socket.Dispose();
        }
        finally
        {
            listener.Stop();
            await provider.DisposeAsync();
        }
    }

    [Fact(Timeout = 10_000)]
    public async Task GetStreamAsync_should_expose_actually_connected_remote_endpoint()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;

        var provider = new TcpClientProvider(OptionsFor(port));
        try
        {
            await using var stream = await provider.GetStreamAsync(CancellationToken.None);

            Assert.NotNull(provider.ConnectedSocket);
            var remote = Assert.IsType<IPEndPoint>(provider.RemoteEndPoint);
            Assert.Equal(IPAddress.Loopback, remote.Address);
            Assert.Equal(port, remote.Port);
        }
        finally
        {
            listener.Stop();
            await provider.DisposeAsync();
        }
    }

    [Fact(Timeout = 10_000)]
    public async Task ConnectWithFallbackAsync_should_throw_SocketException_when_all_addresses_fail()
    {
        var port = GetUnusedPort();
        var provider = new TcpClientProvider(OptionsFor(port));

        await Assert.ThrowsAsync<SocketException>(async () =>
            await provider.ConnectWithFallbackAsync(
                [IPAddress.Loopback, IPAddress.Loopback], port, CancellationToken.None));

        Assert.Null(provider.ConnectedSocket);
        await provider.DisposeAsync();
    }

    [Fact(Timeout = 10_000)]
    public async Task ConnectWithFallbackAsync_should_throw_OperationCanceledException_when_cancelled_mid_connect()
    {
        var provider = new TcpClientProvider(OptionsFor(443));
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        var start = Stopwatch.GetTimestamp();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await provider.ConnectWithFallbackAsync(
                [IPAddress.Parse("192.0.2.1"), IPAddress.Parse("192.0.2.2")], 443, cts.Token));
        var elapsed = Stopwatch.GetElapsedTime(start);

        Assert.True(elapsed < TimeSpan.FromSeconds(5),
            $"Cancellation took {elapsed.TotalMilliseconds:F0}ms to propagate");
        Assert.Null(provider.ConnectedSocket);
        await provider.DisposeAsync();
    }

    [Fact(Timeout = 10_000)]
    public async Task ConnectWithFallbackAsync_should_time_out_with_SocketException_after_ConnectTimeout()
    {
        var options = OptionsFor(443) with { ConnectTimeout = TimeSpan.FromMilliseconds(300) };
        var provider = new TcpClientProvider(options);

        var exception = await Assert.ThrowsAsync<SocketException>(async () =>
            await provider.ConnectWithFallbackAsync(
                [IPAddress.Parse("192.0.2.1")], 443, CancellationToken.None));

        Assert.Equal(SocketError.TimedOut, exception.SocketErrorCode);
        Assert.Null(provider.ConnectedSocket);
        await provider.DisposeAsync();
    }
}
