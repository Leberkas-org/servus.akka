using System.Net.Quic;
using System.Net.Security;
using System.Security.Authentication;
using Servus.Akka.Tests.Utils;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Quic.Client;

namespace Servus.Akka.Tests.Transport.Quic.Client;

[Collection("ClientProvider")]
public sealed class QuicClientProviderSpec
{
    private static async Task<Stream> GetStreamOrSkipAsync(QuicClientProvider provider, CancellationToken ct)
    {
        try
        {
            return await provider.GetStreamAsync(ct);
        }
        catch (AuthenticationException ex)
        {
            Assert.Skip(string.Concat("QUIC ALPN not available: ", ex.Message));
            return null!;
        }
    }

    private static async Task<Stream> GetUnidirectionalStreamOrSkipAsync(QuicClientProvider provider, CancellationToken ct)
    {
        try
        {
            return await provider.GetUnidirectionalStreamAsync(ct);
        }
        catch (AuthenticationException ex)
        {
            Assert.Skip(string.Concat("QUIC ALPN not available: ", ex.Message));
            return null!;
        }
    }

    private static async Task ConnectOrSkipAsync(QuicClientProvider provider, CancellationToken ct)
    {
        try
        {
            await provider.ConnectAsync(ct);
        }
        catch (AuthenticationException ex)
        {
            Assert.Skip(string.Concat("QUIC ALPN not available: ", ex.Message));
        }
    }

    [Fact(Timeout = 15000)]
    public async Task GetStreamAsync_should_return_bidirectional_stream()
    {
        if (!QuicListener.IsSupported)
        {
            return;
        }

        var server = await LoopbackQuicServer.CreateAsync();
        var acceptTask = Task.Run(async () =>
        {
            try
            {
                var conn = await server.AcceptConnectionAsync(TestContext.Current.CancellationToken);
                var stream = await conn.AcceptInboundStreamAsync(TestContext.Current.CancellationToken);
                await stream.WriteAsync(new byte[] { 42 }, TestContext.Current.CancellationToken);
                stream.CompleteWrites();
                return conn;
            }
            catch
            {
                return null;
            }
        });

        var options = new QuicTransportOptions
        {
            Host = "localhost",
            Port = (ushort)server.Port,
            ApplicationProtocols = [LoopbackQuicServer.Alpn],
            ServerCertificateValidationCallback = (_, _, _, _) => true
        };

        var provider = new QuicClientProvider(options);

        try
        {
            var stream = await GetStreamOrSkipAsync(provider, TestContext.Current.CancellationToken);
            Assert.NotNull(stream);
            Assert.IsAssignableFrom<QuicStream>(stream);

            await stream.DisposeAsync();
        }
        finally
        {
            await provider.DisposeAsync();
            await server.DisposeAsync();
            var serverConn = await acceptTask;
            if (serverConn is not null)
            {
                await serverConn.DisposeAsync();
            }
        }
    }

    [Fact(Timeout = 15000)]
    public async Task GetUnidirectionalStreamAsync_should_return_unidirectional_stream()
    {
        if (!QuicListener.IsSupported)
        {
            return;
        }

        var server = await LoopbackQuicServer.CreateAsync();
        var acceptTask = Task.Run(async () =>
        {
            try
            {
                return await server.AcceptConnectionAsync(TestContext.Current.CancellationToken);
            }
            catch
            {
                return null;
            }
        });

        var options = new QuicTransportOptions
        {
            Host = "localhost",
            Port = (ushort)server.Port,
            ApplicationProtocols = [LoopbackQuicServer.Alpn],
            ServerCertificateValidationCallback = (_, _, _, _) => true
        };

        var provider = new QuicClientProvider(options);

        try
        {
            var stream = await GetUnidirectionalStreamOrSkipAsync(provider, TestContext.Current.CancellationToken);
            Assert.NotNull(stream);
            Assert.IsAssignableFrom<QuicStream>(stream);

            await stream.DisposeAsync();
        }
        finally
        {
            await provider.DisposeAsync();
            await server.DisposeAsync();
            var serverConn = await acceptTask;
            if (serverConn is not null)
            {
                await serverConn.DisposeAsync();
            }
        }
    }

    [Fact(Timeout = 15000)]
    public async Task AcceptInboundStreamAsync_should_accept_inbound_stream()
    {
        if (!QuicListener.IsSupported)
        {
            return;
        }

        var server = await LoopbackQuicServer.CreateAsync();
        var serverReady = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var acceptTask = Task.Run(async () =>
        {
            try
            {
                var conn = await server.AcceptConnectionAsync(TestContext.Current.CancellationToken);
                serverReady.SetResult();
                var stream = await conn.OpenOutboundStreamAsync(QuicStreamType.Bidirectional, TestContext.Current.CancellationToken);
                await stream.WriteAsync(new byte[] { 42 }, TestContext.Current.CancellationToken);
                stream.CompleteWrites();
                return conn;
            }
            catch
            {
                serverReady.TrySetResult();
                return null;
            }
        });

        var options = new QuicTransportOptions
        {
            Host = "localhost",
            Port = (ushort)server.Port,
            ApplicationProtocols = [LoopbackQuicServer.Alpn],
            ServerCertificateValidationCallback = (_, _, _, _) => true,
            MaxBidirectionalStreams = 10
        };

        var provider = new QuicClientProvider(options);

        try
        {
            await ConnectOrSkipAsync(provider, TestContext.Current.CancellationToken);
            await serverReady.Task;
            var stream = await provider.AcceptInboundStreamAsync(TestContext.Current.CancellationToken);
            Assert.NotNull(stream);
            Assert.IsAssignableFrom<QuicStream>(stream);

            await stream.DisposeAsync();
        }
        finally
        {
            await provider.DisposeAsync();
            await server.DisposeAsync();
            var serverConn = await acceptTask;
            if (serverConn is not null)
            {
                await serverConn.DisposeAsync();
            }
        }
    }

    [Fact(Timeout = 15000)]
    public async Task EnsureConnectedAsync_should_reuse_connection()
    {
        if (!QuicListener.IsSupported)
        {
            return;
        }

        var server = await LoopbackQuicServer.CreateAsync();
        var acceptTask = Task.Run(async () =>
        {
            try
            {
                var conn = await server.AcceptConnectionAsync(TestContext.Current.CancellationToken);
                var stream1 = await conn.AcceptInboundStreamAsync(TestContext.Current.CancellationToken);
                var stream2 = await conn.AcceptInboundStreamAsync(TestContext.Current.CancellationToken);
                await stream1.WriteAsync(new byte[] { 42 }, TestContext.Current.CancellationToken);
                await stream2.WriteAsync(new byte[] { 43 }, TestContext.Current.CancellationToken);
                stream1.CompleteWrites();
                stream2.CompleteWrites();
                return conn;
            }
            catch
            {
                return null;
            }
        });

        var options = new QuicTransportOptions
        {
            Host = "localhost",
            Port = (ushort)server.Port,
            ApplicationProtocols = [LoopbackQuicServer.Alpn],
            ServerCertificateValidationCallback = (_, _, _, _) => true
        };

        var provider = new QuicClientProvider(options);

        try
        {
            var stream1 = await GetStreamOrSkipAsync(provider, TestContext.Current.CancellationToken);
            var stream2 = await provider.GetStreamAsync(TestContext.Current.CancellationToken);

            Assert.NotNull(stream1);
            Assert.NotNull(stream2);

            await stream1.DisposeAsync();
            await stream2.DisposeAsync();
        }
        finally
        {
            await provider.DisposeAsync();
            await server.DisposeAsync();
            var serverConn = await acceptTask;
            if (serverConn is not null)
            {
                await serverConn.DisposeAsync();
            }
        }
    }

    [Fact(Timeout = 5000)]
    public async Task EnsureConnectedAsync_should_throw_on_empty_host()
    {
        var protocolList = new List<SslApplicationProtocol> { LoopbackQuicServer.Alpn };
        var options = new QuicTransportOptions
        {
            Host = "",
            Port = 443,
            ApplicationProtocols = protocolList,
            ServerCertificateValidationCallback = (_, _, _, _) => true
        };

        var provider = new QuicClientProvider(options);

        try
        {
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                provider.GetStreamAsync(TestContext.Current.CancellationToken));
        }
        finally
        {
            await provider.DisposeAsync();
        }
    }

    [Fact(Timeout = 5000)]
    public async Task EnsureConnectedAsync_should_throw_on_null_host()
    {
        var protocolList = new List<SslApplicationProtocol> { LoopbackQuicServer.Alpn };
        var options = new QuicTransportOptions
        {
            Host = null!,
            Port = 443,
            ApplicationProtocols = protocolList,
            ServerCertificateValidationCallback = (_, _, _, _) => true
        };

        var provider = new QuicClientProvider(options);

        try
        {
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                provider.GetStreamAsync(TestContext.Current.CancellationToken));
        }
        finally
        {
            await provider.DisposeAsync();
        }
    }

    [Fact(Timeout = 15000)]
    public async Task DisposeAsync_should_close_connection()
    {
        if (!QuicListener.IsSupported)
        {
            return;
        }

        var server = await LoopbackQuicServer.CreateAsync();
        var acceptTask = Task.Run(async () =>
        {
            try
            {
                return await server.AcceptConnectionAsync(TestContext.Current.CancellationToken);
            }
            catch
            {
                return null;
            }
        });

        var options = new QuicTransportOptions
        {
            Host = "localhost",
            Port = (ushort)server.Port,
            ApplicationProtocols = [LoopbackQuicServer.Alpn],
            ServerCertificateValidationCallback = (_, _, _, _) => true
        };

        var provider = new QuicClientProvider(options);

        try
        {
            var stream = await GetStreamOrSkipAsync(provider, TestContext.Current.CancellationToken);
            Assert.NotNull(stream);
            await stream.DisposeAsync();

            await provider.DisposeAsync();

            await Assert.ThrowsAsync<ObjectDisposedException>(() =>
                provider.GetStreamAsync(TestContext.Current.CancellationToken));
        }
        finally
        {
            await provider.DisposeAsync();
            await server.DisposeAsync();
            var serverConn = await acceptTask;
            if (serverConn is not null)
            {
                await serverConn.DisposeAsync();
            }
        }
    }

    [Fact(Timeout = 15000)]
    public async Task DisposeAsync_should_be_idempotent()
    {
        if (!QuicListener.IsSupported)
        {
            return;
        }

        var server = await LoopbackQuicServer.CreateAsync();
        var acceptTask = Task.Run(async () =>
        {
            try
            {
                return await server.AcceptConnectionAsync(TestContext.Current.CancellationToken);
            }
            catch
            {
                return null;
            }
        });

        var options = new QuicTransportOptions
        {
            Host = "localhost",
            Port = (ushort)server.Port,
            ApplicationProtocols = [LoopbackQuicServer.Alpn],
            ServerCertificateValidationCallback = (_, _, _, _) => true
        };

        var provider = new QuicClientProvider(options);

        try
        {
            await GetStreamOrSkipAsync(provider, TestContext.Current.CancellationToken);

            await provider.DisposeAsync();
            await provider.DisposeAsync();
            await provider.DisposeAsync();
        }
        finally
        {
            await server.DisposeAsync();
            var serverConn = await acceptTask;
            if (serverConn is not null)
            {
                await serverConn.DisposeAsync();
            }
        }
    }

    [Fact(Timeout = 15000)]
    public async Task LocalEndPoint_should_be_set_after_connection()
    {
        if (!QuicListener.IsSupported)
        {
            return;
        }

        var server = await LoopbackQuicServer.CreateAsync();
        var acceptTask = Task.Run(async () =>
        {
            try
            {
                return await server.AcceptConnectionAsync(TestContext.Current.CancellationToken);
            }
            catch
            {
                return null;
            }
        });

        var options = new QuicTransportOptions
        {
            Host = "localhost",
            Port = (ushort)server.Port,
            ApplicationProtocols = [LoopbackQuicServer.Alpn],
            ServerCertificateValidationCallback = (_, _, _, _) => true
        };

        var provider = new QuicClientProvider(options);

        try
        {
            Assert.Null(provider.LocalEndPoint);

            await GetStreamOrSkipAsync(provider, TestContext.Current.CancellationToken);

            Assert.NotNull(provider.LocalEndPoint);
        }
        finally
        {
            await provider.DisposeAsync();
            await server.DisposeAsync();
            var serverConn = await acceptTask;
            if (serverConn is not null)
            {
                await serverConn.DisposeAsync();
            }
        }
    }

    [Fact(Timeout = 15000)]
    public async Task ConnectAsync_should_establish_connection_on_demand()
    {
        if (!QuicListener.IsSupported)
        {
            return;
        }

        var server = await LoopbackQuicServer.CreateAsync();
        var acceptTask = Task.Run(async () =>
        {
            try
            {
                return await server.AcceptConnectionAsync(TestContext.Current.CancellationToken);
            }
            catch
            {
                return null;
            }
        });

        var options = new QuicTransportOptions
        {
            Host = "localhost",
            Port = (ushort)server.Port,
            ApplicationProtocols = [LoopbackQuicServer.Alpn],
            ServerCertificateValidationCallback = (_, _, _, _) => true
        };

        var provider = new QuicClientProvider(options);

        try
        {
            Assert.Null(provider.LocalEndPoint);

            await ConnectOrSkipAsync(provider, TestContext.Current.CancellationToken);

            Assert.NotNull(provider.LocalEndPoint);
        }
        finally
        {
            await provider.DisposeAsync();
            await server.DisposeAsync();
            var serverConn = await acceptTask;
            if (serverConn is not null)
            {
                await serverConn.DisposeAsync();
            }
        }
    }

    [Fact(Timeout = 15000)]
    public async Task GetStreamAsync_should_handle_concurrent_requests()
    {
        if (!QuicListener.IsSupported)
        {
            return;
        }

        var server = await LoopbackQuicServer.CreateAsync();
        var acceptTask = Task.Run(async () =>
        {
            try
            {
                var conn = await server.AcceptConnectionAsync(TestContext.Current.CancellationToken);
                for (var i = 0; i < 5; i++)
                {
                    var stream = await conn.AcceptInboundStreamAsync(TestContext.Current.CancellationToken);
                    await stream.WriteAsync(new[] { (byte)i }, TestContext.Current.CancellationToken);
                    stream.CompleteWrites();
                }

                return conn;
            }
            catch
            {
                return null;
            }
        });

        var options = new QuicTransportOptions
        {
            Host = "localhost",
            Port = (ushort)server.Port,
            ApplicationProtocols = [LoopbackQuicServer.Alpn],
            ServerCertificateValidationCallback = (_, _, _, _) => true
        };

        var provider = new QuicClientProvider(options);

        try
        {
            var first = await GetStreamOrSkipAsync(provider, TestContext.Current.CancellationToken);
            var tasks = Enumerable.Range(0, 4)
                .Select(async _ => await provider.GetStreamAsync(TestContext.Current.CancellationToken))
                .ToList();

            var streams = new[] { first }.Concat(await Task.WhenAll(tasks)).ToArray();

            Assert.Equal(5, streams.Length);
            foreach (var stream in streams)
            {
                Assert.NotNull(stream);
                await stream.DisposeAsync();
            }
        }
        finally
        {
            await provider.DisposeAsync();
            await server.DisposeAsync();
            var serverConn = await acceptTask;
            if (serverConn is not null)
            {
                await serverConn.DisposeAsync();
            }
        }
    }
}