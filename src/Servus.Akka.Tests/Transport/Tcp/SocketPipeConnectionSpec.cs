using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using Servus.Akka.Transport.Tcp;

namespace Servus.Akka.Tests.Transport.Tcp;

public sealed class SocketPipeConnectionSpec : IAsyncLifetime
{
    private Socket _listener = null!;
    private Socket _client = null!;
    private Socket _server = null!;

    public async ValueTask InitializeAsync()
    {
        _listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        _listener.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        _listener.Listen(1);

        var endpoint = (IPEndPoint)_listener.LocalEndPoint!;
        _client = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        await _client.ConnectAsync(endpoint);
        _server = await _listener.AcceptAsync();
    }

    public ValueTask DisposeAsync()
    {
        _server?.Dispose();
        _client?.Dispose();
        _listener?.Dispose();
        return ValueTask.CompletedTask;
    }

    [Fact(Timeout = 5000)]
    public async Task InputReader_should_receive_socket_data()
    {
        await using var connection = SocketPipeConnection.Create(_client);

        var sent = System.Text.Encoding.UTF8.GetBytes("hello from server");
        await _server.SendAsync(sent, SocketFlags.None);

        var result = await connection.InputReader.ReadAsync();
        var received = System.Text.Encoding.UTF8.GetString(result.Buffer.FirstSpan);
        connection.InputReader.AdvanceTo(result.Buffer.End);

        Assert.Equal("hello from server", received);
    }

    [Fact(Timeout = 5000)]
    public async Task OutputWriter_should_send_data_to_socket()
    {
        await using var connection = SocketPipeConnection.Create(_client);

        var data = System.Text.Encoding.UTF8.GetBytes("hello from client");
        await connection.OutputWriter.WriteAsync(data);

        var buffer = new byte[1024];
        var received = await _server.ReceiveAsync(buffer, SocketFlags.None);

        Assert.Equal("hello from client", System.Text.Encoding.UTF8.GetString(buffer, 0, received));
    }

    [Fact(Timeout = 5000)]
    public async Task InputReader_should_complete_on_socket_close()
    {
        await using var connection = SocketPipeConnection.Create(_client);

        _server.Shutdown(SocketShutdown.Send);

        var result = await connection.InputReader.ReadAsync();

        Assert.True(result.IsCompleted);
        Assert.True(result.Buffer.IsEmpty);
        connection.InputReader.AdvanceTo(result.Buffer.End);
    }

    [Fact(Timeout = 10000)]
    public async Task Backpressure_should_pause_socket_reads_when_pipe_full()
    {
        var options = new SocketPipeConnectionOptions
        {
            InputPauseWriterThreshold = 1024,
            InputResumeWriterThreshold = 512,
            WaitForData = false
        };

        await using var connection = SocketPipeConnection.Create(_client, options);

        // Send 2KB which exceeds the 1KB pause threshold
        var payload = new byte[2 * 1024];
        Array.Fill(payload, (byte)'X');
        await _server.SendAsync(payload, SocketFlags.None);

        // Give the receive loop time to read and hit backpressure
        await Task.Delay(200);

        // Read data from the pipe to verify it arrived
        var totalRead = 0L;
        while (totalRead < payload.Length)
        {
            var result = await connection.InputReader.ReadAsync();
            totalRead += result.Buffer.Length;
            connection.InputReader.AdvanceTo(result.Buffer.End);
        }

        Assert.Equal(payload.Length, totalRead);
    }

    [Fact(Timeout = 5000)]
    public async Task Create_with_stream_should_work_for_tls()
    {
        var stream = new NetworkStream(_client, ownsSocket: false);
        await using var connection = SocketPipeConnection.Create(stream);

        var sent = System.Text.Encoding.UTF8.GetBytes("stream data");
        await _server.SendAsync(sent, SocketFlags.None);

        var result = await connection.InputReader.ReadAsync();
        var received = System.Text.Encoding.UTF8.GetString(result.Buffer.FirstSpan);
        connection.InputReader.AdvanceTo(result.Buffer.End);

        Assert.Equal("stream data", received);

        // Verify output direction works too
        var outData = System.Text.Encoding.UTF8.GetBytes("stream reply");
        await connection.OutputWriter.WriteAsync(outData);

        var buffer = new byte[1024];
        var bytesReceived = await _server.ReceiveAsync(buffer, SocketFlags.None);
        Assert.Equal("stream reply", System.Text.Encoding.UTF8.GetString(buffer, 0, bytesReceived));
    }
}
