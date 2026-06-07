using System.Buffers;
using System.IO.Pipelines;
using System.Net.Security;
using System.Net.Sockets;
using Akka.Actor;
using static Servus.Senf;

namespace Servus.Akka.Transport.Tcp.Listener;

internal sealed class TcpServerStateMachine(
    IConnectionOperations ops,
    IActorRef self,
    Stream stream,
    ConnectionInfo connectionInfo,
    SslStream? sslStream = null,
    bool allowDelayedNegotiation = false,
    SocketPipeConnectionOptions? pipeOptions = null,
    Socket? socket = null)
{
    private SocketPipeConnection? _connection;
    private SequencePosition? _pendingAdvance;
    private int _connectionGen;
    private bool _upstreamFinished;

    public void Start()
    {
        _connectionGen++;
        _connection = socket is not null && sslStream is null
            ? SocketPipeConnection.Create(socket, pipeOptions)
            : SocketPipeConnection.Create(stream, pipeOptions);

        if (sslStream is not null || allowDelayedNegotiation)
        {
            var baseSecurity = connectionInfo.Security;
            var security = baseSecurity is not null
                ? baseSecurity with { SslStream = sslStream, AllowDelayedNegotiation = allowDelayedNegotiation }
                : new SecurityInfo(default, default, SslStream: sslStream,
                    AllowDelayedNegotiation: allowDelayedNegotiation);
            ops.OnPushInbound(new TransportConnected(connectionInfo with { Security = security }));
        }
        else
        {
            ops.OnPushInbound(new TransportConnected(connectionInfo));
        }
    }

    internal void Dispatch(ITcpTransportEvent evt)
    {
        switch (evt)
        {
            case PipeReadComplete e:
                if (e.Gen == _connectionGen)
                {
                    OnPipeReadComplete(e.Result);
                }

                break;
            case PipeReadFailed e:
                if (e.Gen == _connectionGen)
                {
                    OnPipeReadFailed(e.Error);
                }

                break;
            case PipeFlushComplete e:
                if (e.Gen == _connectionGen)
                {
                    ops.OnSignalPullOutbound();
                }

                break;
        }
    }

    public void HandlePush(ITransportOutbound item)
    {
        switch (item)
        {
            case TransportData data:
                HandleTransportData(data);
                break;
            case DisconnectTransport:
                Cleanup();
                ops.OnCompleteStage();
                break;
            default:
                ops.OnSignalPullOutbound();
                break;
        }
    }

    public void HandleUpstreamFinish()
    {
        _upstreamFinished = true;
        Cleanup();
        ops.OnCompleteStage();
    }

    public void HandleDownstreamFinish()
    {
        Cleanup();
    }

    public void PostStop()
    {
        Cleanup();
    }

    public void RequestRead()
    {
        if (_connection is null)
        {
            return;
        }

        if (_pendingAdvance is { } pos)
        {
            _pendingAdvance = null;
            _connection.InputReader.AdvanceTo(pos);
        }

        var gen = _connectionGen;
        _connection.InputReader.ReadAsync().PipeTo(self,
            success: result => new PipeReadComplete(result, gen),
            failure: ex => new PipeReadFailed(ex, gen));
    }

    private void HandleTransportData(TransportData data)
    {
        if (_connection is null)
        {
            data.Buffer.Dispose();
            ops.OnSignalPullOutbound();
            return;
        }

        var mem = _connection.OutputWriter.GetMemory(data.Buffer.Length);
        data.Buffer.Memory.Span.CopyTo(mem.Span);
        _connection.OutputWriter.Advance(data.Buffer.Length);
        data.Buffer.Dispose();

        var gen = _connectionGen;
        var flush = _connection.OutputWriter.FlushAsync();

        if (flush.IsCompleted)
        {
            ops.OnSignalPullOutbound();
        }
        else
        {
            flush.PipeTo(self,
                success: _ => new PipeFlushComplete(gen),
                failure: _ => new PipeFlushComplete(gen));
        }
    }

    private void OnPipeReadComplete(ReadResult result)
    {
        if (result.Buffer.Length > 0)
        {
            var length = (int)result.Buffer.Length;
            var buf = TransportBuffer.Rent(length);
            result.Buffer.CopyTo(buf.FullMemory.Span);
            buf.Length = length;
            _pendingAdvance = result.Buffer.End;
            ops.OnPushInbound(new TransportData(buf));
        }

        if (result.IsCompleted || result.IsCanceled)
        {
            OnInboundComplete(DisconnectReason.Graceful);
        }
    }

    private void OnPipeReadFailed(Exception ex)
    {
        Tracing.For("Connection").Warning(this, "Server pipe read failed: {0}", ex.Message);
        OnInboundComplete(DisconnectReason.Error);
    }

    private void OnInboundComplete(DisconnectReason reason)
    {
        ops.OnPushInbound(new TransportDisconnected(reason));
        DisposeConnection();
        _connection = null;

        if (_upstreamFinished)
        {
            ops.OnCompleteStage();
        }
        else
        {
            ops.OnSignalPullOutbound();
        }
    }

    private void Cleanup()
    {
        _connectionGen++;
        DisposeConnection();
        _connection = null;
        stream.Dispose();
    }

    private void DisposeConnection()
    {
        if (_connection is not null)
        {
            _ = _connection.DisposeAsync();
        }
    }
}