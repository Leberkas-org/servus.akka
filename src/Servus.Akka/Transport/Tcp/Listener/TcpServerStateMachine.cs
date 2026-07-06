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
    private const int MaxSyncReads = 8;

    private SocketPipeConnection? _connection;
    private int _connectionGen;
    private bool _upstreamFinished;
    private int _syncReadBudget = MaxSyncReads;

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
            case ReadCompleted e:
                if (e.Gen == _connectionGen)
                {
                    OnReadCompleted(e.Buffer);
                }
                else
                {
                    // Stale read from a torn-down connection: the buffer is OWNED by this event
                    // (rent-and-receive) — dropping it without dispose leaks the pooled array.
                    e.Buffer?.Dispose();
                }

                break;
            case ReadFailed e:
                if (e.Gen == _connectionGen)
                {
                    OnReadFailed(e.Error);
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

        var gen = _connectionGen;
        var readTask = _connection.ReceiveAsync();

        if (readTask.IsCompletedSuccessfully && _syncReadBudget > 0)
        {
            _syncReadBudget--;
            OnReadCompleted(readTask.Result);
            return;
        }

        _syncReadBudget = MaxSyncReads;
        readTask.PipeTo(self,
            success: buffer => new ReadCompleted(buffer, gen),
            failure: ex => new ReadFailed(ex, gen));
    }

    private void HandleTransportData(TransportData data)
    {
        if (_connection is null)
        {
            data.Buffer.Dispose();
            data.Return();
            ops.OnSignalPullOutbound();
            return;
        }

        var mem = _connection.OutputWriter.GetMemory(data.Buffer.Length);
        data.Buffer.Memory.Span.CopyTo(mem.Span);
        _connection.OutputWriter.Advance(data.Buffer.Length);
        data.Buffer.Dispose();
        data.Return();

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

    private void OnReadCompleted(WireBuffer? buffer)
    {
        if (buffer is null)
        {
            OnInboundComplete(DisconnectReason.Graceful);
            return;
        }

        ops.OnPushInbound(TransportData.Rent(buffer));
    }

    private void OnReadFailed(Exception ex)
    {
        Tracing.For("Connection").Warning(this, "Server read failed: {0}", ex.Message);
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