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
    TransportConnectionOptions? connectionOptions = null,
    Socket? socket = null,
    Func<IDuplexConnection>? connectionFactory = null)
{
    private const int MaxSyncReads = 8;

    private IDuplexConnection? _connection;
    private int _connectionGen;
    private bool _upstreamFinished;
    private int _syncReadBudget = MaxSyncReads;

    // Outbound bytes handed to the connection's send queue that have not yet been reported flushed.
    // Drives watermark backpressure: upstream is pulled only while this stays below the high mark.
    private long _bytesInFlight;
    private long _highWatermark = new TransportConnectionOptions().OutputHighWatermark;
    private long _lowWatermark = new TransportConnectionOptions().OutputLowWatermark;

    private bool _readInProgress;
    private ReadEventState _readState = new(0);

    /// <summary>TEST-ONLY. The cached read transforms for the current generation.</summary>
    internal ReadEventState ReadState => _readState;

    public void Start()
    {
        _connectionGen++;
        var gen = _connectionGen;
        var options = connectionOptions ?? new TransportConnectionOptions();
        _highWatermark = options.OutputHighWatermark;
        _lowWatermark = options.OutputLowWatermark;

        _connection = connectionFactory is not null
            ? connectionFactory()
            : socket is not null && sslStream is null
                ? new RawSocketConnection(socket, options)
                : new StreamConnection(stream, options);

        _readState = new ReadEventState(gen);

        // Assigned once per generation, before the first enqueue: the send loop reports each fully-sent
        // batch back to this actor. Allocation-free per batch — the closure captures only self + gen.
        _connection.OnFlushed = bytes => self.Tell(new SendFlushed(bytes, gen));

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
                    _readInProgress = false;
                    OnReadCompleted(e.Buffer);
                }
                else
                {
                    // Stale read from a torn-down connection: the buffer is OWNED by this event
                    // (rent-and-receive) — dropping it without dispose leaks the pooled array. A stale
                    // event says nothing about the CURRENT gen's read, so _readInProgress is
                    // deliberately left alone.
                    e.Buffer?.Dispose();
                }

                break;
            case ReadFailed e:
                if (e.Gen == _connectionGen)
                {
                    _readInProgress = false;
                    OnReadFailed(e.Error);
                }

                break;
            case SendFlushed e:
                if (e.Gen == _connectionGen)
                {
                    OnSendFlushed(e.Bytes);
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
        if (_connection is null || _readInProgress)
        {
            return;
        }

        _readInProgress = true;

        var readTask = _connection.ReceiveAsync();

        if (readTask.IsCompletedSuccessfully && _syncReadBudget > 0)
        {
            _syncReadBudget--;
            _readInProgress = false;
            OnReadCompleted(readTask.Result);
            return;
        }

        _syncReadBudget = MaxSyncReads;
        readTask.PipeTo(self, success: _readState.ReadSuccess, failure: _readState.ReadFailure);
    }

    private void HandleTransportData(TransportData data)
    {
        var buffer = data.Buffer;
        data.Return();

        if (_connection is null)
        {
            buffer.Dispose();
            ops.OnSignalPullOutbound();
            return;
        }

        if (!_connection.TryEnqueue(buffer))
        {
            buffer.Dispose();
            OnInboundComplete(DisconnectReason.Error);
            return;
        }

        _bytesInFlight += buffer.Length;

        // Pull the next outbound item only while we are below the high watermark; otherwise pause and
        // wait for SendFlushed to drain us back below the low watermark.
        if (_bytesInFlight < _highWatermark)
        {
            ops.OnSignalPullOutbound();
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

    private void OnSendFlushed(int bytes)
    {
        var before = _bytesInFlight;
        _bytesInFlight -= bytes;

        // Resume the outbound pull only on the edge where in-flight bytes just crossed below the low
        // watermark, so a burst of small flushes doesn't fire a pull per batch.
        if (before >= _lowWatermark && _bytesInFlight < _lowWatermark)
        {
            ops.OnSignalPullOutbound();
        }
    }

    private void OnInboundComplete(DisconnectReason reason)
    {
        ops.OnPushInbound(new TransportDisconnected(reason));
        DisposeConnection();
        _connection = null;
        _bytesInFlight = 0;

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
        _readInProgress = false;
        _readState = new ReadEventState(_connectionGen);
        _bytesInFlight = 0;

        DisposeConnection();
        _connection = null;
        stream.Dispose();
    }

    private void DisposeConnection()
    {
        if (_connection is not null)
        {
            // Fire-and-forget: the connection cancels its receive internally and dispose-drains its
            // outbound channel, so no dead-letter leak survives PostStop.
            _ = _connection.DisposeAsync();
        }
    }
}
