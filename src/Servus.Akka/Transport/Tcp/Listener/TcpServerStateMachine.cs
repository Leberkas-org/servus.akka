using Akka.Actor;
using static Servus.Senf;

namespace Servus.Akka.Transport.Tcp.Listener;

internal sealed class TcpServerStateMachine(
    IConnectionOperations ops,
    IActorRef self,
    IDuplexConnection connection,
    ConnectionInfo connectionInfo,
    TransportConnectionOptions options)
{
    private readonly int _maxSyncReads = options.MaxSyncReads;

    private IDuplexConnection? _connection;
    private int _connectionGen;
    private bool _upstreamFinished;
    private int _syncReadBudget = options.MaxSyncReads;

    private long _bytesInFlight;
    private readonly long _highWatermark = options.OutputHighWatermark;
    private readonly long _lowWatermark = options.OutputLowWatermark;

    private bool _readInProgress;
    private ReadEventState _readState = new(0);

    public void Start()
    {
        _connectionGen++;
        var gen = _connectionGen;

        _connection = connection;
        _readState = new ReadEventState(gen);
        _connection.OnFlushed = bytes => self.Tell(new SendFlushed(bytes, gen));

        ops.OnPushInbound(new TransportConnected(connectionInfo));
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

        _syncReadBudget = _maxSyncReads;
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
    }

    private void DisposeConnection()
    {
        if (_connection is not null)
        {
            _ = _connection.DisposeAsync();
        }
    }
}
