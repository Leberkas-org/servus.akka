using Servus.Akka.Transport;

namespace Servus.Akka.TestKit;

public sealed class TestConnectionStageBuilder
{
    private readonly List<TestConnectionStage.OutboundHandler> _handlers = [];
    private ActivityLog? _activityLog;
    private ConnectionInfo? _autoConnectInfo;
    private bool _autoConnect;

    public TestConnectionStageBuilder AutoConnect(ConnectionInfo? info = null)
    {
        _autoConnect = true;
        _autoConnectInfo = info ?? ConnectionInfo.None;
        return this;
    }

    public TestConnectionStageBuilder AutoDisconnect()
    {
        return OnOutbound<DisconnectTransport>((msg, ctx)
            => ctx.Push(new TransportDisconnected(msg.Reason)));
    }

    public TestConnectionStageBuilder OnOutbound<T>(Action<T, IStageContext> handler)
        where T : ITransportOutbound
    {
        _handlers.Add(new TestConnectionStage.OutboundHandler(
            typeof(T),
            (msg, ctx) => handler((T)msg, ctx)));
        return this;
    }

    public TestConnectionStageBuilder WithActivityLog(ActivityLog log)
    {
        _activityLog = log;
        return this;
    }

    public TestConnectionStage Build()
    {
        var stage = new TestConnectionStage([.. _handlers], _activityLog);

        if (_autoConnect)
        {
            stage.EnqueueInitial(new TransportConnected(_autoConnectInfo!));
        }

        return stage;
    }
}
