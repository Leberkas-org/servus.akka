using Akka;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Stage;
using Servus.Akka.Transport;

namespace Servus.Akka.TestKit;

public sealed class TestListenerStage
    : GraphStage<SourceShape<Flow<ITransportOutbound, ITransportInbound, NotUsed>>>
{
    private readonly Action<TestConnectionStageBuilder>? _defaultFactory;
    private readonly Func<int, TestConnectionStage?>? _onAccept;
    private readonly List<TestConnectionStage> _acceptedConnections = [];
    private int _acceptIndex;

    private readonly Outlet<Flow<ITransportOutbound, ITransportInbound, NotUsed>> _out =
        new("TestListener.Out");

    public override SourceShape<Flow<ITransportOutbound, ITransportInbound, NotUsed>> Shape { get; }

    public ActivityLog ActivityLog { get; } = new();

    public IReadOnlyList<Activity> Activities => ActivityLog.Entries;

    public IReadOnlyList<TestConnectionStage> AcceptedConnections => _acceptedConnections;

    internal TestListenerStage(
        Action<TestConnectionStageBuilder>? defaultFactory,
        Func<int, TestConnectionStage?>? onAccept)
    {
        _defaultFactory = defaultFactory;
        _onAccept = onAccept;
        Shape = new SourceShape<Flow<ITransportOutbound, ITransportInbound, NotUsed>>(_out);
    }

    public TestConnectionStage GetConnection(int index) => _acceptedConnections[index];

    public Source<Flow<ITransportOutbound, ITransportInbound, NotUsed>, NotUsed> AsSource()
        => Source.FromGraph(this);

    public static implicit operator
        Source<Flow<ITransportOutbound, ITransportInbound, NotUsed>, NotUsed>(TestListenerStage stage)
        => stage.AsSource();

    protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes)
        => new Logic(this);

    private TestConnectionStage ResolveConnection()
    {
        var index = _acceptIndex++;
        var fromFactory = false;

        TestConnectionStage? connection = null;

        if (_onAccept is not null)
        {
            connection = _onAccept(index);
            fromFactory = connection is not null;
        }

        connection ??= BuildDefault();

        _acceptedConnections.Add(connection);
        ActivityLog.Record(new ListenerConnectionAccepted(index, fromFactory));

        return connection;
    }

    private TestConnectionStage BuildDefault()
    {
        var builder = new TestConnectionStageBuilder();

        if (_defaultFactory is not null)
        {
            _defaultFactory(builder);
        }
        else
        {
            builder.AutoConnect();
        }

        return builder.Build();
    }

    private sealed class Logic : GraphStageLogic
    {
        private readonly TestListenerStage _stage;

        public Logic(TestListenerStage stage) : base(stage.Shape)
        {
            _stage = stage;

            SetHandler(stage._out, onPull: () =>
            {
                var connection = _stage.ResolveConnection();
                Push(_stage._out, connection.AsFlow());
            });
        }
    }
}
