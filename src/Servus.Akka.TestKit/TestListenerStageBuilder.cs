namespace Servus.Akka.TestKit;

public sealed class TestListenerStageBuilder
{
    private Action<TestConnectionStageBuilder>? _defaultFactory;
    private Func<int, TestConnectionStage?>? _onAccept;

    public TestListenerStageBuilder WithDefaultConnection(Action<TestConnectionStageBuilder> configure)
    {
        _defaultFactory = configure;
        return this;
    }

    public TestListenerStageBuilder OnAccept(Func<int, TestConnectionStage?> factory)
    {
        _onAccept = factory;
        return this;
    }

    public TestListenerStage Build()
    {
        return new TestListenerStage(_defaultFactory, _onAccept);
    }
}
