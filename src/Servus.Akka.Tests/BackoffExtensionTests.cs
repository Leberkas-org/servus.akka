using Akka.Actor;

namespace Servus.Akka.Tests;

public class BackoffExtensionTests
{
    // Index of minBackoff/maxBackoff in the Props.Arguments Akka.Pattern.Backoff.OnFailure/OnStop builds.
    private const int MinBackoffArgIndex = 2;
    private const int MaxBackoffArgIndex = 3;

    public class TestActor : ReceiveActor
    {
    }

    [Fact]
    public void BackoffOnFailure_WrapsPropsInBackoffOnRestartSupervisor()
    {
        var props = Props.Create<TestActor>()
            .BackoffOnFailure("test-actor", minBackoff: TimeSpan.FromSeconds(1), maxBackoff: TimeSpan.FromSeconds(10));

        Assert.Multiple(
            () => Assert.Equal("BackoffOnRestartSupervisor", props.Type.Name),
            () => Assert.Equal(TimeSpan.FromSeconds(1), props.Arguments[MinBackoffArgIndex]),
            () => Assert.Equal(TimeSpan.FromSeconds(10), props.Arguments[MaxBackoffArgIndex]));
    }

    [Fact]
    public void BackoffOnStop_WrapsPropsInBackoffSupervisor()
    {
        var props = Props.Create<TestActor>()
            .BackoffOnStop("test-actor", minBackoff: TimeSpan.FromSeconds(2), maxBackoff: TimeSpan.FromSeconds(20));

        Assert.Multiple(
            () => Assert.Equal("BackoffSupervisor", props.Type.Name),
            () => Assert.Equal(TimeSpan.FromSeconds(2), props.Arguments[MinBackoffArgIndex]),
            () => Assert.Equal(TimeSpan.FromSeconds(20), props.Arguments[MaxBackoffArgIndex]));
    }

    [Fact]
    public void BackoffOnFailure_Configure_AppliesAkkasOwnFluentOptions()
    {
        var props = Props.Create<TestActor>()
            .BackoffOnFailure("test-actor", configure: o => o.WithMaxNrOfRetries(5));

        Assert.Equal("BackoffOnRestartSupervisor", props.Type.Name);
    }

    [Fact]
    public void BackoffOnStop_Configure_AppliesAkkasOwnFluentOptions()
    {
        var props = Props.Create<TestActor>()
            .BackoffOnStop("test-actor", configure: o => o.WithMaxNrOfRetries(5));

        Assert.Equal("BackoffSupervisor", props.Type.Name);
    }
}
