using Akka.Actor;
using Akka.Hosting;
using Akka.Hosting.TestKit;

namespace Servus.Akka.Tests;

public class ResolveExtensionTests : TestKit
{
    public class TestChildActor : ReceiveActor
    {
        public TestChildActor()
        {
            Receive<string>(msg => Sender.Tell($"echo:{msg}"));
        }
    }

    public class TestParentActor : ReceiveActor
    {
        public TestParentActor()
        {
            var child = Context.ResolveChildActor<TestChildActor>("configured-child",
                props => props.WithDispatcher("akka.actor.default-dispatcher"));

            Receive<string>(msg => child.Forward(msg));
        }
    }

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        builder.WithResolvableActor<RegisterExtensionTests.TestActor1>();
    }

    [Fact]
    public void ResolveTest()
    {
        var actorRef = Sys.ResolveActor<RegisterExtensionTests.TestActor1>();
        Assert.NotEqual(Nobody.Instance, actorRef);
    }

    [Fact]
    public void ResolveActor_WithConfigure_AppliesPropsTransformation()
    {
        var actorRef = Sys.ResolveActor<TestChildActor>(configure: props => props.WithDispatcher("akka.actor.default-dispatcher"));

        Assert.NotEqual(Nobody.Instance, actorRef);
    }

    [Fact]
    public async Task ResolveChildActor_WithConfigure_MessagesReachTheConfiguredChild()
    {
        var parent = Sys.ResolveActor<TestParentActor>();

        var reply = await parent.Ask<string>("ping", TestContext.Current.CancellationToken);

        Assert.Equal("echo:ping", reply);
    }
}
