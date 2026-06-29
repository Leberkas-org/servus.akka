
using Akka.Actor;
using Akka.Hosting;
using Akka.Hosting.TestKit;

namespace Servus.Akka.Tests;

public class ResolveExtensionTests : TestKit
{
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
}