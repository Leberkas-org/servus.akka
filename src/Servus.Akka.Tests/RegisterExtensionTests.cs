using Akka.Actor;
using Akka.Hosting;
using Akka.Hosting.TestKit;
using Microsoft.Extensions.DependencyInjection;

namespace Servus.Akka.Tests;

public class RegisterExtensionTests : TestKit
{
    public class TestActor1 : ReceiveActor
    {
    }
    public class TestActor2 : ReceiveActor
    {
    }
    public class TestActor3 : ReceiveActor
    {
    }

    [Fact]
    public void ActorsAreRegistered()
    {
        var registry = Host.Services.GetRequiredService<IActorRegistry>();

        Assert.True(registry.TryGet<TestActor1>(out _));
        Assert.True(registry.TryGet<TestActor2>(out _));
        Assert.True(registry.TryGet<TestActor3>(out _));
    }

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        builder
            .WithResolvableActors(helper =>
            {
                helper
                    .Register<TestActor1>()
                    .Register<TestActor2>();
            })
            .WithResolvableActor<TestActor3>();
    }
}