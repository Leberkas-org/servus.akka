using Akka.Actor;
using Akka.Hosting;
using Akka.Hosting.TestKit;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Servus.Akka.DependencyInjection;

namespace Servus.Akka.Tests;

public class ActorRefInjectionTests : TestKit
{
    public class ResolvingTestActor : ReceiveActor
    {
        public ResolvingTestActor()
        {
            ReceiveAny(msg =>
            {
                Sender.Tell("hello");
            });
        }
    }

    [Fact]
    public void Test1()
    {
        var a = Host.Services.GetService<ActorRef<ResolvingTestActor>>();
        a.Tell("hello");
        ExpectMsg("hello", cancellationToken: TestContext.Current.CancellationToken);
    }

    protected override void ConfigureHostBuilder(IHostBuilder builder)
    {
        builder.UseServiceProviderFactory(new ActorRefProviderFactory());
        base.ConfigureHostBuilder(builder);
    }

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        builder.WithResolvableActor<ResolvingTestActor>();
    }
}