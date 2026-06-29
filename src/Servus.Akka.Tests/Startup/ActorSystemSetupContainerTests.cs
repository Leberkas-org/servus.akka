using Akka.Actor;
using Akka.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Servus.Akka.Startup;
using Servus.Application.Startup;

namespace Servus.Akka.Tests.Startup;

public class ActorSystemSetupContainerTests
{
    [Fact(Timeout = 30000)]
    public async Task SetupServices_CallsAddAkkaWithBuildSystemAction()
    {
        ActorSystem? capturesSystem = null;
        var semaphore = new SemaphoreSlim(0, 1);

        var cts = new CancellationTokenSource();
        var app = AppBuilder
            .Create()
            .WithSetup<TestActorSystemSetupContainer>()
            .OnApplicationStarted(sp =>
            {
                capturesSystem = sp.GetRequiredService<ActorSystem>();
                semaphore.Release();
            })
            .Build();

        await app.StartAsync(cts.Token);
        await semaphore.WaitAsync(cts.Token);

        Assert.NotNull(capturesSystem);
        Assert.Equal("test-system", capturesSystem.Name);
    }
}

// Test implementation of the abstract class
public class TestActorSystemSetupContainer : ActorSystemSetupContainer
{

    protected override string GetActorSystemName() => "test-system";

    protected override void BuildSystem(AkkaConfigurationBuilder builder, IServiceProvider serviceProvider)
    {
        // nop
    }
}