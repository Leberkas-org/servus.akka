using Akka.Actor;
using Akka.Hosting;
using Akka.Hosting.TestKit;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Servus.Akka.DependencyInjection;

namespace Servus.Akka.Tests;

public interface INotRegistered;

public sealed class KeyedThing;

public sealed class DisposableThing : IDisposable
{
    public bool Disposed { get; private set; }
    public void Dispose() => Disposed = true;
}

public class ActorRefServiceProviderTests : TestKit
{
    public class PongActor : ReceiveActor
    {
        public PongActor() => ReceiveAny(_ => Sender.Tell("pong"));
    }

    protected override void ConfigureHostBuilder(IHostBuilder builder)
    {
        builder.UseServiceProviderFactory(new ActorRefProviderFactory());
        base.ConfigureHostBuilder(builder);
    }

    protected override void ConfigureServices(HostBuilderContext context, IServiceCollection services)
    {
        services.AddKeyedSingleton<KeyedThing>("k");
        base.ConfigureServices(context, services);
    }

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        builder.WithResolvableActor<PongActor>();
    }

    [Fact]
    public void Unregistered_types_resolve_to_null()
    {
        Assert.Null(Host.Services.GetService<INotRegistered>());
        Assert.Null(Host.Services.GetService<List<string>>());
    }

    [Fact]
    public void Actor_refs_resolve_inside_scopes()
    {
        using var scope = Host.Services.CreateScope();
        var pong = scope.ServiceProvider.GetRequiredService<ActorRef<PongActor>>();

        pong.Tell("ping", TestActor);

        ExpectMsg("pong", cancellationToken: TestContext.Current.CancellationToken);
    }

    [Fact]
    public void Actor_refs_count_as_services()
    {
        var isService = Host.Services.GetRequiredService<IServiceProviderIsService>();

        Assert.True(isService.IsService(typeof(ActorRef<PongActor>)));
        Assert.True(isService.IsService(typeof(IActorRegistry)));
        Assert.False(isService.IsService(typeof(INotRegistered)));
    }

    [Fact]
    public void Keyed_services_resolve_through_the_wrapper()
    {
        Assert.NotNull(Host.Services.GetKeyedService<KeyedThing>("k"));
    }

    [Fact]
    public async Task Actor_refs_resolve_from_many_threads()
    {
        var refs = await Task.WhenAll(Enumerable.Range(0, 64).Select(_ =>
            Task.Run(() => Host.Services.GetRequiredService<ActorRef<PongActor>>())));

        Assert.All(refs, r => Assert.Equal(refs[0].Path, r.Path));
    }

    [Fact]
    public void Disposing_the_provider_disposes_its_singletons()
    {
        var services = new ServiceCollection().AddSingleton<DisposableThing>();
        var provider = (ActorRefServiceProvider)new ActorRefProviderFactory().CreateServiceProvider(services);
        var thing = provider.GetRequiredService<DisposableThing>();

        provider.Dispose();

        Assert.True(thing.Disposed);
    }
}
