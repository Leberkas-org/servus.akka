using Akka.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Servus.Application.Startup;

namespace Servus.Akka.Startup;

public abstract class ActorSystemSetupContainer : IServiceSetupContainer
{
    public void SetupServices(IServiceCollection services, IConfiguration configuration)
    {
        services.AddAkka(GetActorSystemName(), BuildSystem);
    }

    protected abstract string GetActorSystemName();
    
    protected abstract void BuildSystem(AkkaConfigurationBuilder builder, IServiceProvider serviceProvider);
}