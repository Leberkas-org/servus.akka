using Microsoft.Extensions.Hosting;
using Servus.Akka.DependencyInjection;
using Servus.Application.Startup;

namespace Servus.Akka.Startup;

public class ActorRefProviderStartupContainer : IHostBuilderSetupContainer
{
    public void ConfigureHostBuilder(IHostBuilder builder)
    {
        builder.UseServiceProviderFactory(new ActorRefProviderFactory());
    }
}