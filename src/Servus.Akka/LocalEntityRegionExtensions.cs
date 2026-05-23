using Akka.Actor;
using Akka.Hosting;
using Servus.Akka.Local;

namespace Servus.Akka;

public static class LocalEntityRegionExtensions
{
    public static AkkaConfigurationBuilder WithLocalEntityRegion<TKey>(
        this AkkaConfigurationBuilder builder,
        string typeName,
        Func<string, Props> entityPropsFactory)
    {
        return builder.WithLocalEntityRegion<TKey>(
            typeName, entityPropsFactory, new EntityIdExtractor());
    }

    public static AkkaConfigurationBuilder WithLocalEntityRegion<TKey>(
        this AkkaConfigurationBuilder builder,
        string typeName,
        Func<string, Props> entityPropsFactory,
        IEntityIdExtractor messageExtractor)
    {
        return builder.WithLocalEntityRegion<TKey>(
            typeName, entityPropsFactory, messageExtractor, new LocalEntityRegionOptions());
    }

    public static AkkaConfigurationBuilder WithLocalEntityRegion<TKey>(
        this AkkaConfigurationBuilder builder,
        string typeName,
        Func<string, Props> entityPropsFactory,
        IEntityIdExtractor messageExtractor,
        LocalEntityRegionOptions options)
    {
        return builder.WithActors((system, registry) =>
        {
            var props = Props.Create(() =>
                new LocalEntityRegionActor(entityPropsFactory, messageExtractor, options));

            var regionRef = system.ActorOf(props, typeName);
            registry.Register<TKey>(regionRef);
        });
    }

    public static AkkaConfigurationBuilder WithLocalEntityRegion<TKey>(
        this AkkaConfigurationBuilder builder,
        string typeName,
        Func<ActorSystem, IActorRegistry, Func<string, Props>> entityPropsFactory,
        IEntityIdExtractor messageExtractor,
        LocalEntityRegionOptions options)
    {
        return builder.WithActors((system, registry) =>
        {
            var propsFactory = entityPropsFactory(system, registry);
            var props = Props.Create(() =>
                new LocalEntityRegionActor(propsFactory, messageExtractor, options));

            var regionRef = system.ActorOf(props, typeName);
            registry.Register<TKey>(regionRef);
        });
    }
}
