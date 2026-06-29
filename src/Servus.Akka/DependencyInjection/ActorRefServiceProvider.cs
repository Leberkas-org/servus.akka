using System.Reflection;
using Akka.Actor;
using Akka.Hosting;
using Microsoft.Extensions.DependencyInjection;

namespace Servus.Akka.DependencyInjection;

public class ActorRefServiceProvider(IServiceProvider inner) : IServiceProvider
{
    private readonly Dictionary<Type, ConstructorInfo> _constructorInfos = [];

    public object GetService(Type serviceType)
    {
        var service = inner.GetService(serviceType);
        if (service != null) return service;

        // Custom fallback logic
        if (!serviceType.IsAssignableTo(typeof(IActorRef)) && serviceType.GenericTypeArguments.Length > 0) return null!;
        var registry = inner.GetRequiredService<IActorRegistry>();

        return Create(serviceType, registry);
    }

    private object Create(Type type, IActorRegistry registry)
    {
        // check if constructor is already in cache
        if (_constructorInfos.TryGetValue(type, out var constructor)) return constructor.Invoke([registry]);

        // create default ctor
        var genericType = typeof(ActorRef<>).MakeGenericType(type.GenericTypeArguments.First());
        constructor = genericType.GetConstructor([typeof(IActorRegistry)]);
        if (constructor is null) return null!;

        // add to cache
        _constructorInfos[type] = constructor;
        return constructor.Invoke([registry]);
    }
}