using System.Collections.Concurrent;
using System.Reflection;
using Akka.Hosting;
using Microsoft.Extensions.DependencyInjection;

namespace Servus.Akka.DependencyInjection;

/// <summary>
/// Wraps a service provider so <see cref="ActorRef{TActor}"/> resolves from the <see cref="IActorRegistry"/> without
/// being registered. Every other type resolves from the inner provider as usual (null when it is not registered).
/// Scopes created through it resolve actor refs too, so request services (controllers) can take them.
/// </summary>
public class ActorRefServiceProvider(IServiceProvider inner)
    : IKeyedServiceProvider, IServiceProviderIsService, IDisposable, IAsyncDisposable
{
    private static readonly ConcurrentDictionary<Type, ConstructorInfo> Constructors = new();

    public object? GetService(Type serviceType)
    {
        if (serviceType == typeof(IServiceScopeFactory))
            return new ScopeFactory(inner.GetRequiredService<IServiceScopeFactory>());
        if (serviceType == typeof(IServiceProvider)) return this;
        if (serviceType == typeof(IServiceProviderIsService)) return this;

        var service = inner.GetService(serviceType);
        if (service is not null || !IsActorRef(serviceType)) return service;

        var constructor = Constructors.GetOrAdd(serviceType,
            t => t.GetConstructor([typeof(IActorRegistry)])!);
        return constructor.Invoke([inner.GetRequiredService<IActorRegistry>()]);
    }

    public object? GetKeyedService(Type serviceType, object? serviceKey) =>
        (inner as IKeyedServiceProvider)?.GetKeyedService(serviceType, serviceKey);

    public object GetRequiredKeyedService(Type serviceType, object? serviceKey) =>
        inner as IKeyedServiceProvider is { } keyed
            ? keyed.GetRequiredKeyedService(serviceType, serviceKey)
            : throw new InvalidOperationException("The inner service provider does not support keyed services.");

    public bool IsService(Type serviceType) =>
        IsActorRef(serviceType)
        || inner.GetService<IServiceProviderIsService>()?.IsService(serviceType) == true;

    private static bool IsActorRef(Type type) =>
        type is { IsGenericType: true, ContainsGenericParameters: false }
        && type.GetGenericTypeDefinition() == typeof(ActorRef<>);

    public void Dispose() => (inner as IDisposable)?.Dispose();

    public ValueTask DisposeAsync() => inner is IAsyncDisposable d ? d.DisposeAsync() : DisposeSync();

    private ValueTask DisposeSync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    private sealed class ScopeFactory(IServiceScopeFactory inner) : IServiceScopeFactory
    {
        public IServiceScope CreateScope() => new Scope(inner.CreateScope());
    }

    private sealed class Scope(IServiceScope inner) : IServiceScope, IAsyncDisposable
    {
        public IServiceProvider ServiceProvider { get; } = new ActorRefServiceProvider(inner.ServiceProvider);

        public void Dispose() => inner.Dispose();

        public ValueTask DisposeAsync() => inner is IAsyncDisposable d ? d.DisposeAsync() : DisposeSync();

        private ValueTask DisposeSync()
        {
            inner.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
