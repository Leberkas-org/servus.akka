# Getting Started

**Servus.Akka** packages practical quality-of-life helpers for Akka.NET applications: easier actor registration, DI integration for `IActorRef`, trace-aware messaging, and startup containers.

## Installation

### Package Manager

```powershell
Install-Package Servus.Akka
```

### .NET CLI

```bash
dotnet add package Servus.Akka
```

### PackageReference

```xml
<PackageReference Include="Servus.Akka" Version="1.0.0" />
```

## First setup

```csharp
using Akka.Hosting;
using Servus.Akka;
using Servus.Akka.DependencyInjection;

var builder = WebApplication.CreateBuilder(args);
builder.Host.UseServiceProviderFactory(new ActorRefProviderFactory());

builder.Services.AddAkka("servus", akka =>
{
    akka.WithResolvableActors(helper =>
    {
        helper.Register<MyActor>();
    });
});
```

## Modules

- [**Extensions**](./modules/extensions/) — resolve/register helpers and convenience APIs around `ActorSystem` and `IActorContext`.
- [**Dependency Injection**](./modules/dependency-injection/) — `ActorRef<TActor>` and custom service-provider resolution.
- [**Diagnostics**](./modules/diagnostics/) — trace-aware actor base class and traced messaging extensions.
- [**Messaging**](./modules/messaging/) — envelope contracts used for traced messages.
- [**Startup**](./modules/startup/) — setup containers for host and actor-system composition.

---

**Servus and happy coding!** 🥨🍺
