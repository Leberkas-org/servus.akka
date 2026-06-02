# Akka Option Match

Small extension methods to make `Akka.Util.Option<T>` matching more ergonomic.

## Value-returning match

```csharp
Option<string> option = Option<string>.Create("servus");

var isServus = option.Match(
    some: value => value == "servus",
    none: () => false);
```

## Action-based match

```csharp
option.Match(
    some: value => Console.WriteLine($"Value: {value}"),
    none: () => Console.WriteLine("No value"));
```

## API

```csharp
public static class AkkaOptionsExtensions
{
    public static TOut Match<TIn, TOut>(
        this Option<TIn> option,
        Func<TIn, TOut> some,
        Func<TOut> none);

    public static void Match<T>(
        this Option<T> option,
        Action<T> some,
        Action none);
}
```
