using System.Runtime.CompilerServices;

namespace Servus.Akka.Tests;

internal static class TestInitializer
{
    [ModuleInitializer]
    internal static void Initialize()
    {
        ThreadPool.SetMinThreads(128, 128);
    }
}
