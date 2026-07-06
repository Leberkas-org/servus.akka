using Servus.Akka.Transport;

namespace Servus.Akka.TestKit.Tests;

internal static class WireBufferTestExtensions
{
    public static WireBuffer ToWireBuffer(this byte[] data)
    {
        var buf = WireBuffer.Rent(data.Length);
        data.AsSpan().CopyTo(buf.FullMemory.Span);
        buf.Length = data.Length;
        return buf;
    }
}
