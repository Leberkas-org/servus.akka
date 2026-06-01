using System.Buffers;
using System.Text;
using Akka;
using Akka.Streams.Dsl;

namespace Servus.Akka.Sse;

public static class SseFormatterFlow
{
    private const byte Lf = (byte)'\n';

    public static Flow<ServerSentEvent, ReadOnlyMemory<byte>, NotUsed> Instance { get; }
        = Flow.Create<ServerSentEvent>().Select(Format);

    private static ReadOnlyMemory<byte> Format(ServerSentEvent evt)
    {
        var size = EstimateSize(evt);
        var buffer = ArrayPool<byte>.Shared.Rent(size);
        var pos = 0;

        if (evt.EventType is not null && evt.EventType != "message")
        {
            pos += WriteField(buffer.AsSpan(pos), "event: "u8, evt.EventType.AsSpan());
        }

        WriteLinesWithPrefix(buffer, ref pos, "data: "u8, evt.Data.AsSpan());
        buffer[pos++] = Lf;

        if (evt.Id is not null && !evt.Id.Contains('\0') && !evt.Id.AsSpan().ContainsAny('\r', '\n'))
        {
            pos += WriteField(buffer.AsSpan(pos), "id: "u8, evt.Id.AsSpan());
        }

        if (evt.Retry is not null && evt.Retry.Value >= TimeSpan.Zero)
        {
            Span<byte> retryBuf = stackalloc byte[20];
            ((long)evt.Retry.Value.TotalMilliseconds).TryFormat(retryBuf, out var retryLen);
            pos += WriteFieldBytes(buffer.AsSpan(pos), "retry: "u8, retryBuf[..retryLen]);
        }

        buffer[pos++] = Lf;

        var result = new byte[pos];
        buffer.AsSpan(0, pos).CopyTo(result);
        ArrayPool<byte>.Shared.Return(buffer);

        return result.AsMemory();
    }

    private static int WriteField(Span<byte> dest, ReadOnlySpan<byte> prefix, ReadOnlySpan<char> value)
    {
        prefix.CopyTo(dest);
        var written = prefix.Length;
        written += Encoding.UTF8.GetBytes(value, dest[written..]);
        dest[written++] = Lf;
        return written;
    }

    private static int WriteFieldBytes(Span<byte> dest, ReadOnlySpan<byte> prefix, ReadOnlySpan<byte> value)
    {
        prefix.CopyTo(dest);
        var written = prefix.Length;
        value.CopyTo(dest[written..]);
        written += value.Length;
        dest[written++] = Lf;
        return written;
    }

    private static void WriteLinesWithPrefix(byte[] buffer, ref int pos, ReadOnlySpan<byte> prefix, ReadOnlySpan<char> data)
    {
        while (true)
        {
            prefix.CopyTo(buffer.AsSpan(pos));
            pos += prefix.Length;

            var nlIndex = data.IndexOfAny('\r', '\n');
            if (nlIndex < 0)
            {
                break;
            }

            pos += Encoding.UTF8.GetBytes(data[..nlIndex], buffer.AsSpan(pos));
            buffer[pos++] = Lf;

            if (data[nlIndex] == '\r' && nlIndex + 1 < data.Length && data[nlIndex + 1] == '\n')
            {
                data = data[(nlIndex + 2)..];
            }
            else
            {
                data = data[(nlIndex + 1)..];
            }
        }

        pos += Encoding.UTF8.GetBytes(data, buffer.AsSpan(pos));
    }

    private static int EstimateSize(ServerSentEvent evt)
    {
        var size = Encoding.UTF8.GetMaxByteCount(evt.Data.Length) + evt.Data.Length + 32;
        if (evt.EventType is not null)
        {
            size += Encoding.UTF8.GetMaxByteCount(evt.EventType.Length) + 10;
        }

        if (evt.Id is not null)
        {
            size += Encoding.UTF8.GetMaxByteCount(evt.Id.Length) + 6;
        }

        if (evt.Retry is not null)
        {
            size += 28;
        }

        return size;
    }
}
