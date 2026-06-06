using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using Servus.Akka.Transport;
using Servus.Akka.Transport.Tcp;

namespace Servus.Akka.Tests.Transport.Tcp;

public sealed class PipeSegmentLeaseSpec
{
    [Fact(Timeout = 5000)]
    public async Task Memory_should_expose_pipe_segment_data()
    {
        var pipe = new Pipe();
        var expected = "hello world"u8.ToArray();

        await pipe.Writer.WriteAsync(expected);
        var result = await pipe.Reader.ReadAsync();

        using var lease = new PipeSegmentLease(result.Buffer, pipe.Reader);

        Assert.Equal(expected, lease.Memory.ToArray());
    }

    [Fact(Timeout = 5000)]
    public async Task Dispose_should_advance_pipe_reader()
    {
        var pipe = new Pipe();
        var data = "first"u8.ToArray();

        await pipe.Writer.WriteAsync(data);
        var result = await pipe.Reader.ReadAsync();

        var lease = new PipeSegmentLease(result.Buffer, pipe.Reader);
        lease.Dispose();

        var moreData = "second"u8.ToArray();
        await pipe.Writer.WriteAsync(moreData);
        var result2 = await pipe.Reader.ReadAsync();

        Assert.Equal(moreData, result2.Buffer.ToArray());

        pipe.Reader.AdvanceTo(result2.Buffer.End);
        pipe.Writer.Complete();
        pipe.Reader.Complete();
    }

    [Fact(Timeout = 5000)]
    public async Task Lease_should_implement_ITransportInbound()
    {
        var pipe = new Pipe();
        await pipe.Writer.WriteAsync("x"u8.ToArray());
        var result = await pipe.Reader.ReadAsync();

        using var lease = new PipeSegmentLease(result.Buffer, pipe.Reader);

        Assert.IsAssignableFrom<ITransportInbound>(lease);
    }

    [Fact(Timeout = 5000)]
    public async Task ToTransportBuffer_should_copy_for_legacy_compat()
    {
        var pipe = new Pipe();
        var expected = "legacy data"u8.ToArray();

        await pipe.Writer.WriteAsync(expected);
        var result = await pipe.Reader.ReadAsync();

        using var lease = new PipeSegmentLease(result.Buffer, pipe.Reader);
        using var buf = lease.ToTransportBuffer();

        Assert.Equal(expected.Length, buf.Length);
        Assert.Equal(expected, buf.Memory.ToArray());
    }
}
