using Servus.Akka.Transport;

namespace Servus.Akka.Tests.Transport;

public sealed class ObjectPoolSpec
{
    [Fact(Timeout = 5000)]
    public void TryRent_from_empty_pool_returns_false()
    {
        var pool = new ObjectPool<object>(4);

        Assert.False(pool.TryRent(out _));
    }

    [Fact(Timeout = 5000)]
    public void Return_then_Rent_hands_back_the_same_instance()
    {
        var pool = new ObjectPool<object>(4);
        var item = new object();

        Assert.True(pool.TryReturn(item));
        Assert.True(pool.TryRent(out var rented));
        Assert.Same(item, rented);
    }

    [Fact(Timeout = 5000)]
    public void Return_uses_the_fast_slot_then_the_array_and_drops_when_full()
    {
        // Capacity is 1 array slot plus the fast slot, so the third return has nowhere to go.
        var pool = new ObjectPool<object>(1);

        Assert.True(pool.TryReturn(new object()));   // fast slot
        Assert.True(pool.TryReturn(new object()));   // array[0]
        Assert.False(pool.TryReturn(new object()));  // full -> dropped
    }

    [Fact(Timeout = 5000)]
    public void A_batch_of_returned_instances_all_come_back_out()
    {
        // The deterministic core of the old QuicStreamState repool assertion, isolated from the
        // process-wide shared pool so it can no longer race parallel test classes.
        const int count = 6;
        var pool = new ObjectPool<object>(count * 2);
        var originals = new HashSet<object>(ReferenceEqualityComparer.Instance);

        for (var i = 0; i < count; i++)
        {
            var item = new object();
            originals.Add(item);
            Assert.True(pool.TryReturn(item));
        }

        for (var i = 0; i < count; i++)
        {
            Assert.True(pool.TryRent(out var rented));
            Assert.Contains(rented, originals);
        }
    }
}
