namespace Servus.Akka.Transport;

internal sealed class ObjectPool<T> where T : class
{
    private T? _fastItem;
    private readonly T?[] _items;

    public ObjectPool(int size)
    {
        _items = new T?[size];
    }

    public bool TryRent(out T item)
    {
        var fast = _fastItem;
        if (fast is not null && Interlocked.CompareExchange(ref _fastItem, null, fast) == fast)
        {
            item = fast;
            return true;
        }

        var items = _items;
        for (var i = 0; i < items.Length; i++)
        {
            var current = items[i];
            if (current is not null && Interlocked.CompareExchange(ref items[i], null, current) == current)
            {
                item = current;
                return true;
            }
        }

        item = default!;
        return false;
    }

    public void Return(T item)
    {
        if (Interlocked.CompareExchange(ref _fastItem, item, null) is null)
        {
            return;
        }

        var items = _items;
        for (var i = 0; i < items.Length; i++)
        {
            if (Interlocked.CompareExchange(ref items[i], item, null) is null)
            {
                return;
            }
        }
    }
}
