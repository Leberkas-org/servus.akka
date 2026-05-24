namespace Servus.Akka.Local;

public class FileEntityIdStore : IEntityIdStore
{
    private const string FileName = "entities.store";

    private readonly string _filePath;
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private HashSet<string> _entities = [];

    public FileEntityIdStore(string directory)
    {
        _filePath = Path.Combine(directory, FileName);
    }

    public FileEntityIdStore(Environment.SpecialFolder folder, string name)
        : this(Path.Combine(Environment.GetFolderPath(folder), name))
    {
    }

    public async Task<IReadOnlyCollection<string>> LoadEntitiesAsync()
    {
        if (!File.Exists(_filePath))
        {
            _entities = [];
            return [];
        }

        var lines = await File.ReadAllLinesAsync(_filePath);
        _entities = lines.Where(e => !string.IsNullOrWhiteSpace(e)).ToHashSet();

        return _entities.ToList();
    }

    public async Task EntityStarted(string entityId)
    {
        _entities.Add(entityId);
        await FlushAsync();
    }

    public async Task EntityStopped(string entityId)
    {
        _entities.Remove(entityId);
        await FlushAsync();
    }

    private async Task FlushAsync()
    {
        await _writeLock.WaitAsync();
        try
        {
            var directory = Path.GetDirectoryName(_filePath);
            if (directory is not null)
                Directory.CreateDirectory(directory);

            var tempPath = _filePath + ".tmp";
            await File.WriteAllLinesAsync(tempPath, _entities);
            File.Move(tempPath, _filePath, overwrite: true);
        }
        finally
        {
            _writeLock.Release();
        }
    }
}
