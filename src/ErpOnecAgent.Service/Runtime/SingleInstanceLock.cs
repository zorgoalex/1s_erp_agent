using System.Security.Cryptography;
using System.Text;

namespace ErpOnecAgent.Service.Runtime;

public sealed class SingleInstanceLock : IDisposable
{
    private Mutex? _mutex;

    public void Acquire(string agentId)
    {
        var suffix = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(agentId)))[..24];
        _mutex = new Mutex(initiallyOwned: true, "Global\\ErpOnecAgent-" + suffix, out var createdNew);
        if (!createdNew) { _mutex.Dispose(); _mutex = null; throw new InvalidOperationException($"Another ErpOnecAgent instance for '{agentId}' is already running."); }
    }

    public void Dispose()
    {
        if (_mutex is null) return;
        try { _mutex.ReleaseMutex(); } catch (ApplicationException) { }
        _mutex.Dispose(); _mutex = null;
    }
}

