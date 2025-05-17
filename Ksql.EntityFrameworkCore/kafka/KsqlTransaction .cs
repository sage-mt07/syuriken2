using Ksql.EntityFramework.Interfaces;

namespace Ksql.EntityFramework;

internal class KsqlTransaction : IKsqlTransaction
{
    private bool _disposed;

    public Task CommitAsync()
    {
        // これはKafka Transactionsを使用したトランザクションをコミットするためのプレースホルダー実装です
        return Task.CompletedTask;
    }

    public Task AbortAsync()
    {
        // これはトランザクションを中止するためのプレースホルダー実装です
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    public ValueTask DisposeAsync()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            if (disposing)
            {
                // マネージドリソースの破棄
            }

            _disposed = true;
        }
    }
}