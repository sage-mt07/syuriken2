namespace Ksql.EntityFramework.Interfaces;

public interface IKsqlTransaction : IDisposable, IAsyncDisposable
{
   Task CommitAsync();

   Task AbortAsync();
}