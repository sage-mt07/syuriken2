using Ksql.EntityFramework.Configuration;

namespace Ksql.EntityFramework.Interfaces;

public interface IKsqlDbContext : IDisposable, IAsyncDisposable
{
   IKsqlDatabase Database { get; }

   KsqlDbContextOptions Options { get; }

   IKsqlStream<T> CreateStream<T>(string name) where T : class;

   IKsqlTable<T> CreateTable<T>(string name) where T : class;

   IKsqlTable<T> CreateTable<T>(string name, Func<TableBuilder<T>, TableBuilder<T>> tableBuilder) where T : class;

   Task EnsureTopicCreatedAsync<T>() where T : class;

   Task EnsureStreamCreatedAsync<T>() where T : class;

   Task EnsureTableCreatedAsync<T>(IKsqlTable<T> table) where T : class;

   Task SaveChangesAsync();

   Task<IKsqlTransaction> BeginTransactionAsync();

   Task RefreshMetadataAsync();
}