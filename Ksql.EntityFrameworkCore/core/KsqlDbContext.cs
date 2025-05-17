using Ksql.EntityFramework.Configuration;
using Ksql.EntityFramework.Interfaces;
using Ksql.EntityFramework.Schema;

namespace Ksql.EntityFramework;

public abstract class KsqlDbContext : IKsqlDbContext
{
   private readonly Dictionary<Type, object> _streams = new Dictionary<Type, object>();
   private readonly Dictionary<Type, object> _tables = new Dictionary<Type, object>();
   private readonly List<object> _pendingChanges = new List<object>();
   private readonly SchemaManager _schemaManager;
   private readonly KsqlDatabase _database;
   private bool _disposed;

   public IKsqlDatabase Database => _database;

   public KsqlDbContextOptions Options { get; }

   protected KsqlDbContext() : this(new KsqlDbContextOptions())
   {
   }

   protected KsqlDbContext(KsqlDbContextOptions options)
   {
       Options = options ?? throw new ArgumentNullException(nameof(options));
       _schemaManager = new SchemaManager(options);
       _database = new KsqlDatabase(options, _schemaManager);
       InitializeContext();
   }

   public IKsqlStream<T> CreateStream<T>(string name) where T : class
   {
       var stream = new KsqlStream<T>(name, this, _schemaManager);
       _streams[typeof(T)] = stream;
       return stream;
   }

   public IKsqlTable<T> CreateTable<T>(string name) where T : class
   {
       var table = new KsqlTable<T>(name, this, _schemaManager);
       _tables[typeof(T)] = table;
       return table;
   }

   public IKsqlTable<T> CreateTable<T>(string name, Func<TableBuilder<T>, TableBuilder<T>> tableBuilder) where T : class
   {
       var builder = new TableBuilder<T>(name);
       builder = tableBuilder(builder);
       var source = builder.GetSource();
       var options = builder.Build();

       var table = new KsqlTable<T>(name, this, _schemaManager, options);
       _tables[typeof(T)] = table;
       return table;
   }

   public async Task EnsureTopicCreatedAsync<T>() where T : class
   {
       var topicDescriptor = _schemaManager.GetTopicDescriptor<T>();
       await _database.EnsureTopicCreatedAsync(topicDescriptor).ConfigureAwait(false);
   }

   public async Task EnsureStreamCreatedAsync<T>() where T : class
   {
       var topicDescriptor = _schemaManager.GetTopicDescriptor<T>();
       await _database.EnsureStreamCreatedAsync(topicDescriptor).ConfigureAwait(false);
   }

   public async Task EnsureTableCreatedAsync<T>(IKsqlTable<T> table) where T : class
   {
       if (table == null) throw new ArgumentNullException(nameof(table));

       var ksqlTable = table as KsqlTable<T> ?? throw new ArgumentException("The table must be created by this context.", nameof(table));
       await _database.EnsureTableCreatedAsync(ksqlTable.GetTableDescriptor()).ConfigureAwait(false);
   }

   public async Task SaveChangesAsync()
   {
       foreach (var change in _pendingChanges)
       {
           await SaveChangeAsync(change).ConfigureAwait(false);
       }

       _pendingChanges.Clear();
   }

   public async Task<IKsqlTransaction> BeginTransactionAsync()
   {
          return await _database.BeginTransactionAsync().ConfigureAwait(false);
   }

   public async Task RefreshMetadataAsync()
   {
       await _database.RefreshMetadataAsync().ConfigureAwait(false);
   }

   internal void AddToPendingChanges<T>(T entity) where T : class
   {
       _pendingChanges.Add(entity);
   }

   private void InitializeContext()
   {
       var properties = GetType().GetProperties();
       foreach (var property in properties)
       {
           if (property.PropertyType.IsGenericType)
           {
               var genericType = property.PropertyType.GetGenericTypeDefinition();
               var entityType = property.PropertyType.GetGenericArguments()[0];

               if (genericType == typeof(IKsqlStream<>))
               {
                   var createStreamMethod = typeof(KsqlDbContext).GetMethod(nameof(CreateStream))?.MakeGenericMethod(entityType);
                   var stream = createStreamMethod?.Invoke(this, new object[] { entityType.Name.ToLowerInvariant() });
                   property.SetValue(this, stream);
               }
               else if (genericType == typeof(IKsqlTable<>))
               {
                   var createTableMethod = typeof(KsqlDbContext).GetMethod(nameof(CreateTable), new[] { typeof(string) })?.MakeGenericMethod(entityType);
                   var table = createTableMethod?.Invoke(this, new object[] { entityType.Name.ToLowerInvariant() });
                   property.SetValue(this, table);
               }
           }
       }
   }

   private async Task SaveChangeAsync(object change)
   {
       var changeType = change.GetType();

       if (_streams.TryGetValue(changeType, out var streamObj))
       {
           var streamType = typeof(KsqlStream<>).MakeGenericType(changeType);
           var produceAsyncMethod = streamType.GetMethod("ProduceAsync", new[] { changeType });
           await (Task)produceAsyncMethod.Invoke(streamObj, new[] { change });
       }
       else if (_tables.TryGetValue(changeType, out var tableObj))
       {
           var tableType = typeof(KsqlTable<>).MakeGenericType(changeType);
           var insertAsyncMethod = tableType.GetMethod("InsertAsync", new[] { changeType });
           await (Task)insertAsyncMethod.Invoke(tableObj, new[] { change });
       }
       else
       {
           throw new InvalidOperationException($"No stream or table found for entity type {changeType.Name}");
       }
   }

   public void Dispose()
   {
       Dispose(true);
       GC.SuppressFinalize(this);
   }

   public async ValueTask DisposeAsync()
   {
       await DisposeAsyncCore().ConfigureAwait(false);
       Dispose(false);
       GC.SuppressFinalize(this);
   }

   protected virtual void Dispose(bool disposing)
   {
       if (!_disposed)
       {
           if (disposing)
           {
               _database.Dispose();
           }

           _disposed = true;
       }
   }

   protected virtual async ValueTask DisposeAsyncCore()
   {
       if (_database is IAsyncDisposable asyncDisposable)
       {
           await asyncDisposable.DisposeAsync().ConfigureAwait(false);
       }
       else
       {
           _database.Dispose();
       }
   }
}