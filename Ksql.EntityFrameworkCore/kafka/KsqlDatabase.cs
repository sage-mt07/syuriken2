using Ksql.EntityFramework.Configuration;
using Ksql.EntityFramework.Interfaces;
using Ksql.EntityFramework.Ksql;
using Ksql.EntityFramework.Schema;

namespace Ksql.EntityFramework;

internal class KsqlDatabase : IKsqlDatabase, IDisposable
{
   private readonly KsqlDbContextOptions _options;
   private readonly SchemaManager _schemaManager;
   private readonly KsqlClient _ksqlClient;
   private bool _disposed;

   public KsqlDatabase(KsqlDbContextOptions options, SchemaManager schemaManager)
   {
       _options = options ?? throw new ArgumentNullException(nameof(options));
       _schemaManager = schemaManager ?? throw new ArgumentNullException(nameof(schemaManager));
       _ksqlClient = new KsqlClient(options.ConnectionString);
   }

   public async Task CreateTableAsync<T>(string tableName, Func<TableOptions, TableOptions> options) where T : class
   {
       var topicDescriptor = _schemaManager.GetTopicDescriptor<T>();
       var tableOptions = options(new TableOptions
       {
           TopicName = topicDescriptor.Name,
           ValueFormat = topicDescriptor.ValueFormat
       });

       var createTableStatement = BuildCreateTableStatement(tableName, topicDescriptor, tableOptions);
       await _ksqlClient.ExecuteKsqlAsync(createTableStatement);
   }

   public async Task DropTableAsync(string tableName)
   {
       var dropStatement = $"DROP TABLE IF EXISTS {tableName};";
       await _ksqlClient.ExecuteKsqlAsync(dropStatement);
   }

   public async Task DropTopicAsync(string topicName)
   {
       var dropStatement = $"DROP TOPIC IF EXISTS {topicName};";
       await _ksqlClient.ExecuteKsqlAsync(dropStatement);
   }

   public async Task ExecuteKsqlAsync(string ksqlStatement)
   {
       await _ksqlClient.ExecuteKsqlAsync(ksqlStatement);
   }

   internal async Task EnsureTopicCreatedAsync(TopicDescriptor topicDescriptor)
   {
       // In a real implementation, this would create the Kafka topic if it doesn't exist
       await Task.CompletedTask;
   }

   internal async Task EnsureStreamCreatedAsync(TopicDescriptor topicDescriptor)
   {
       var createStreamStatement = BuildCreateStreamStatement(topicDescriptor);
       await _ksqlClient.ExecuteKsqlAsync(createStreamStatement);
   }

   internal async Task EnsureTableCreatedAsync(TableDescriptor tableDescriptor)
   {
       var createTableStatement = BuildCreateTableStatement(
           tableDescriptor.Name, 
           tableDescriptor.TopicDescriptor, 
           tableDescriptor.Options);
           
       await _ksqlClient.ExecuteKsqlAsync(createTableStatement);
   }

    internal async Task<IKsqlTransaction> BeginTransactionAsync()
    {
        // This is a placeholder for Kafka transaction support
        return new KsqlTransaction();
    }

    internal async Task RefreshMetadataAsync()
   {
       // In a real implementation, this would refresh metadata from Kafka/KSQL
       await Task.CompletedTask;
   }

   private string BuildCreateStreamStatement(TopicDescriptor topicDescriptor)
   {
       var columns = _schemaManager.GetColumnDefinitions(topicDescriptor.EntityType);
       var columnsString = string.Join(", ", columns);

       var valueFormat = topicDescriptor.ValueFormat.ToString().ToUpperInvariant();

       var timestampClause = string.Empty;
       if (!string.IsNullOrEmpty(topicDescriptor.TimestampColumn))
       {
           timestampClause = $", TIMESTAMP='{topicDescriptor.TimestampColumn}'";
           
           if (!string.IsNullOrEmpty(topicDescriptor.TimestampFormat))
           {
               timestampClause += $" FORMAT='{topicDescriptor.TimestampFormat}'";
           }
       }

       var keyClause = string.IsNullOrEmpty(topicDescriptor.KeyColumn) 
           ? string.Empty 
           : $", KEY='{topicDescriptor.KeyColumn}'";

       return $"CREATE STREAM IF NOT EXISTS {topicDescriptor.Name.ToLowerInvariant()} ({columnsString}) " +
              $"WITH (KAFKA_TOPIC='{topicDescriptor.Name}', VALUE_FORMAT='{valueFormat}'{keyClause}{timestampClause});";
   }

   private string BuildCreateTableStatement(string tableName, TopicDescriptor topicDescriptor, TableOptions options)
   {
       var columns = _schemaManager.GetColumnDefinitions(topicDescriptor.EntityType);
       var columnsString = string.Join(", ", columns);

       var valueFormat = options.ValueFormat.ToString().ToUpperInvariant();
       
       var keyClause = options.KeyColumns.Count > 0 
           ? $", KEY='{string.Join(",", options.KeyColumns)}'" 
           : string.Empty;
           
       var timestampClause = string.IsNullOrEmpty(options.TimestampColumn) 
           ? string.Empty 
           : $", TIMESTAMP='{options.TimestampColumn}'";
           
       if (!string.IsNullOrEmpty(options.TimestampFormat) && !string.IsNullOrEmpty(timestampClause))
       {
           timestampClause += $" FORMAT='{options.TimestampFormat}'";
       }

       var partitionClause = string.IsNullOrEmpty(options.PartitionBy) 
           ? string.Empty 
           : $", PARTITIONS BY='{options.PartitionBy}'";

       return $"CREATE TABLE IF NOT EXISTS {tableName.ToLowerInvariant()} ({columnsString}) " +
              $"WITH (KAFKA_TOPIC='{options.TopicName ?? topicDescriptor.Name}', VALUE_FORMAT='{valueFormat}'{keyClause}{timestampClause}{partitionClause});";
   }

   public void Dispose()
   {
       Dispose(true);
       GC.SuppressFinalize(this);
   }

   protected virtual void Dispose(bool disposing)
   {
       if (!_disposed)
       {
           if (disposing)
           {
               _ksqlClient.Dispose();
           }

           _disposed = true;
       }
   }
}