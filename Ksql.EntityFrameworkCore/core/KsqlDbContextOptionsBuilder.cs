using Ksql.EntityFramework.Models;

namespace Ksql.EntityFramework.Configuration;

public class KsqlDbContextOptionsBuilder
{
   private readonly KsqlDbContextOptions _options = new KsqlDbContextOptions();

   public KsqlDbContextOptionsBuilder UseConnectionString(string connectionString)
   {
       _options.ConnectionString = connectionString;
       return this;
   }

   public KsqlDbContextOptionsBuilder UseSchemaRegistry(string url)
   {
       _options.SchemaRegistryUrl = url;
       return this;
   }

   public KsqlDbContextOptionsBuilder UseDefaultValueFormat(ValueFormat format)
   {
       _options.DefaultValueFormat = format;
       return this;
   }

   public KsqlDbContextOptionsBuilder UseDeserializationErrorPolicy(ErrorPolicy policy)
   {
       _options.DeserializationErrorPolicy = policy;
       return this;
   }

   public KsqlDbContextOptionsBuilder UseDeadLetterQueue(string topicName, Func<byte[]?, Exception, DeadLetterMessage>? errorHandler = null)
   {
       _options.DeadLetterQueue = topicName;
       _options.DeadLetterQueueErrorHandler = errorHandler;
       return this;
   }

   public KsqlDbContextOptionsBuilder UseDefaultPartitionCount(int partitionCount)
   {
       _options.DefaultPartitionCount = partitionCount;
       return this;
   }

   public KsqlDbContextOptionsBuilder UseDefaultReplicationFactor(int replicationFactor)
   {
       _options.DefaultReplicationFactor = replicationFactor;
       return this;
   }

   public KsqlDbContextOptionsBuilder UseConnectionTimeout(int timeoutSeconds)
   {
       _options.ConnectionTimeoutSeconds = timeoutSeconds;
       return this;
   }

   public KsqlDbContextOptionsBuilder UseRetryPolicy(int maxRetries, int backoffMs)
   {
       _options.MaxRetries = maxRetries;
       _options.RetryBackoffMs = backoffMs;
       return this;
   }

   public KsqlDbContextOptions Build()
   {
       return _options;
   }
}