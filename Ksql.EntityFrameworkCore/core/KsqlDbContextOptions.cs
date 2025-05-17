using Ksql.EntityFramework.Models;

namespace Ksql.EntityFramework.Configuration;

public class KsqlDbContextOptions
{
   public string? ConnectionString { get; set; }

   public string? SchemaRegistryUrl { get; set; }

   public ValueFormat DefaultValueFormat { get; set; } = ValueFormat.Avro;

   public ErrorPolicy DeserializationErrorPolicy { get; set; } = ErrorPolicy.Abort;

   public string? DeadLetterQueue { get; set; }

   public Func<byte[]?, Exception, DeadLetterMessage>? DeadLetterQueueErrorHandler { get; set; }

   public int DefaultPartitionCount { get; set; } = 3;

   public int DefaultReplicationFactor { get; set; } = 3;

   public int ConnectionTimeoutSeconds { get; set; } = 30;

   public int MaxRetries { get; set; } = 3;

   public int RetryBackoffMs { get; set; } = 500;
}