using System.Runtime.CompilerServices;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Ksql.EntityFramework.Configuration;
using Ksql.EntityFramework.Models;

namespace Ksql.EntityFramework.Kafka;

internal class KafkaConsumer<TKey, TValue> : IDisposable where TValue : class
{
   private readonly IConsumer<TKey, TValue> _consumer;
   private readonly string _topic;
   private readonly KsqlDbContextOptions _options;
   private bool _disposed;

   public KafkaConsumer(string topic, KsqlDbContextOptions options, string? groupId = null)
   {
       _topic = topic ?? throw new ArgumentNullException(nameof(topic));
       _options = options ?? throw new ArgumentNullException(nameof(options));

       var config = new ConsumerConfig
       {
           BootstrapServers = ExtractBootstrapServers(options.ConnectionString),
           GroupId = groupId ?? $"ksql-entityframework-consumer-{Guid.NewGuid()}",
           AutoOffsetReset = AutoOffsetReset.Earliest,
           EnableAutoCommit = true,
       };

       var schemaRegistryConfig = new SchemaRegistryConfig
       {
           Url = options.SchemaRegistryUrl
       };

       var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);

       var builder = new ConsumerBuilder<TKey, TValue>(config)
                .SetKeyDeserializer(new Confluent.SchemaRegistry.Serdes.AvroDeserializer<TKey>(schemaRegistry).AsSyncOverAsync())
                .SetValueDeserializer(new Confluent.SchemaRegistry.Serdes.AvroDeserializer<TValue>(schemaRegistry).AsSyncOverAsync());

       _consumer = builder.Build();

       _consumer.Subscribe(_topic);
   }

   public IEnumerable<TValue> Consume([EnumeratorCancellation] CancellationToken cancellationToken = default)
   {
       while (!cancellationToken.IsCancellationRequested)
       {
           var consumeResult = _consumer.Consume(TimeSpan.FromMilliseconds(100));

           if (consumeResult != null && consumeResult.Message != null)
           {
               yield return consumeResult.Message.Value;
           }
       }
   }

   private string ExtractBootstrapServers(string connectionString)
   {
       return connectionString;
   }

   private void OnConsumerError(IConsumer<TKey, TValue> consumer, Error error)
   {
       Console.WriteLine($"Consumer error: {error.Reason}");

       if (_options.DeserializationErrorPolicy == ErrorPolicy.Abort)
       {
           throw new KafkaException(error);
       }
   }

   private void HandleConsumeError(ConsumeException ex)
   {
       Console.WriteLine($"Consume error: {ex.Error.Reason}");

       if (_options.DeserializationErrorPolicy == ErrorPolicy.Abort)
       {
           throw ex;
       }
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
               _consumer.Close();
               _consumer.Dispose();
           }

           _disposed = true;
       }
   }
}