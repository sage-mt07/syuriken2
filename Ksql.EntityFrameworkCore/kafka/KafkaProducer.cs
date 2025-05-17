using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Ksql.EntityFramework.Configuration;

namespace Ksql.EntityFramework.Kafka;

internal class KafkaProducer<TKey, TValue> : IDisposable where TValue : class
{
   private readonly IProducer<TKey, TValue> _producer;
   private readonly string _topic;
   private readonly KsqlDbContextOptions _options;
   private bool _disposed;

   public KafkaProducer(string topic, KsqlDbContextOptions options)
   {
       _topic = topic ?? throw new ArgumentNullException(nameof(topic));
       _options = options ?? throw new ArgumentNullException(nameof(options));

       var config = new ProducerConfig
       {
           BootstrapServers = ExtractBootstrapServers(options.ConnectionString),
           EnableDeliveryReports = true,
           ClientId = $"ksql-entityframework-producer-{Guid.NewGuid()}",
       };

       var schemaRegistryConfig = new SchemaRegistryConfig
       {
           Url = options.SchemaRegistryUrl
       };

       var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);

       var avroSerializerConfig = new AvroSerializerConfig
       {
           AutoRegisterSchemas = true
       };

       _producer = new ProducerBuilder<TKey, TValue>(config)
           .SetKeySerializer(new AvroSerializer<TKey>(schemaRegistry, avroSerializerConfig))
           .SetValueSerializer(new AvroSerializer<TValue>(schemaRegistry, avroSerializerConfig))
           .Build();
   }

   public async Task<DeliveryResult<TKey, TValue>> ProduceAsync(TKey key, TValue value)
   {
       var message = new Message<TKey, TValue>
       {
           Key = key,
           Value = value
       };

       try
       {
           return await _producer.ProduceAsync(_topic, message);
       }
       catch (ProduceException<TKey, TValue> ex)
       {
           Console.WriteLine($"Failed to deliver message: {ex.Error.Reason}");
           throw;
       }
   }

   public async Task ProduceBatchAsync(IEnumerable<KeyValuePair<TKey, TValue>> messages)
   {
       var tasks = new List<Task<DeliveryResult<TKey, TValue>>>();

       foreach (var pair in messages)
       {
           tasks.Add(ProduceAsync(pair.Key, pair.Value));
       }

       await Task.WhenAll(tasks);
   }

   private string ExtractBootstrapServers(string connectionString)
   {
       return connectionString;
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
               _producer.Dispose();
           }

           _disposed = true;
       }
   }
}