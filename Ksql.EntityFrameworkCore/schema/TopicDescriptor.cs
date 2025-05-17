using Ksql.EntityFramework.Models;

namespace Ksql.EntityFramework.Schema;

public class TopicDescriptor
{
   public string Name { get; set; } = string.Empty;

   public Type EntityType { get; set; } = typeof(object);

   public int PartitionCount { get; set; } = 1;

   public int ReplicationFactor { get; set; } = 1;

   public string? KeyColumn { get; set; }

   public string? TimestampColumn { get; set; }

   public string? TimestampFormat { get; set; }

   public ValueFormat ValueFormat { get; set; } = ValueFormat.Avro;
   
   public List<string> KeyColumns { get; set; } = new List<string>();
}