namespace Ksql.EntityFramework.Attributes;

[AttributeUsage(AttributeTargets.Class)]
public class TopicAttribute : Attribute
{
   public string Name { get; }

   public int PartitionCount { get; set; } = 1;

   public int ReplicationFactor { get; set; } = 1;

   public TopicAttribute(string name)
   {
       Name = name ?? throw new ArgumentNullException(nameof(name));
   }
}