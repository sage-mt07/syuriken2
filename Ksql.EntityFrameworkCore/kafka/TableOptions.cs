using Ksql.EntityFramework.Models;

namespace Ksql.EntityFramework.Configuration;

public class TableOptions
{
   public string? TopicName { get; set; }

   public ValueFormat ValueFormat { get; set; } = ValueFormat.Avro;

   public List<string> KeyColumns { get; set; } = new List<string>();

   public string? PartitionBy { get; set; }

   public string? TimestampColumn { get; set; }

   public string? TimestampFormat { get; set; }

   public TableOptions WithKeyColumns(params string[] columnNames)
   {
       KeyColumns.Clear();
       KeyColumns.AddRange(columnNames);
       return this;
   }

   public TableOptions WithTopic(string topicName)
   {
       TopicName = topicName;
       return this;
   }

   public TableOptions WithValueFormat(ValueFormat format)
   {
       ValueFormat = format;
       return this;
   }

   public TableOptions WithPartitionBy(string columnName)
   {
       PartitionBy = columnName;
       return this;
   }

   public TableOptions WithTimestamp(string columnName, string? format = null)
   {
       TimestampColumn = columnName;
       TimestampFormat = format;
       return this;
   }
}