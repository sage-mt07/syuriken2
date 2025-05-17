using Ksql.EntityFramework.Configuration;

namespace Ksql.EntityFramework.Schema;

internal class TableDescriptor
{
   public string Name { get; set; } = string.Empty;

   public TopicDescriptor TopicDescriptor { get; set; } = new TopicDescriptor();

   public TableOptions Options { get; set; } = new TableOptions();
}