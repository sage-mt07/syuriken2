namespace Ksql.EntityFramework.Models;

public class DeadLetterMessage
{
   public byte[]? OriginalData { get; set; }

   public string? ErrorMessage { get; set; }

   public DateTimeOffset Timestamp { get; set; }

   public string? SourceTopic { get; set; }

   public string? ErrorContext { get; set; }
}