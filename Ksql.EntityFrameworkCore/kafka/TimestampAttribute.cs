namespace Ksql.EntityFramework.Attributes;

public enum TimestampType
{
   EventTime,
   ProcessingTime
}

[AttributeUsage(AttributeTargets.Property)]
public class TimestampAttribute : Attribute
{
   public string? Format { get; set; }

   public TimestampType Type { get; set; } = TimestampType.EventTime;
}