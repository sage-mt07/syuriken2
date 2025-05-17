namespace Ksql.EntityFramework.Windows;

public class TumblingWindow : WindowSpecification
{
   public TimeSpan Size { get; }

   public override WindowType WindowType => WindowType.Tumbling;

   public TumblingWindow(TimeSpan size)
   {
       Size = size;
   }

   public static TumblingWindow Of(TimeSpan size)
   {
       return new TumblingWindow(size);
   }

   public override string ToKsqlString()
   {
       string timeUnit;
       long value;

       if (Size.TotalMilliseconds < 1000)
       {
           timeUnit = "MILLISECONDS";
           value = (long)Size.TotalMilliseconds;
       }
       else if (Size.TotalSeconds < 60)
       {
           timeUnit = "SECONDS";
           value = (long)Size.TotalSeconds;
       }
       else if (Size.TotalMinutes < 60)
       {
           timeUnit = "MINUTES";
           value = (long)Size.TotalMinutes;
       }
       else if (Size.TotalHours < 24)
       {
           timeUnit = "HOURS";
           value = (long)Size.TotalHours;
       }
       else
       {
           timeUnit = "DAYS";
           value = (long)Size.TotalDays;
       }

       return $"TUMBLING (SIZE {value} {timeUnit})";
   }
}