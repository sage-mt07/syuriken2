namespace Ksql.EntityFramework.Windows;

public abstract class WindowSpecification
{
   public abstract WindowType WindowType { get; }

   public abstract string ToKsqlString();
}

public enum WindowType
{
   Tumbling,
   Hopping,
   Session
}