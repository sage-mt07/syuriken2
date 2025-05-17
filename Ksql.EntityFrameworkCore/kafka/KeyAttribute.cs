namespace Ksql.EntityFramework.Attributes;

[AttributeUsage(AttributeTargets.Property)]
public class KeyAttribute : Attribute
{
   public int Order { get; set; } = 0;
}