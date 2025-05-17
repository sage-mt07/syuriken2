namespace Ksql.EntityFramework.Attributes;

[AttributeUsage(AttributeTargets.Property)]
public class DecimalPrecisionAttribute : Attribute
{
   public int Precision { get; }

   public int Scale { get; }

   public DecimalPrecisionAttribute(int precision, int scale)
   {
       if (precision <= 0) throw new ArgumentOutOfRangeException(nameof(precision), "Precision must be greater than zero.");
       if (scale < 0 || scale > precision) throw new ArgumentOutOfRangeException(nameof(scale), "Scale must be between 0 and precision.");

       Precision = precision;
       Scale = scale;
   }
}