namespace Ksql.EntityFramework.Models;

internal class JoinOperation
{
   public JoinType Type { get; }

   public string LeftSource { get; }

   public string RightSource { get; }

   public string JoinCondition { get; }

   public string? WindowSpecification { get; }

   public JoinOperation(JoinType type, string leftSource, string rightSource, string joinCondition, string? windowSpecification = null)
   {
       Type = type;
       LeftSource = leftSource ?? throw new ArgumentNullException(nameof(leftSource));
       RightSource = rightSource ?? throw new ArgumentNullException(nameof(rightSource));
       JoinCondition = joinCondition ?? throw new ArgumentNullException(nameof(joinCondition));
       WindowSpecification = windowSpecification;
   }

   public string ToKsqlString()
   {
       var joinTypeString = Type switch
       {
           JoinType.Inner => "JOIN",
           JoinType.Left => "LEFT JOIN",
           JoinType.FullOuter => "FULL OUTER JOIN",
           _ => throw new InvalidOperationException($"Unsupported join type: {Type}")
       };

       var windowClause = string.IsNullOrEmpty(WindowSpecification)
           ? string.Empty
           : $" WITHIN {WindowSpecification}";

       return $"{LeftSource} {joinTypeString} {RightSource} ON {JoinCondition}{windowClause}";
   }
}