namespace Ksql.EntityFramework.Models;

public enum ErrorPolicy
{
   Abort,
   Skip,
   Retry,
   DeadLetterQueue
}