namespace Ksql.EntityFramework.Models;

public class ChangeNotification<T> where T : class
{
   public ChangeType ChangeType { get; }

   public T Entity { get; }

   public object Key { get; }

   public T? PreviousEntity { get; }

   public DateTimeOffset Timestamp { get; }

   public ChangeNotification(ChangeType changeType, T entity, object key, T? previousEntity, DateTimeOffset timestamp)
   {
       ChangeType = changeType;
       Entity = entity;
       Key = key;
       PreviousEntity = previousEntity;
       Timestamp = timestamp;
   }
}