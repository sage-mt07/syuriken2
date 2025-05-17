namespace Ksql.EntityFramework.Interfaces;

public interface IKsqlTable<T> : IQueryable<T> where T : class
{
   Task<T?> GetAsync(object key);

   Task<T?> FindAsync(object key);

   Task<bool> InsertAsync(T entity);

   Task<List<T>> ToListAsync();

   void Add(T entity);

   void Remove(T entity);
}