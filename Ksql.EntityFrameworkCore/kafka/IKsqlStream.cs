using System.Linq.Expressions;
using Ksql.EntityFramework.Models;
using Ksql.EntityFramework.Windows;
using Ksql.EntityFrameworkCore.Models;

namespace Ksql.EntityFramework.Interfaces;

public interface IKsqlStream<T> : IQueryable<T> where T : class
{
   Task<long> ProduceAsync(T entity);

   Task<long> ProduceAsync(string key, T entity);

   Task ProduceBatchAsync(IEnumerable<T> entities);

   IDisposable Subscribe(
       Action<T> onNext,
       Action<Exception>? onError = null,
       Action? onCompleted = null,
       CancellationToken cancellationToken = default);

   IKsqlStream<T> WithWatermark<TTimestamp>(System.Linq.Expressions.Expression<Func<T, TTimestamp>> timestampSelector, TimeSpan maxOutOfOrderness);

   IKsqlStream<T> OnError(ErrorAction errorAction);

   IWindowedKsqlStream<T> Window(WindowSpecification window);

   void Add(T entity);

   IKsqlStream<TResult> Join<TRight, TKey, TResult>(
       IKsqlStream<TRight> rightStream,
       Expression<Func<T, TKey>> leftKeySelector,
       Expression<Func<TRight, TKey>> rightKeySelector,
       Expression<Func<T, TRight, TResult>> resultSelector,
       WindowSpecification window)
       where TRight : class,new()
       where TResult : class;

   IKsqlStream<TResult> Join<TRight, TKey, TResult>(
       IKsqlTable<TRight> table,
       Expression<Func<T, TKey>> leftKeySelector,
       Expression<Func<TRight, TKey>> rightKeySelector,
       Expression<Func<T, TRight, TResult>> resultSelector)
       where TRight : class
       where TResult : class;

   IKsqlStream<TResult> LeftJoin<TRight, TKey, TResult>(
       IKsqlTable<TRight> table,
       Expression<Func<T, TKey>> leftKeySelector,
       Expression<Func<TRight, TKey>> rightKeySelector,
       Expression<Func<T, TRight, TResult>> resultSelector)
       where TRight : class
       where TResult : class;
}