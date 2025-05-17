using System.Collections;
using System.Linq.Expressions;
using Ksql.EntityFramework.Interfaces;
using Ksql.EntityFramework.Models;
using Ksql.EntityFramework.Schema;
using Ksql.EntityFramework.Windows;
using Ksql.EntityFrameworkCore.Models;

namespace Ksql.EntityFramework;

internal class KsqlStream<T> : IKsqlStream<T> where T : class
{
   private readonly KsqlDbContext _context;
   private readonly SchemaManager _schemaManager;
   private readonly Dictionary<string, object> _streamProperties = new Dictionary<string, object>();
   private readonly KsqlQueryProvider _queryProvider = new KsqlQueryProvider();
   private ErrorAction _errorAction = ErrorAction.Stop;

   public Type ElementType => typeof(T);

   public Expression Expression => System.Linq.Expressions.Expression.Constant(this);

   public IQueryProvider Provider => _queryProvider;

   public string Name { get; }

   public KsqlStream(string name, KsqlDbContext context, SchemaManager schemaManager)
   {
       Name = name ?? throw new ArgumentNullException(nameof(name));
       _context = context ?? throw new ArgumentNullException(nameof(context));
       _schemaManager = schemaManager ?? throw new ArgumentNullException(nameof(schemaManager));
   }

   public async Task<long> ProduceAsync(T entity)
   {
       if (entity == null)
           throw new ArgumentNullException(nameof(entity));

       var topicDescriptor = _schemaManager.GetTopicDescriptor<T>();
       var key = _schemaManager.ExtractKey(entity) ?? Guid.NewGuid().ToString();

       return await ProduceAsync(key, entity);
   }

   public async Task<long> ProduceAsync(string key, T entity)
   {
       if (entity == null)
           throw new ArgumentNullException(nameof(entity));
       if (string.IsNullOrEmpty(key))
           throw new ArgumentNullException(nameof(key));

       var topicDescriptor = _schemaManager.GetTopicDescriptor<T>();

       using var producer = new Kafka.KafkaProducer<string, T>(topicDescriptor.Name, _context.Options);
       var result = await producer.ProduceAsync(key, entity);

       return result.Offset.Value;
   }

   public async Task ProduceBatchAsync(IEnumerable<T> entities)
   {
       if (entities == null)
           throw new ArgumentNullException(nameof(entities));

       var topicDescriptor = _schemaManager.GetTopicDescriptor<T>();

       using var producer = new Kafka.KafkaProducer<string, T>(topicDescriptor.Name, _context.Options);
       var keyValuePairs = new List<KeyValuePair<string, T>>();

       foreach (var entity in entities)
       {
           var key = _schemaManager.ExtractKey(entity) ?? Guid.NewGuid().ToString();
           keyValuePairs.Add(new KeyValuePair<string, T>(key, entity));
       }

       await producer.ProduceBatchAsync(keyValuePairs);
   }

   public IDisposable Subscribe(
       Action<T> onNext,
       Action<Exception>? onError = null,
       Action? onCompleted = null,
       CancellationToken cancellationToken = default)
   {
       if (onNext == null)
           throw new ArgumentNullException(nameof(onNext));

       var topicDescriptor = _schemaManager.GetTopicDescriptor<T>();
       var consumer = new Kafka.KafkaConsumer<string, T>(topicDescriptor.Name, _context.Options);

       var tokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
       var cancellationTokenLinked = tokenSource.Token;

       var task = Task.Run(() =>
       {
           try
           {
               foreach (var entity in consumer.Consume(cancellationTokenLinked))
               {
                   try
                   {
                       onNext(entity);
                   }
                   catch (Exception ex)
                   {
                       onError?.Invoke(ex);
                       
                       if (_errorAction == ErrorAction.Stop)
                       {
                           break;
                       }
                   }
               }

               onCompleted?.Invoke();
           }
           catch (OperationCanceledException)
           {
               onCompleted?.Invoke();
           }
           catch (Exception ex)
           {
               onError?.Invoke(ex);
           }
           finally
           {
               consumer.Dispose();
           }
       }, cancellationTokenLinked);

       return new KsqlSubscription(tokenSource, task);
   }

   public IKsqlStream<T> WithWatermark<TTimestamp>(Expression<Func<T, TTimestamp>> timestampSelector, TimeSpan maxOutOfOrderness)
   {
       if (timestampSelector == null)
           throw new ArgumentNullException(nameof(timestampSelector));

       var memberExpression = timestampSelector.Body as MemberExpression;
       if (memberExpression == null)
           throw new ArgumentException("Timestamp selector must be a property expression", nameof(timestampSelector));

       _streamProperties["WatermarkColumn"] = memberExpression.Member.Name;
       _streamProperties["MaxOutOfOrderness"] = maxOutOfOrderness;

       return this;
   }

   public IKsqlStream<T> OnError(ErrorAction errorAction)
   {
       _errorAction = errorAction;
       return this;
   }

   public IWindowedKsqlStream<T> Window(WindowSpecification window)
   {
       if (window == null)
           throw new ArgumentNullException(nameof(window));

       return new WindowedKsqlStream<T>(this, window);
   }

   public void Add(T entity)
   {
       if (entity == null)
           throw new ArgumentNullException(nameof(entity));

       _context.AddToPendingChanges(entity);
   }

   public IKsqlStream<TResult> Join<TRight, TKey, TResult>(
       IKsqlStream<TRight> rightStream,
       Expression<Func<T, TKey>> leftKeySelector,
       Expression<Func<TRight, TKey>> rightKeySelector,
       Expression<Func<T, TRight, TResult>> resultSelector,
       WindowSpecification window)
       where TRight : class, new()
       where TResult : class
   {
       if (rightStream == null) throw new ArgumentNullException(nameof(rightStream));
       if (leftKeySelector == null) throw new ArgumentNullException(nameof(leftKeySelector));
       if (rightKeySelector == null) throw new ArgumentNullException(nameof(rightKeySelector));
       if (resultSelector == null) throw new ArgumentNullException(nameof(resultSelector));
       if (window == null) throw new ArgumentNullException(nameof(window));

       // Extract property names from key selectors
       string leftKeyProperty = ExtractPropertyName(leftKeySelector);
       string rightKeyProperty = ExtractPropertyName(rightKeySelector);

       // Create a unique name for the result stream
       string resultStreamName = $"{Name}_{((KsqlStream<TRight>)rightStream).Name}_join_{Guid.NewGuid():N}";

       // Create the join condition
       string joinCondition = $"{Name}.{leftKeyProperty} = {((KsqlStream<TRight>)rightStream).Name}.{rightKeyProperty}";

       // Create the join operation
       var joinOperation = new JoinOperation(
           JoinType.Inner,
           Name,
           ((KsqlStream<TRight>)rightStream).Name,
           joinCondition,
           window.ToKsqlString());

       // Create a new stream for the join result
       var resultStream = new KsqlJoinStream<T, TRight, TResult>(
           resultStreamName,
           _context,
           _schemaManager,
           this,
           rightStream,
           joinOperation,
           resultSelector);

       return resultStream;
   }

   public IKsqlStream<TResult> Join<TRight, TKey, TResult>(
       IKsqlTable<TRight> table,
       Expression<Func<T, TKey>> leftKeySelector,
       Expression<Func<TRight, TKey>> rightKeySelector,
       Expression<Func<T, TRight, TResult>> resultSelector)
       where TRight : class
       where TResult : class
   {
       if (table == null) throw new ArgumentNullException(nameof(table));
       if (leftKeySelector == null) throw new ArgumentNullException(nameof(leftKeySelector));
       if (rightKeySelector == null) throw new ArgumentNullException(nameof(rightKeySelector));
       if (resultSelector == null) throw new ArgumentNullException(nameof(resultSelector));

       // Extract property names from key selectors
       string leftKeyProperty = ExtractPropertyName(leftKeySelector);
       string rightKeyProperty = ExtractPropertyName(rightKeySelector);

       // Create a unique name for the result stream
       string resultStreamName = $"{Name}_{((KsqlTable<TRight>)table).Name}_join_{Guid.NewGuid():N}";

       // Create the join condition
       string joinCondition = $"{Name}.{leftKeyProperty} = {((KsqlTable<TRight>)table).Name}.{rightKeyProperty}";

       // Create the join operation
       var joinOperation = new JoinOperation(
           JoinType.Inner,
           Name,
           ((KsqlTable<TRight>)table).Name,
           joinCondition);

       // Create a new stream for the join result
       var resultStream = new KsqlJoinStream<T, TRight, TResult>(
           resultStreamName,
           _context,
           _schemaManager,
           this,
           table,
           joinOperation,
           resultSelector);

       return resultStream;
   }

   public IKsqlStream<TResult> LeftJoin<TRight, TKey, TResult>(
       IKsqlTable<TRight> table,
       Expression<Func<T, TKey>> leftKeySelector,
       Expression<Func<TRight, TKey>> rightKeySelector,
       Expression<Func<T, TRight, TResult>> resultSelector)
       where TRight : class
       where TResult : class
   {
       if (table == null) throw new ArgumentNullException(nameof(table));
       if (leftKeySelector == null) throw new ArgumentNullException(nameof(leftKeySelector));
       if (rightKeySelector == null) throw new ArgumentNullException(nameof(rightKeySelector));
       if (resultSelector == null) throw new ArgumentNullException(nameof(resultSelector));

       // Extract property names from key selectors
       string leftKeyProperty = ExtractPropertyName(leftKeySelector);
       string rightKeyProperty = ExtractPropertyName(rightKeySelector);

       // Create a unique name for the result stream
       string resultStreamName = $"{Name}_{((KsqlTable<TRight>)table).Name}_leftjoin_{Guid.NewGuid():N}";

       // Create the join condition
       string joinCondition = $"{Name}.{leftKeyProperty} = {((KsqlTable<TRight>)table).Name}.{rightKeyProperty}";

       // Create the join operation
       var joinOperation = new JoinOperation(
           JoinType.Left,
           Name,
           ((KsqlTable<TRight>)table).Name,
           joinCondition);

       // Create a new stream for the join result
       var resultStream = new KsqlJoinStream<T, TRight, TResult>(
           resultStreamName,
           _context,
           _schemaManager,
           this,
           table,
           joinOperation,
           resultSelector);

       return resultStream;
   }

   public IEnumerator<T> GetEnumerator()
   {
       // This is a placeholder for implementing IEnumerable<T>
       // In a real implementation, this would execute a query against KSQL
       yield break;
   }

   IEnumerator IEnumerable.GetEnumerator()
   {
       return GetEnumerator();
   }

   private static string ExtractPropertyName<TSource, TProperty>(Expression<Func<TSource, TProperty>> propertySelector)
   {
       if (propertySelector.Body is MemberExpression memberExpression)
       {
           return memberExpression.Member.Name;
       }

       throw new ArgumentException("The expression must be a property selector.", nameof(propertySelector));
   }

   private class KsqlSubscription : IDisposable
   {
       private readonly CancellationTokenSource _cancellationTokenSource;
       private readonly Task _task;
       private bool _disposed;

       public KsqlSubscription(CancellationTokenSource cancellationTokenSource, Task task)
       {
           _cancellationTokenSource = cancellationTokenSource;
           _task = task;
       }

       public void Dispose()
       {
           if (!_disposed)
           {
               _cancellationTokenSource.Cancel();
               _cancellationTokenSource.Dispose();
               _disposed = true;
           }
       }
   }
}