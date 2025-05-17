using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Ksql.EntityFramework.Interfaces;
using Ksql.EntityFramework.Models;
using Ksql.EntityFramework.Query.Execution;
using Ksql.EntityFramework.Query.Translation;
using Ksql.EntityFramework.Schema;
using Ksql.EntityFrameworkCore.Models;

namespace Ksql.EntityFramework
{
    public class KsqlStreamBase<T> : IKsqlStream<T>, IQueryable<T> where T : class, new()
    {
        private readonly KsqlDbContext _context;
        private readonly SchemaManager _schemaManager;
        private readonly KsqlEntityMap<T> _entityMap;
        private readonly KsqlQueryTranslator _queryTranslator;
        private readonly KsqlQueryExecutor _queryExecutor;
        private readonly Expression _expression;
        private readonly IQueryProvider _provider;
        private readonly string _streamName;
        private ErrorAction _errorAction = ErrorAction.Stop;

        public KsqlStreamBase(string name, KsqlDbContext context, SchemaManager schemaManager)
        {
            _streamName = name ?? throw new ArgumentNullException(nameof(name));
            _context = context ?? throw new ArgumentNullException(nameof(context));
            _schemaManager = schemaManager ?? throw new ArgumentNullException(nameof(schemaManager));
            _entityMap = new KsqlEntityMap<T>();
            _queryTranslator = new KsqlQueryTranslator();
            _queryExecutor = new KsqlQueryExecutor(context.Options);
            _expression = Expression.Constant(this);
            _provider = new KsqlQueryProvider();
        }

        // IQueryable メンバー
        public Type ElementType => typeof(T);
        public Expression Expression => _expression;
        public IQueryProvider Provider => _provider;

        // IKsqlStream メンバー
        public string Name => _streamName;

        public async Task<long> ProduceAsync(T entity)
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));

            // 自動生成キーでProduceを処理
            string key = GenerateAutoKey(entity);
            return await ProduceAsync(key, entity);
        }

        public async Task<long> ProduceAsync(string key, T entity)
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));

            try
            {
                using var producer = new Kafka.KafkaProducer<string, T>(_entityMap.TopicName, _context.Options);
                var result = await producer.ProduceAsync(key, entity);
                return result.Offset.Value;
            }
            catch (Exception ex)
            {
                HandleProduceError(ex, entity);
                throw;
            }
        }

        public async Task ProduceBatchAsync(IEnumerable<T> entities)
        {
            if (entities == null)
                throw new ArgumentNullException(nameof(entities));

            try
            {
                using var producer = new Kafka.KafkaProducer<string, T>(_entityMap.TopicName, _context.Options);
                var keyValuePairs = entities.Select(e => new KeyValuePair<string, T>(GenerateAutoKey(e), e));
                await producer.ProduceBatchAsync(keyValuePairs);
            }
            catch (Exception ex)
            {
                HandleProduceError(ex, entities);
                throw;
            }
        }

        public IDisposable Subscribe(
            Action<T> onNext,
            Action<Exception> ?onError = null,
            Action? onCompleted = null,
            CancellationToken cancellationToken = default)
        {
            if (onNext == null)
                throw new ArgumentNullException(nameof(onNext));

            var subscription = new KsqlSubscription<T>(
                async () =>
                {
                    try
                    {
                        var queryText = _queryTranslator.Translate(this);
                        var streamResult = await _queryExecutor.ExecuteStreamQueryAsync<T>(queryText);

                        await foreach (var entity in streamResult.ReadResultsAsync().WithCancellation(cancellationToken))
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
                        // サブスクリプションがキャンセルされた - これは想定内
                        onCompleted?.Invoke();
                    }
                    catch (Exception ex)
                    {
                        onError?.Invoke(ex);
                    }
                },
                cancellationToken);

            // サブスクリプションを開始
            subscription.Start();
            return subscription;
        }

        public IKsqlStream<T> WithWatermark<TTimestamp>(Expression<Func<T, TTimestamp>> timestampSelector, TimeSpan maxOutOfOrderness)
        {
            if (timestampSelector == null)
                throw new ArgumentNullException(nameof(timestampSelector));

            // ウォーターマーク設定を適用したストリームを返す
            // 実際の実装ではウォーターマーク設定を保持する必要がある
            return this;
        }

        public IKsqlStream<T> OnError(ErrorAction errorAction)
        {
            _errorAction = errorAction;
            return this;
        }

        public IWindowedKsqlStream<T> Window(Windows.WindowSpecification window)
        {
            if (window == null)
                throw new ArgumentNullException(nameof(window));

            return new Windows.WindowedKsqlStream<T>(this, window);
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
            Windows.WindowSpecification window)
            where TRight : class, new()
            where TResult : class
        {
            if (rightStream == null) throw new ArgumentNullException(nameof(rightStream));
            if (leftKeySelector == null) throw new ArgumentNullException(nameof(leftKeySelector));
            if (rightKeySelector == null) throw new ArgumentNullException(nameof(rightKeySelector));
            if (resultSelector == null) throw new ArgumentNullException(nameof(resultSelector));
            if (window == null) throw new ArgumentNullException(nameof(window));

            // ストリーム結合を表すストリームを作成
            return new KsqlJoinStream<T, TRight, TResult>(
                $"{_streamName}_{((KsqlStreamBase<TRight>)rightStream).Name}_join",
                _context,
                _schemaManager,
                this,
                rightStream,
                new Models.JoinOperation(Models.JoinType.Inner, _streamName, ((KsqlStreamBase<TRight>)rightStream).Name, ""),
                resultSelector);
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

            // ストリームとテーブルの結合を表すストリームを作成
            return new KsqlJoinStream<T, TRight, TResult>(
                $"{_streamName}_{((KsqlTable<TRight>)table).Name}_join",
                _context,
                _schemaManager,
                this,
                table,
                new Models.JoinOperation(Models.JoinType.Inner, _streamName, ((KsqlTable<TRight>)table).Name, ""),
                resultSelector);
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

            // 左結合を表すストリームを作成
            return new KsqlJoinStream<T, TRight, TResult>(
                $"{_streamName}_{((KsqlTable<TRight>)table).Name}_leftjoin",
                _context,
                _schemaManager,
                this,
                table,
                new Models.JoinOperation(Models.JoinType.Left, _streamName, ((KsqlTable<TRight>)table).Name, ""),
                resultSelector);
        }

        public IEnumerator<T> GetEnumerator()
        {
            // IEnumerableの実装 - 同期的に結果を取得
            var task = ExecuteQuery();
            return task.GetAwaiter().GetResult().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        private async Task<List<T>> ExecuteQuery()
        {
            try
            {
                string ksqlQuery = _queryTranslator.Translate(this);
                var streamResult = await _queryExecutor.ExecuteStreamQueryAsync<T>(ksqlQuery);
                
                var results = new List<T>();
                await foreach (var entity in streamResult.ReadResultsAsync())
                {
                    results.Add(entity);
                }
                
                return results;
            }
            catch (Exception ex)
            {
                throw new KsqlQueryExecutionException($"Failed to execute query: {ex.Message}", ex);
            }
        }

        private string GenerateAutoKey(T entity)
        {
            if (_entityMap.KeyProperties.Count == 0)
                return Guid.NewGuid().ToString();

            // キープロパティの値を連結してキーを生成
            var keyValues = _entityMap.KeyProperties.Values
                .Select(p => p.GetValue(entity)?.ToString() ?? "null");
                
            return string.Join("_", keyValues);
        }

        private void HandleProduceError(Exception ex, object entity)
        {
            // エラーポリシーに基づいてエラーを処理
            var errorPolicy = _context.Options.DeserializationErrorPolicy;
            
            switch (errorPolicy)
            {
                case ErrorPolicy.Abort:
                    throw ex;
                    
                case ErrorPolicy.Skip:
                    // ログに記録して続行
                    Console.WriteLine($"KSQL produce error skipped: {ex.Message}");
                    break;
                    
                case ErrorPolicy.DeadLetterQueue:
                    if (!string.IsNullOrEmpty(_context.Options.DeadLetterQueue))
                    {
                        try
                        {
                            SendToDeadLetterQueue(entity, ex);
                        }
                        catch (Exception dlqEx)
                        {
                            Console.WriteLine($"Failed to send to dead letter queue: {dlqEx.Message}");
                            throw ex; // 元の例外をスロー
                        }
                    }
                    else
                    {
                        throw ex;
                    }
                    break;
                    
                default:
                    throw ex;
            }
        }

        private void SendToDeadLetterQueue(object entity, Exception ex)
        {
            // デッドレターキューにメッセージを送信
            // 実際の実装ではシリアライズしたエンティティとメタデータを送信する必要がある
            var deadLetterMessage = new DeadLetterMessage
            {
                ErrorMessage = ex.Message,
                Timestamp = DateTimeOffset.UtcNow,
                SourceTopic = _entityMap.TopicName,
                ErrorContext = entity?.ToString()
            };
            
            if (_context.Options.DeadLetterQueueErrorHandler != null)
            {
                deadLetterMessage = _context.Options.DeadLetterQueueErrorHandler(null, ex);
            }
            
            // デッドレターキューへの送信ロジック
            // ...
        }
    }

    public class KsqlSubscription<T> : IDisposable where T : class
    {
        private readonly Func<Task> _subscriptionTask;
        private readonly CancellationTokenSource _cts;
        private Task _runningTask;
        private bool _disposed;

        public KsqlSubscription(Func<Task> subscriptionTask, CancellationToken externalToken = default)
        {
            _subscriptionTask = subscriptionTask ?? throw new ArgumentNullException(nameof(subscriptionTask));
            _cts = CancellationTokenSource.CreateLinkedTokenSource(externalToken);
        }

        public void Start()
        {
            if (_disposed) throw new ObjectDisposedException(nameof(KsqlSubscription<T>));
            if (_runningTask != null) throw new InvalidOperationException("Subscription is already running.");

            _runningTask = Task.Run(_subscriptionTask);
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _cts.Cancel();
                _cts.Dispose();
                _disposed = true;
            }
        }
    }
}