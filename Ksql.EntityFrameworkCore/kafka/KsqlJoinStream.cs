using System.Collections;
using System.Linq.Expressions;
using Ksql.EntityFramework.Interfaces;
using Ksql.EntityFramework.Models;
using Ksql.EntityFramework.Schema;
using Ksql.EntityFramework.Windows;
using Ksql.EntityFrameworkCore.Models;

namespace Ksql.EntityFramework;

internal class KsqlJoinStream<TLeft, TRight, TResult> : IKsqlStream<TResult>
   where TLeft : class
   where TRight : class
   where TResult : class
{
    private readonly KsqlDbContext _context;
    private readonly SchemaManager _schemaManager;
    private readonly JoinOperation _joinOperation;
    private readonly Expression<Func<TLeft, TRight, TResult>> _resultSelector;
    private readonly object _leftSource; // IKsqlStream<TLeft> または KsqlStream<TLeft>
    private readonly object _rightSource; // IKsqlStream<TRight>, IKsqlTable<TRight> など
    private ErrorAction _errorAction = ErrorAction.Stop;

    public string Name { get; }

    public Type ElementType => typeof(TResult);

    public Expression Expression => System.Linq.Expressions.Expression.Constant(this);

    public IQueryProvider Provider => new KsqlQueryProvider();

    public KsqlJoinStream(
        string name,
        KsqlDbContext context,
        SchemaManager schemaManager,
        object leftSource,
        object rightSource,
        JoinOperation joinOperation,
        Expression<Func<TLeft, TRight, TResult>> resultSelector)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        _context = context ?? throw new ArgumentNullException(nameof(context));
        _schemaManager = schemaManager ?? throw new ArgumentNullException(nameof(schemaManager));
        _leftSource = leftSource ?? throw new ArgumentNullException(nameof(leftSource));
        _rightSource = rightSource ?? throw new ArgumentNullException(nameof(rightSource));
        _joinOperation = joinOperation ?? throw new ArgumentNullException(nameof(joinOperation));
        _resultSelector = resultSelector ?? throw new ArgumentNullException(nameof(resultSelector));

        // 実際の実装では、KSQL join streamを作成します
        CreateJoinStream();
    }

    private void CreateJoinStream()
    {
        // 実際の実装では、join を作成するKSQLステートメントを実行します
        // 例:
        // CREATE STREAM result_stream AS
        // SELECT * FROM left_stream JOIN right_stream
        // ON left_stream.key = right_stream.key
        // WITHIN 1 HOURS;

        Console.WriteLine($"Creating join stream: {Name}");
        Console.WriteLine($"Join operation: {_joinOperation.ToKsqlString()}");
    }

    public Task<long> ProduceAsync(TResult entity)
    {
        // join結果に対する直接のプロデュースはサポートされていません
        throw new NotSupportedException("Direct production to a join result stream is not supported.");
    }

    public Task<long> ProduceAsync(string key, TResult entity)
    {
        // join結果に対する直接のプロデュースはサポートされていません
        throw new NotSupportedException("Direct production to a join result stream is not supported.");
    }

    public Task ProduceBatchAsync(IEnumerable<TResult> entities)
    {
        // join結果に対する直接のプロデュースはサポートされていません
        throw new NotSupportedException("Direct production to a join result stream is not supported.");
    }

    public IDisposable Subscribe(
        Action<TResult> onNext,
        Action<Exception>? onError = null,
        Action? onCompleted = null,
        CancellationToken cancellationToken = default)
    {
        if (onNext == null)
            throw new ArgumentNullException(nameof(onNext));

        var subscription = new KsqlSubscription<TResult>(
            async () =>
            {
                try
                {
                    await foreach (var entity in SubscribeAsync().WithCancellation(cancellationToken))
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
            });

        // サブスクリプションを開始
        subscription.Start();

        return subscription;
    }

    public async IAsyncEnumerable<TResult> SubscribeAsync()
    {
        // 実際の実装では、join結果ストリームをサブスクライブします
        // プレースホルダとして、空のenumerableを返します
        await Task.CompletedTask;
        yield break;
    }

    public IKsqlStream<TResult> WithWatermark<TTimestamp>(Expression<Func<TResult, TTimestamp>> timestampSelector, TimeSpan maxOutOfOrderness)
    {
        // 実際の実装では、ストリームにウォーターマークを設定します
        return this;
    }

    public IKsqlStream<TResult> OnError(ErrorAction errorAction)
    {
        _errorAction = errorAction;
        return this;
    }

    public IWindowedKsqlStream<TResult> Window(WindowSpecification window)
    {
        return new WindowedKsqlStream<TResult>(this, window);
    }

    public void Add(TResult entity)
    {
        // join結果に対する直接の追加はサポートされていません
        throw new NotSupportedException("Direct addition to a join result stream is not supported.");
    }

    public IKsqlStream<TNewResult> Join<TJoinRight, TKey, TNewResult>(
        IKsqlStream<TJoinRight> rightStream,
        Expression<Func<TResult, TKey>> leftKeySelector,
        Expression<Func<TJoinRight, TKey>> rightKeySelector,
        Expression<Func<TResult, TJoinRight, TNewResult>> resultSelector,
        WindowSpecification window)
        where TJoinRight : class, new()
        where TNewResult : class
    {
        if (rightStream == null) throw new ArgumentNullException(nameof(rightStream));
        if (leftKeySelector == null) throw new ArgumentNullException(nameof(leftKeySelector));
        if (rightKeySelector == null) throw new ArgumentNullException(nameof(rightKeySelector));
        if (resultSelector == null) throw new ArgumentNullException(nameof(resultSelector));
        if (window == null) throw new ArgumentNullException(nameof(window));

        // プロパティ名をキーセレクターから抽出
        string leftKeyProperty = ExtractPropertyName(leftKeySelector);
        string rightKeyProperty = ExtractPropertyName(rightKeySelector);

        // 結果ストリームの一意な名前を作成
        string resultStreamName = $"{Name}_{((KsqlStream<TJoinRight>)rightStream).Name}_join_{Guid.NewGuid():N}";

        // join条件を作成
        string joinCondition = $"{Name}.{leftKeyProperty} = {((KsqlStream<TJoinRight>)rightStream).Name}.{rightKeyProperty}";

        // join操作を作成
        var joinOperation = new JoinOperation(
            JoinType.Inner,
            Name,
            ((KsqlStream<TJoinRight>)rightStream).Name,
            joinCondition,
            window.ToKsqlString());

        // join結果用の新しいストリームを作成
        var resultStream = new KsqlJoinStream<TResult, TJoinRight, TNewResult>(
            resultStreamName,
            _context,
            _schemaManager,
            this,
            rightStream,
            joinOperation,
            resultSelector);

        return resultStream;
    }

    public IKsqlStream<TNewResult> Join<TJoinRight, TKey, TNewResult>(
        IKsqlTable<TJoinRight> table,
        Expression<Func<TResult, TKey>> leftKeySelector,
        Expression<Func<TJoinRight, TKey>> rightKeySelector,
        Expression<Func<TResult, TJoinRight, TNewResult>> resultSelector)
        where TJoinRight : class
        where TNewResult : class
    {
        if (table == null) throw new ArgumentNullException(nameof(table));
        if (leftKeySelector == null) throw new ArgumentNullException(nameof(leftKeySelector));
        if (rightKeySelector == null) throw new ArgumentNullException(nameof(rightKeySelector));
        if (resultSelector == null) throw new ArgumentNullException(nameof(resultSelector));

        // プロパティ名をキーセレクターから抽出
        string leftKeyProperty = ExtractPropertyName(leftKeySelector);
        string rightKeyProperty = ExtractPropertyName(rightKeySelector);

        // 結果ストリームの一意な名前を作成
        string resultStreamName = $"{Name}_{((KsqlTable<TJoinRight>)table).Name}_join_{Guid.NewGuid():N}";

        // join条件を作成
        string joinCondition = $"{Name}.{leftKeyProperty} = {((KsqlTable<TJoinRight>)table).Name}.{rightKeyProperty}";

        // join操作を作成
        var joinOperation = new JoinOperation(
            JoinType.Inner,
            Name,
            ((KsqlTable<TJoinRight>)table).Name,
            joinCondition);

        // join結果用の新しいストリームを作成
        var resultStream = new KsqlJoinStream<TResult, TJoinRight, TNewResult>(
            resultStreamName,
            _context,
            _schemaManager,
            this,
            table,
            joinOperation,
            resultSelector);

        return resultStream;
    }

    public IKsqlStream<TNewResult> LeftJoin<TJoinRight, TKey, TNewResult>(
        IKsqlTable<TJoinRight> table,
        Expression<Func<TResult, TKey>> leftKeySelector,
        Expression<Func<TJoinRight, TKey>> rightKeySelector,
        Expression<Func<TResult, TJoinRight, TNewResult>> resultSelector)
        where TJoinRight : class
        where TNewResult : class
    {
        if (table == null) throw new ArgumentNullException(nameof(table));
        if (leftKeySelector == null) throw new ArgumentNullException(nameof(leftKeySelector));
        if (rightKeySelector == null) throw new ArgumentNullException(nameof(rightKeySelector));
        if (resultSelector == null) throw new ArgumentNullException(nameof(resultSelector));

        // プロパティ名をキーセレクターから抽出
        string leftKeyProperty = ExtractPropertyName(leftKeySelector);
        string rightKeyProperty = ExtractPropertyName(rightKeySelector);

        // 結果ストリームの一意な名前を作成
        string resultStreamName = $"{Name}_{((KsqlTable<TJoinRight>)table).Name}_leftjoin_{Guid.NewGuid():N}";

        // join条件を作成
        string joinCondition = $"{Name}.{leftKeyProperty} = {((KsqlTable<TJoinRight>)table).Name}.{rightKeyProperty}";

        // join操作を作成
        var joinOperation = new JoinOperation(
            JoinType.Left,
            Name,
            ((KsqlTable<TJoinRight>)table).Name,
            joinCondition);

        // join結果用の新しいストリームを作成
        var resultStream = new KsqlJoinStream<TResult, TJoinRight, TNewResult>(
            resultStreamName,
            _context,
            _schemaManager,
            this,
            table,
            joinOperation,
            resultSelector);

        return resultStream;
    }

    public IEnumerator<TResult> GetEnumerator()
    {
        // ストリームをenumerateするプレースホルダ実装
        // 実際の実装では、ストリームに対してクエリを実行します
        return Enumerable.Empty<TResult>().GetEnumerator();
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
}
