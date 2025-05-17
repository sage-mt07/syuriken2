using System.Collections;
using System.Linq.Expressions;

namespace Ksql.EntityFramework;

internal class KsqlQuery<T> : IQueryable<T>, IOrderedQueryable<T>
{
    public Type ElementType => typeof(T);

    public Expression Expression { get; }

    public IQueryProvider Provider { get; }

    public KsqlQuery(Expression expression, IQueryProvider provider)
    {
        Expression = expression ?? throw new ArgumentNullException(nameof(expression));
        Provider = provider ?? throw new ArgumentNullException(nameof(provider));
    }

    public IEnumerator<T> GetEnumerator()
    {
        // これはプレースホルダ実装です - 実際の実装ではKSQLクエリを実行して結果を返します
        return Enumerable.Empty<T>().GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}