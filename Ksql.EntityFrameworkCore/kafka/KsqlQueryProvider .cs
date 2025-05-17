// Ksql.EntityFramework/KsqlQueryProvider.cs
using System.Linq.Expressions;

namespace Ksql.EntityFramework;

internal class KsqlQueryProvider : IQueryProvider
{
    public IQueryable CreateQuery(Expression expression)
    {
        var elementType = expression.Type.GetGenericArguments()[0];
        var queryType = typeof(KsqlQuery<>).MakeGenericType(elementType);
        var constructor = queryType.GetConstructor(new[] { typeof(Expression), typeof(IQueryProvider) });

        return (IQueryable)constructor.Invoke(new object[] { expression, this });
    }

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        return new KsqlQuery<TElement>(expression, this);
    }

    public object Execute(Expression expression)
    {
        throw new NotImplementedException("Executing KSQL queries synchronously is not supported.");
    }

    public TResult Execute<TResult>(Expression expression)
    {
        throw new NotImplementedException("Executing KSQL queries synchronously is not supported.");
    }
}