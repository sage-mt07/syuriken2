using System;
using System.Linq.Expressions;
using Ksql.EntityFramework.Query.Translation;

namespace Ksql.EntityFramework.Query.Expressions;

public class KsqlPredicateBuilder
{
    private readonly KsqlExpressionVisitor _expressionVisitor;

    public KsqlPredicateBuilder(KsqlExpressionVisitor expressionVisitor)
    {
        _expressionVisitor = expressionVisitor ?? throw new ArgumentNullException(nameof(expressionVisitor));
    }

    public string Build<T>(Expression<Func<T, bool>> predicate)
    {
        if (predicate == null)
            throw new ArgumentNullException(nameof(predicate));

        return _expressionVisitor.Visit(predicate.Body);
    }
    // KsqlPredicateBuilder クラスに追加
    public string Build(LambdaExpression predicate)
    {
        if (predicate == null)
            throw new ArgumentNullException(nameof(predicate));

        if (predicate.Parameters.Count != 1)
            throw new ArgumentException("Predicate must have exactly one parameter", nameof(predicate));

        // 型パラメーターを動的に取得
        var parameterType = predicate.Parameters[0].Type;
        var body = predicate.Body;

        // 式を訪問してKSQL条件を生成
        return _expressionVisitor.Visit(body);
    }
    public string BuildWithParameter<T>(Expression<Func<T, bool>> predicate, Expression parameter)
    {
        if (predicate == null)
            throw new ArgumentNullException(nameof(predicate));

        if (parameter == null)
            throw new ArgumentNullException(nameof(parameter));

        var body = SubstituteParameter(predicate.Body, predicate.Parameters[0], parameter);
        return _expressionVisitor.Visit(body);
    }

    private Expression SubstituteParameter(Expression expression, ParameterExpression source, Expression target)
    {
        var visitor = new ParameterSubstitutionVisitor(source, target);
        return visitor.Visit(expression);
    }

    private class ParameterSubstitutionVisitor : ExpressionVisitor
    {
        private readonly ParameterExpression _source;
        private readonly Expression _target;

        public ParameterSubstitutionVisitor(ParameterExpression source, Expression target)
        {
            _source = source;
            _target = target;
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            return node == _source ? _target : base.VisitParameter(node);
        }
    }

    public string BuildConjunction(string leftPredicate, string rightPredicate)
    {
        if (string.IsNullOrEmpty(leftPredicate))
            return rightPredicate;

        if (string.IsNullOrEmpty(rightPredicate))
            return leftPredicate;

        return $"({leftPredicate} AND {rightPredicate})";
    }

    public string BuildDisjunction(string leftPredicate, string rightPredicate)
    {
        if (string.IsNullOrEmpty(leftPredicate))
            return rightPredicate;

        if (string.IsNullOrEmpty(rightPredicate))
            return leftPredicate;

        return $"({leftPredicate} OR {rightPredicate})";
    }

    public string BuildNegation(string predicate)
    {
        if (string.IsNullOrEmpty(predicate))
            return string.Empty;

        return $"NOT ({predicate})";
    }

    public string BuildExists<T>(System.Linq.IQueryable<T> subquery)
    {
        if (subquery == null)
            throw new ArgumentNullException(nameof(subquery));

        // KSQLでは EXISTS をサポートしていないため、近似的な実装が必要
        // 一般的には COUNT(*) > 0 としてサポートされる
        throw new NotSupportedException("EXISTS subqueries are not directly supported in KSQL.");
    }

    public string BuildIn<T, TValue>(Expression<Func<T, TValue>> selector, params TValue[] values)
    {
        if (selector == null)
            throw new ArgumentNullException(nameof(selector));

        if (values == null || values.Length == 0)
            return "1=0"; // 常に偽の条件

        //  Ksql.EntityFramework.Query.Translation
       

            var column = _expressionVisitor.Visit(selector.Body);
        var valueList = string.Join(", ", Array.ConvertAll(values, v => 
            KsqlTypeMapper.GetKsqlLiteral(v)));

        return $"{column} IN ({valueList})";
    }

    public string BuildBetween<T, TValue>(Expression<Func<T, TValue>> selector, TValue min, TValue max)
        where TValue : IComparable<TValue>
    {
        if (selector == null)
            throw new ArgumentNullException(nameof(selector));

        var column = _expressionVisitor.Visit(selector.Body);
        var minValue = KsqlTypeMapper.GetKsqlLiteral(min);
        var maxValue = KsqlTypeMapper.GetKsqlLiteral(max);

        return $"{column} BETWEEN {minValue} AND {maxValue}";
    }

    public string BuildLike<T>(Expression<Func<T, string>> selector, string pattern)
    {
        if (selector == null)
            throw new ArgumentNullException(nameof(selector));

        if (pattern == null)
            throw new ArgumentNullException(nameof(pattern));

        var column = _expressionVisitor.Visit(selector.Body);
        var escapedPattern = pattern.Replace("'", "''");

        return $"{column} LIKE '{escapedPattern}'";
    }

    public string BuildIsNull<T, TValue>(Expression<Func<T, TValue>> selector)
    {
        if (selector == null)
            throw new ArgumentNullException(nameof(selector));

        var column = _expressionVisitor.Visit(selector.Body);
        return $"{column} IS NULL";
    }

    public string BuildIsNotNull<T, TValue>(Expression<Func<T, TValue>> selector)
    {
        if (selector == null)
            throw new ArgumentNullException(nameof(selector));

        var column = _expressionVisitor.Visit(selector.Body);
        return $"{column} IS NOT NULL";
    }
}