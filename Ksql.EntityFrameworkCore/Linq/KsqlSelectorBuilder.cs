using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Ksql.EntityFramework.Query.Expressions;

namespace Ksql.EntityFramework.Query.Expressions
{
    public class KsqlSelectorBuilder
    {
        private readonly KsqlExpressionVisitor _expressionVisitor;

        public KsqlSelectorBuilder(KsqlExpressionVisitor expressionVisitor)
        {
            _expressionVisitor = expressionVisitor ?? throw new ArgumentNullException(nameof(expressionVisitor));
        }

        public string BuildSelector<T, TResult>(Expression<Func<T, TResult>> selector)
        {
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            return BuildSelectorInternal(selector.Body);
        }

        private string BuildSelectorInternal(Expression expression)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.Parameter:
                    return "*";

                case ExpressionType.New:
                    return BuildNewExpression((NewExpression)expression);

                case ExpressionType.MemberInit:
                    return BuildMemberInitExpression((MemberInitExpression)expression);

                case ExpressionType.Call:
                    return _expressionVisitor.Visit(expression);

                case ExpressionType.MemberAccess:
                    return _expressionVisitor.Visit(expression);

                default:
                    return _expressionVisitor.Visit(expression);
            }
        }

        private string BuildNewExpression(NewExpression newExpression)
        {
            if (newExpression.Members == null)
            {
                throw new NotSupportedException("Anonymous types without member names are not supported in KSQL.");
            }

            List<string> projections = new List<string>();

            for (int i = 0; i < newExpression.Arguments.Count; i++)
            {
                var argument = newExpression.Arguments[i];
                var memberName = newExpression.Members[i].Name;
                var value = _expressionVisitor.Visit(argument);

                projections.Add($"{value} AS {memberName}");
            }

            return string.Join(", ", projections);
        }

        private string BuildMemberInitExpression(MemberInitExpression memberInitExpression)
        {
            List<string> projections = new List<string>();

            foreach (var binding in memberInitExpression.Bindings)
            {
                if (binding is MemberAssignment assignment)
                {
                    var memberName = assignment.Member.Name;
                    var value = _expressionVisitor.Visit(assignment.Expression);

                    projections.Add($"{value} AS {memberName}");
                }
                else
                {
                    throw new NotSupportedException($"Binding type {binding.BindingType} is not supported in KSQL.");
                }
            }

            return string.Join(", ", projections);
        }
        // KsqlSelectorBuilder ƒNƒ‰ƒX‚É’Ç‰Á
        public string BuildGroupSelector(LambdaExpression keySelector)
        {
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));

            switch (keySelector.Body.NodeType)
            {
                case ExpressionType.MemberAccess:
                    var memberAccess = (MemberExpression)keySelector.Body;
                    return memberAccess.Member.Name;

                case ExpressionType.New:
                    var newExpression = (NewExpression)keySelector.Body;

                    if (newExpression.Members == null)
                    {
                        throw new NotSupportedException("Anonymous types without member names are not supported for grouping in KSQL.");
                    }

                    return string.Join(", ", newExpression.Members.Select(m => m.Name));

                default:
                    throw new NotSupportedException($"Group key expression type {keySelector.Body.NodeType} is not supported in KSQL.");
            }
        }
        public string BuildGroupSelector<T, TKey>(Expression<Func<T, TKey>> keySelector)
        {
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));

            switch (keySelector.Body.NodeType)
            {
                case ExpressionType.MemberAccess:
                    var memberAccess = (MemberExpression)keySelector.Body;
                    return memberAccess.Member.Name;

                case ExpressionType.New:
                    var newExpression = (NewExpression)keySelector.Body;
                    
                    if (newExpression.Members == null)
                    {
                        throw new NotSupportedException("Anonymous types without member names are not supported for grouping in KSQL.");
                    }

                    return string.Join(", ", newExpression.Members.Select(m => m.Name));

                default:
                    throw new NotSupportedException($"Group key expression type {keySelector.Body.NodeType} is not supported in KSQL.");
            }
        }

        public string BuildStar()
        {
            return "*";
        }

        public string BuildCount()
        {
            return "COUNT(*)";
        }

        public string BuildAggregate<T, TResult>(Expression<Func<T, TResult>> selector, string aggregateFunction)
        {
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            if (string.IsNullOrEmpty(aggregateFunction))
                throw new ArgumentNullException(nameof(aggregateFunction));

            var column = _expressionVisitor.Visit(selector.Body);
            return $"{aggregateFunction}({column})";
        }

        public IEnumerable<string> GetSelectedColumns<T, TResult>(Expression<Func<T, TResult>> selector)
        {
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            if (typeof(TResult) == typeof(T))
            {
                return GetAllColumns<T>();
            }

            List<string> columns = new List<string>();
            CollectSelectedColumns(selector.Body, columns);
            return columns;
        }

        private IEnumerable<string> GetAllColumns<T>()
        {
            return typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Select(p => p.Name);
        }

        private void CollectSelectedColumns(Expression expression, List<string> columns)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.MemberAccess:
                    var memberAccess = (MemberExpression)expression;
                    if (memberAccess.Expression.NodeType == ExpressionType.Parameter)
                    {
                        columns.Add(memberAccess.Member.Name);
                    }
                    break;

                case ExpressionType.New:
                    var newExpression = (NewExpression)expression;
                    if (newExpression.Members != null)
                    {
                        foreach (var arg in newExpression.Arguments)
                        {
                            CollectSelectedColumns(arg, columns);
                        }
                    }
                    break;

                case ExpressionType.MemberInit:
                    var memberInit = (MemberInitExpression)expression;
                    foreach (var binding in memberInit.Bindings)
                    {
                        if (binding is MemberAssignment assignment)
                        {
                            CollectSelectedColumns(assignment.Expression, columns);
                        }
                    }
                    break;
            }
        }
    }
}