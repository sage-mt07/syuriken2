using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Ksql.EntityFramework.Query.Builders;
using Ksql.EntityFramework.Query.Expressions;

namespace Ksql.EntityFramework.Query.Translation
{
    public class KsqlQueryTranslator
    {
        private readonly KsqlExpressionVisitor _expressionVisitor;
        private readonly KsqlPredicateBuilder _predicateBuilder;
        private readonly KsqlSelectorBuilder _selectorBuilder;
        private readonly Dictionary<Type, string> _tableAliases;
        private int _aliasCounter;

        public KsqlQueryTranslator()
        {
            _expressionVisitor = new KsqlExpressionVisitor();
            _predicateBuilder = new KsqlPredicateBuilder(_expressionVisitor);
            _selectorBuilder = new KsqlSelectorBuilder(_expressionVisitor);
            _tableAliases = new Dictionary<Type, string>();
            _aliasCounter = 0;
        }

        public string Translate<T>(IQueryable<T> query)
        {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            var sqlBuilder = new KsqlSqlBuilder();
            var rootType = typeof(T);

            RegisterTable(rootType);
            VisitExpression(query.Expression, sqlBuilder, rootType);

            return sqlBuilder.Build();
        }

        private void VisitExpression(Expression expression, KsqlSqlBuilder sqlBuilder, Type resultType)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.Constant:
                    VisitConstantExpression((ConstantExpression)expression, sqlBuilder, resultType);
                    break;

                case ExpressionType.Call:
                    VisitMethodCallExpression((MethodCallExpression)expression, sqlBuilder, resultType);
                    break;

                default:
                    throw new NotSupportedException($"Expression type {expression.NodeType} is not supported in KSQL query translation.");
            }
        }

        private void VisitConstantExpression(ConstantExpression expression, KsqlSqlBuilder sqlBuilder, Type resultType)
        {
            if (expression.Value is IQueryable queryable)
            {
                string tableName = GetTableName(resultType);
                string alias = GetTableAlias(resultType);
                sqlBuilder.AppendSelect("*").AppendFrom(tableName, alias);
            }
            else
            {
                throw new NotSupportedException($"Constant expression of type {expression.Type} is not supported in KSQL query translation.");
            }
        }

        private void VisitMethodCallExpression(MethodCallExpression expression, KsqlSqlBuilder sqlBuilder, Type resultType)
        {
            if (expression.Method.DeclaringType == typeof(Queryable) || 
                expression.Method.DeclaringType == typeof(Enumerable))
            {
                VisitQueryableMethod(expression, sqlBuilder, resultType);
            }
            else
            {
                throw new NotSupportedException($"Method call {expression.Method.DeclaringType}.{expression.Method.Name} is not supported in KSQL query translation.");
            }
        }

        private void VisitQueryableMethod(MethodCallExpression expression, KsqlSqlBuilder sqlBuilder, Type resultType)
        {
            switch (expression.Method.Name)
            {
                case nameof(Queryable.Select):
                    VisitSelectMethod(expression, sqlBuilder, resultType);
                    break;

                case nameof(Queryable.Where):
                    VisitWhereMethod(expression, sqlBuilder, resultType);
                    break;

                case nameof(Queryable.OrderBy):
                case nameof(Queryable.OrderByDescending):
                    VisitOrderByMethod(expression, sqlBuilder, resultType, expression.Method.Name == nameof(Queryable.OrderByDescending));
                    break;

                case nameof(Queryable.ThenBy):
                case nameof(Queryable.ThenByDescending):
                    VisitThenByMethod(expression, sqlBuilder, resultType, expression.Method.Name == nameof(Queryable.ThenByDescending));
                    break;

                case nameof(Queryable.GroupBy):
                    VisitGroupByMethod(expression, sqlBuilder, resultType);
                    break;

                case nameof(Queryable.Take):
                    VisitTakeMethod(expression, sqlBuilder, resultType);
                    break;

                case nameof(Queryable.Skip):
                    VisitSkipMethod(expression, sqlBuilder, resultType);
                    break;

                case nameof(Queryable.Join):
                    VisitJoinMethod(expression, sqlBuilder, resultType);
                    break;

                case nameof(Queryable.GroupJoin):
                    throw new NotSupportedException("GroupJoin method is not supported in KSQL.");

                case nameof(Queryable.Distinct):
                    VisitDistinctMethod(expression, sqlBuilder, resultType);
                    break;

                case nameof(Queryable.Count):
                case nameof(Queryable.LongCount):
                    VisitCountMethod(expression, sqlBuilder, resultType);
                    break;

                case nameof(Queryable.Any):
                    VisitAnyMethod(expression, sqlBuilder, resultType);
                    break;

                case nameof(Queryable.All):
                    VisitAllMethod(expression, sqlBuilder, resultType);
                    break;

                case nameof(Queryable.First):
                case nameof(Queryable.FirstOrDefault):
                case nameof(Queryable.Single):
                case nameof(Queryable.SingleOrDefault):
                    VisitFirstOrSingleMethod(expression, sqlBuilder, resultType);
                    break;

                case nameof(Queryable.Sum):
                case nameof(Queryable.Average):
                case nameof(Queryable.Min):
                case nameof(Queryable.Max):
                    VisitAggregateMethod(expression, sqlBuilder, resultType);
                    break;

                default:
                    throw new NotSupportedException($"Queryable method {expression.Method.Name} is not supported in KSQL query translation.");
            }
        }

        private void VisitSelectMethod(MethodCallExpression expression, KsqlSqlBuilder sqlBuilder, Type resultType)
        {
            VisitExpression(expression.Arguments[0], sqlBuilder, GetElementType(expression.Arguments[0].Type));

            var selectorLambda = (LambdaExpression)StripQuotes(expression.Arguments[1]);
            var selectorBody = selectorLambda.Body;

            // 元の型と結果型を取得
            var sourceType = GetElementType(expression.Arguments[0].Type);
            var targetType = selectorBody.Type;

            // 型引数を明示的に指定
            var methodInfo = typeof(KsqlSelectorBuilder).GetMethod("BuildSelector").MakeGenericMethod(sourceType, targetType);
            var columns = (string)methodInfo.Invoke(_selectorBuilder, new object[] { selectorLambda });

            sqlBuilder.AppendSelect(columns);
        }

        private void VisitWhereMethod(MethodCallExpression expression, KsqlSqlBuilder sqlBuilder, Type resultType)
        {
            VisitExpression(expression.Arguments[0], sqlBuilder, resultType);

            var predicateLambda = (LambdaExpression)StripQuotes(expression.Arguments[1]);

            // 型パラメーターを取得
            var sourceType = GetElementType(expression.Arguments[0].Type);

            // ジェネリックメソッドを動的に呼び出す
            var methodInfo = typeof(KsqlPredicateBuilder).GetMethod("Build").MakeGenericMethod(sourceType);
            var whereClause = (string)methodInfo.Invoke(_predicateBuilder, new object[] { predicateLambda });

            sqlBuilder.AppendWhere(whereClause);
        }

        private void VisitOrderByMethod(MethodCallExpression expression, KsqlSqlBuilder sqlBuilder, Type resultType, bool descending)
        {
            VisitExpression(expression.Arguments[0], sqlBuilder, resultType);

            var keySelectorLambda = (LambdaExpression)StripQuotes(expression.Arguments[1]);
            var keySelector = _expressionVisitor.Visit(keySelectorLambda.Body);
            
            var orderByClause = descending ? $"{keySelector} DESC" : $"{keySelector} ASC";
            sqlBuilder.AppendOrderBy(orderByClause);
        }

        private void VisitThenByMethod(MethodCallExpression expression, KsqlSqlBuilder sqlBuilder, Type resultType, bool descending)
        {
            VisitExpression(expression.Arguments[0], sqlBuilder, resultType);

            var keySelectorLambda = (LambdaExpression)StripQuotes(expression.Arguments[1]);
            var keySelector = _expressionVisitor.Visit(keySelectorLambda.Body);
            
            var orderByClause = sqlBuilder.ToString();
            var orderByParts = orderByClause.Split(new[] { " ORDER BY " }, StringSplitOptions.None);
            
            if (orderByParts.Length != 2)
                throw new InvalidOperationException("Invalid ORDER BY clause in the query.");
            
            var existingOrderBy = orderByParts[1];
            var newOrderBy = descending ? $"{existingOrderBy}, {keySelector} DESC" : $"{existingOrderBy}, {keySelector} ASC";
            
            sqlBuilder.AppendOrderBy(newOrderBy);
        }

        private void VisitGroupByMethod(MethodCallExpression expression, KsqlSqlBuilder sqlBuilder, Type resultType)
        {
            VisitExpression(expression.Arguments[0], sqlBuilder, GetElementType(expression.Arguments[0].Type));

            var keySelectorLambda = (LambdaExpression)StripQuotes(expression.Arguments[1]);
            var groupByClause = _selectorBuilder.BuildGroupSelector(keySelectorLambda);
            
            sqlBuilder.AppendGroupBy(groupByClause);
        }

        private void VisitTakeMethod(MethodCallExpression expression, KsqlSqlBuilder sqlBuilder, Type resultType)
        {
            VisitExpression(expression.Arguments[0], sqlBuilder, resultType);

            var countExpression = expression.Arguments[1];
            if (countExpression.NodeType != ExpressionType.Constant)
                throw new NotSupportedException("Take method requires a constant count expression.");
            
            var count = (int)((ConstantExpression)countExpression).Value;
            sqlBuilder.AppendLimit(count);
        }

        private void VisitSkipMethod(MethodCallExpression expression, KsqlSqlBuilder sqlBuilder, Type resultType)
        {
            VisitExpression(expression.Arguments[0], sqlBuilder, resultType);

            var countExpression = expression.Arguments[1];
            if (countExpression.NodeType != ExpressionType.Constant)
                throw new NotSupportedException("Skip method requires a constant count expression.");
            
            var count = (int)((ConstantExpression)countExpression).Value;
            sqlBuilder.AppendOffset(count);
        }

        private void VisitJoinMethod(MethodCallExpression expression, KsqlSqlBuilder sqlBuilder, Type resultType)
        {
            var outerSourceType = GetElementType(expression.Arguments[0].Type);
            VisitExpression(expression.Arguments[0], sqlBuilder, outerSourceType);

            var innerSourceExpression = expression.Arguments[1];
            var innerSourceType = GetElementType(innerSourceExpression.Type);
            var innerSourceTableName = GetTableName(innerSourceType);
            var innerSourceAlias = RegisterTable(innerSourceType);

            var outerKeySelectorLambda = (LambdaExpression)StripQuotes(expression.Arguments[2]);
            var innerKeySelectorLambda = (LambdaExpression)StripQuotes(expression.Arguments[3]);
            
            var outerKeySelector = _expressionVisitor.Visit(outerKeySelectorLambda.Body);
            var innerKeySelector = _expressionVisitor.Visit(innerKeySelectorLambda.Body);
            
            var joinCondition = $"{GetTableAlias(outerSourceType)}.{outerKeySelector} = {innerSourceAlias}.{innerKeySelector}";
            
            sqlBuilder.AppendJoin(innerSourceTableName, joinCondition, "JOIN", innerSourceAlias);
        }

        private void VisitDistinctMethod(MethodCallExpression expression, KsqlSqlBuilder sqlBuilder, Type resultType)
        {
            VisitExpression(expression.Arguments[0], sqlBuilder, resultType);
            
            var selectClause = sqlBuilder.ToString();
            var selectParts = selectClause.Split(new[] { " SELECT " }, StringSplitOptions.None);
            
            if (selectParts.Length != 2)
                throw new InvalidOperationException("Invalid SELECT clause in the query.");
            
            var columns = selectParts[1].Split(new[] { " FROM " }, StringSplitOptions.None)[0];
            sqlBuilder.AppendSelect(columns, true);
        }

        private void VisitCountMethod(MethodCallExpression expression, KsqlSqlBuilder sqlBuilder, Type resultType)
        {
            if (expression.Arguments.Count == 1)
            {
                VisitExpression(expression.Arguments[0], sqlBuilder, GetElementType(expression.Arguments[0].Type));
                sqlBuilder.AppendSelect("COUNT(*)");
            }
            else if (expression.Arguments.Count == 2)
            {
                VisitExpression(expression.Arguments[0], sqlBuilder, GetElementType(expression.Arguments[0].Type));

                var predicateLambda = (LambdaExpression)StripQuotes(expression.Arguments[1]);

                // 型パラメーターを取得
                var sourceType = GetElementType(expression.Arguments[0].Type);

                // ジェネリックメソッドを動的に呼び出す
                var methodInfo = typeof(KsqlPredicateBuilder).GetMethod("Build").MakeGenericMethod(sourceType);
                var whereClause = (string)methodInfo.Invoke(_predicateBuilder, new object[] { predicateLambda });

                sqlBuilder.AppendWhere(whereClause);
                sqlBuilder.AppendSelect("COUNT(*)");
            }
        }

        private void VisitAnyMethod(MethodCallExpression expression, KsqlSqlBuilder sqlBuilder, Type resultType)
        {
            if (expression.Arguments.Count == 1)
            {
                VisitExpression(expression.Arguments[0], sqlBuilder, GetElementType(expression.Arguments[0].Type));
                sqlBuilder.AppendSelect("CASE WHEN COUNT(*) > 0 THEN TRUE ELSE FALSE END AS AnyResult");
            }
            else if (expression.Arguments.Count == 2)
            {
                VisitExpression(expression.Arguments[0], sqlBuilder, GetElementType(expression.Arguments[0].Type));
                
                var predicateLambda = (LambdaExpression)StripQuotes(expression.Arguments[1]);
                var whereClause = _predicateBuilder.Build(predicateLambda);
                
                sqlBuilder.AppendWhere(whereClause);
                sqlBuilder.AppendSelect("CASE WHEN COUNT(*) > 0 THEN TRUE ELSE FALSE END AS AnyResult");
            }
        }

        private void VisitAllMethod(MethodCallExpression expression, KsqlSqlBuilder sqlBuilder, Type resultType)
        {
            if (expression.Arguments.Count == 2)
            {
                VisitExpression(expression.Arguments[0], sqlBuilder, GetElementType(expression.Arguments[0].Type));
                
                var predicateLambda = (LambdaExpression)StripQuotes(expression.Arguments[1]);
                var negatedPredicate = _predicateBuilder.BuildNegation(_predicateBuilder.Build(predicateLambda));
                
                sqlBuilder.AppendWhere(negatedPredicate);
                sqlBuilder.AppendSelect("CASE WHEN COUNT(*) = 0 THEN TRUE ELSE FALSE END AS AllResult");
            }
        }

        private void VisitFirstOrSingleMethod(MethodCallExpression expression, KsqlSqlBuilder sqlBuilder, Type resultType)
        {
            if (expression.Arguments.Count == 1)
            {
                VisitExpression(expression.Arguments[0], sqlBuilder, GetElementType(expression.Arguments[0].Type));
                sqlBuilder.AppendLimit(1);
            }
            else if (expression.Arguments.Count == 2)
            {
                VisitExpression(expression.Arguments[0], sqlBuilder, GetElementType(expression.Arguments[0].Type));
                
                var predicateLambda = (LambdaExpression)StripQuotes(expression.Arguments[1]);
                var whereClause = _predicateBuilder.Build(predicateLambda);
                
                sqlBuilder.AppendWhere(whereClause);
                sqlBuilder.AppendLimit(1);
            }
        }

        private void VisitAggregateMethod(MethodCallExpression expression, KsqlSqlBuilder sqlBuilder, Type resultType)
        {
            var aggregateFunction = expression.Method.Name.ToUpper();
            
            if (expression.Arguments.Count == 1)
            {
                VisitExpression(expression.Arguments[0], sqlBuilder, GetElementType(expression.Arguments[0].Type));
                sqlBuilder.AppendSelect($"{aggregateFunction}(*)");
            }
            else if (expression.Arguments.Count == 2)
            {
                VisitExpression(expression.Arguments[0], sqlBuilder, GetElementType(expression.Arguments[0].Type));
                
                var selectorLambda = (LambdaExpression)StripQuotes(expression.Arguments[1]);
                var selector = _expressionVisitor.Visit(selectorLambda.Body);
                
                sqlBuilder.AppendSelect($"{aggregateFunction}({selector})");
            }
        }

        private string GetTableName(Type entityType)
        {
            return entityType.Name.ToLowerInvariant();
        }

        private string? RegisterTable(Type entityType)
        {
            if (!_tableAliases.ContainsKey(entityType))
            {
                _tableAliases[entityType] = $"t{_aliasCounter++}";
            }
            
            return GetTableAlias(entityType);
        }

        private string? GetTableAlias(Type entityType)
        {
            return _tableAliases.TryGetValue(entityType, out var alias) ? alias : null;
        }

        private static Type GetElementType(Type sequenceType)
        {
            var ienum = FindGenericType(typeof(IEnumerable<>), sequenceType);
            return ienum?.GetGenericArguments()[0];
        }

        private static Type? FindGenericType(Type genericType, Type type)
        {
            while (type != null && type != typeof(object))
            {
                if (type.IsGenericType && type.GetGenericTypeDefinition() == genericType)
                {
                    return type;
                }
                
                if (genericType.IsInterface)
                {
                    foreach (var interfaceType in type.GetInterfaces())
                    {
                        var found = FindGenericType(genericType, interfaceType);
                        if (found != null)
                        {
                            return found;
                        }
                    }
                }
                
                type = type?.BaseType;
            }
            
            return null;
        }

        private static Expression StripQuotes(Expression expression)
        {
            while (expression.NodeType == ExpressionType.Quote)
            {
                expression = ((UnaryExpression)expression).Operand;
            }
            
            return expression;
        }
    }
}