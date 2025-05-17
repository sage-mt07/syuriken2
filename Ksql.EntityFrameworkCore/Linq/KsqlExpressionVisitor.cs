using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Ksql.EntityFramework.Query.Translation;

namespace Ksql.EntityFramework.Query.Expressions
{
    public class KsqlExpressionVisitor
    {
        private readonly KsqlMethodCallTranslator _methodCallTranslator;
        private readonly Dictionary<Expression, string> _expressionCache;

        public KsqlExpressionVisitor()
        {
            _expressionCache = new Dictionary<Expression, string>();
            _methodCallTranslator = new KsqlMethodCallTranslator(this);
        }

        public string Visit(Expression expression)
        {
            if (expression == null)
                return string.Empty;

            if (_expressionCache.TryGetValue(expression, out var cachedResult))
                return cachedResult;

            string result = VisitExpression(expression);
            _expressionCache[expression] = result;
            return result;
        }

        private string VisitExpression(Expression expression)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.Constant:
                    return VisitConstant((ConstantExpression)expression);
                case ExpressionType.MemberAccess:
                    return VisitMemberAccess((MemberExpression)expression);
                case ExpressionType.Call:
                    return VisitMethodCall((MethodCallExpression)expression);
                case ExpressionType.Add:
                case ExpressionType.Subtract:
                case ExpressionType.Multiply:
                case ExpressionType.Divide:
                case ExpressionType.Modulo:
                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.AndAlso:
                case ExpressionType.OrElse:
                case ExpressionType.And:
                case ExpressionType.Or:
                case ExpressionType.Coalesce:
                    return VisitBinary((BinaryExpression)expression);
                case ExpressionType.Not:
                case ExpressionType.Negate:
                    return VisitUnary((UnaryExpression)expression);
                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                    return VisitConvert((UnaryExpression)expression);
                case ExpressionType.Lambda:
                    return VisitLambda((LambdaExpression)expression);
                case ExpressionType.New:
                    return VisitNew((NewExpression)expression);
                case ExpressionType.Conditional:
                    return VisitConditional((ConditionalExpression)expression);
                case ExpressionType.Parameter:
                    return VisitParameter((ParameterExpression)expression);
                case ExpressionType.NewArrayInit:
                    return VisitNewArray((NewArrayExpression)expression);
                default:
                    throw new NotSupportedException($"Expression type {expression.NodeType} is not supported in KSQL.");
            }
        }

        private string VisitConstant(ConstantExpression constantExpression)
        {
            if (constantExpression?.Value == null ) throw new ArgumentNullException("VisitConstant ConstantExpression null");
            return KsqlTypeMapper.GetKsqlLiteral(constantExpression.Value);
        }

        private string VisitMemberAccess(MemberExpression memberExpression)
        {
            if (memberExpression.Expression != null)
            {
                if (memberExpression.Expression.NodeType == ExpressionType.Parameter)
                {
                    return memberExpression.Member.Name;
                }
                else if (memberExpression.Expression.NodeType == ExpressionType.Constant)
                {
                    var container = ((ConstantExpression)memberExpression.Expression).Value;
                    var value = memberExpression.Member.GetMemberValue(container);
                    return KsqlTypeMapper.GetKsqlLiteral(value);
                }
                else if (memberExpression.Expression.NodeType == ExpressionType.MemberAccess)
                {
                    var parentMember = Visit(memberExpression.Expression);
                    return $"{parentMember}.{memberExpression.Member.Name}";
                }
            }

            throw new NotSupportedException($"Member access expression for {memberExpression} is not supported in KSQL.");
        }

        private string VisitMethodCall(MethodCallExpression methodCallExpression)
        {
            return _methodCallTranslator.Translate(methodCallExpression);
        }

        private string VisitBinary(BinaryExpression binaryExpression)
        {
            var left = Visit(binaryExpression.Left);
            var right = Visit(binaryExpression.Right);

            if (binaryExpression.NodeType == ExpressionType.Equal && right == "NULL")
                return $"{left} IS NULL";
            
            if (binaryExpression.NodeType == ExpressionType.NotEqual && right == "NULL")
                return $"{left} IS NOT NULL";

            if (KsqlOperatorMapper.IsStringOperator(binaryExpression.NodeType, 
                                                  binaryExpression.Left.Type, 
                                                  binaryExpression.Right.Type))
            {
                return KsqlOperatorMapper.GetStringOperator(binaryExpression.NodeType, left, right);
            }

            var op = KsqlOperatorMapper.GetKsqlOperator(binaryExpression.NodeType);
            return $"({left} {op} {right})";
        }

        private string VisitUnary(UnaryExpression unaryExpression)
        {
            var operand = Visit(unaryExpression.Operand);
            return KsqlOperatorMapper.GetUnaryOperator(unaryExpression.NodeType, operand);
        }

        private string VisitConvert(UnaryExpression unaryExpression)
        {
            var operand = Visit(unaryExpression.Operand);
            var targetType = unaryExpression.Type;

            if (targetType == typeof(string))
            {
                return $"CAST({operand} AS VARCHAR)";
            }
            else if (targetType == typeof(int) || targetType == typeof(int?))
            {
                return $"CAST({operand} AS INTEGER)";
            }
            else if (targetType == typeof(long) || targetType == typeof(long?))
            {
                return $"CAST({operand} AS BIGINT)";
            }
            else if (targetType == typeof(double) || targetType == typeof(double?))
            {
                return $"CAST({operand} AS DOUBLE)";
            }
            else if (targetType == typeof(decimal) || targetType == typeof(decimal?))
            {
                return $"CAST({operand} AS DECIMAL(18, 2))";
            }
            else if (targetType == typeof(DateTime) || targetType == typeof(DateTime?))
            {
                return $"CAST({operand} AS TIMESTAMP)";
            }
            else if (targetType == typeof(bool) || targetType == typeof(bool?))
            {
                return $"CAST({operand} AS BOOLEAN)";
            }

            return operand;
        }

        private string VisitLambda(LambdaExpression lambdaExpression)
        {
            return Visit(lambdaExpression.Body);
        }

        private string VisitNew(NewExpression newExpression)
        {
            if (newExpression.Members == null)
                throw new NotSupportedException("Anonymous types without member names are not supported in KSQL.");

            var projections = new string[newExpression.Arguments.Count];
            
            for (int i = 0; i < newExpression.Arguments.Count; i++)
            {
                var expression = newExpression.Arguments[i];
                var memberName = newExpression.Members[i].Name;
                var value = Visit(expression);
                
                projections[i] = $"{value} AS {memberName}";
            }
            
            return string.Join(", ", projections);
        }

        private string VisitConditional(ConditionalExpression conditionalExpression)
        {
            var test = Visit(conditionalExpression.Test);
            var ifTrue = Visit(conditionalExpression.IfTrue);
            var ifFalse = Visit(conditionalExpression.IfFalse);
            
            return $"CASE WHEN {test} THEN {ifTrue} ELSE {ifFalse} END";
        }

        private string VisitParameter(ParameterExpression parameterExpression)
        {
            return parameterExpression.Name ?? "unknown_parameter";
        }

        private string VisitNewArray(NewArrayExpression newArrayExpression)
        {
            var elements = newArrayExpression.Expressions.Select(Visit);
            return $"ARRAY[{string.Join(", ", elements)}]";
        }
    }

    public static class MemberInfoExtensions
    {
        public static object? GetMemberValue(this System.Reflection.MemberInfo member, object instance)
        {
            switch (member)
            {
                case System.Reflection.PropertyInfo propertyInfo:
                    return propertyInfo.GetValue(instance);
                case System.Reflection.FieldInfo fieldInfo:
                    return fieldInfo.GetValue(instance);
                default:
                    throw new NotSupportedException($"Member type {member.GetType()} is not supported.");
            }
        }
    }
}