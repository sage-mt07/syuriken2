using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Ksql.EntityFramework.Query.Expressions;

namespace Ksql.EntityFramework.Query.Translation
{
    /// <summary>
    /// LINQ メソッド呼び出しを KSQL 関数に変換するトランスレーター
    /// </summary>
    public class KsqlMethodCallTranslator
    {
        private readonly KsqlExpressionVisitor _expressionVisitor;
        private readonly Dictionary<(Type, string), Func<MethodCallExpression, string>> _methodTranslators;

        public KsqlMethodCallTranslator(KsqlExpressionVisitor expressionVisitor)
        {
            _expressionVisitor = expressionVisitor ?? throw new ArgumentNullException(nameof(expressionVisitor));
            _methodTranslators = InitializeMethodTranslators();
        }

        /// <summary>
        /// メソッド呼び出し式を KSQL 関数に変換します
        /// </summary>
        /// <param name="methodCallExpression">メソッド呼び出し式</param>
        /// <returns>KSQL 関数呼び出し文字列</returns>
        public string Translate(MethodCallExpression methodCallExpression)
        {
            if (methodCallExpression == null)
                throw new ArgumentNullException(nameof(methodCallExpression));

            // メソッドのキーを取得
            var methodKey = GetMethodKey(methodCallExpression.Method);

            // 登録済みのトランスレーターがあればそれを使用
            if (_methodTranslators.TryGetValue(methodKey, out var translator))
            {
                return translator(methodCallExpression);
            }

            // 拡張メソッドの場合は、第一引数がthisパラメータ
            if (methodCallExpression.Method.IsStatic && 
                methodCallExpression.Method.IsDefined(typeof(System.Runtime.CompilerServices.ExtensionAttribute), false) &&
                methodCallExpression.Arguments.Count > 0)
            {
                var instanceType = methodCallExpression.Arguments[0].Type;
                var extMethodKey = (instanceType, methodCallExpression.Method.Name);

                if (_methodTranslators.TryGetValue(extMethodKey, out translator))
                {
                    return translator(methodCallExpression);
                }
            }

            throw new NotSupportedException(
                $"メソッド {methodCallExpression.Method.DeclaringType?.Name}.{methodCallExpression.Method.Name} はKSQLでサポートされていません。");
        }

        /// <summary>
        /// メソッドトランスレーターの辞書を初期化します
        /// </summary>
        private Dictionary<(Type, string), Func<MethodCallExpression, string>> InitializeMethodTranslators()
        {
            var translators = new Dictionary<(Type, string), Func<MethodCallExpression, string>>
            {
                // String メソッド
                { (typeof(string), nameof(string.ToUpper)), TranslateToUpper },
                { (typeof(string), nameof(string.ToLower)), TranslateToLower },
                { (typeof(string), nameof(string.Trim)), TranslateTrim },
                { (typeof(string), nameof(string.TrimStart)), TranslateTrimStart },
                { (typeof(string), nameof(string.TrimEnd)), TranslateTrimEnd },
                { (typeof(string), nameof(string.Substring)), TranslateSubstring },
                { (typeof(string), nameof(string.Contains)), TranslateContains },
                { (typeof(Enumerable), nameof(Enumerable.Contains)), TranslateCollectionContains },
                { (typeof(string), nameof(string.StartsWith)), TranslateStartsWith },
                { (typeof(string), nameof(string.EndsWith)), TranslateEndsWith },
                { (typeof(string), nameof(string.IndexOf)), TranslateIndexOf },
                { (typeof(string), nameof(string.Replace)), TranslateReplace },
                { (typeof(string), nameof(string.Length)), TranslateLength },

                // DateTime メソッド
                { (typeof(DateTime), nameof(DateTime.AddDays)), TranslateAddDays },
                { (typeof(DateTime), nameof(DateTime.AddHours)), TranslateAddHours },
                { (typeof(DateTime), nameof(DateTime.AddMinutes)), TranslateAddMinutes },
                { (typeof(DateTime), nameof(DateTime.AddMonths)), TranslateAddMonths },
                { (typeof(DateTime), nameof(DateTime.AddYears)), TranslateAddYears },

                // Math メソッド
                { (typeof(Math), nameof(Math.Abs)), TranslateAbs },
                { (typeof(Math), nameof(Math.Ceiling)), TranslateCeiling },
                { (typeof(Math), nameof(Math.Floor)), TranslateFloor },
                { (typeof(Math), nameof(Math.Round)), TranslateRound },
                { (typeof(Math), nameof(Math.Pow)), TranslatePow },

                // 集計関数
                { (typeof(Enumerable), nameof(Enumerable.Count)), TranslateCount },
                { (typeof(Enumerable), nameof(Enumerable.Sum)), TranslateSum },
                { (typeof(Enumerable), nameof(Enumerable.Min)), TranslateMin },
                { (typeof(Enumerable), nameof(Enumerable.Max)), TranslateMax },
                { (typeof(Enumerable), nameof(Enumerable.Average)), TranslateAverage },

                // その他拡張メソッド
                { (typeof(string), nameof(string.IsNullOrEmpty)), TranslateIsNullOrEmpty }
            };

            return translators;
        }

        /// <summary>
        /// メソッドキーを取得します
        /// </summary>
        private (Type, string) GetMethodKey(MethodInfo method)
        {
            return (method.DeclaringType, method.Name);
        }

        #region String Method Translators

        private string TranslateToUpper(MethodCallExpression methodCall)
        {
            if (methodCall?.Object == null) throw new ArgumentException("TranslateToUpper methodCall null ");
            var instance = _expressionVisitor.Visit(methodCall.Object);
            return $"UPPER({instance})";
        }

        private string TranslateToLower(MethodCallExpression methodCall)
        {
            if (methodCall?.Object == null) throw new ArgumentException("TranslateToLower methodCall null ");
            var instance = _expressionVisitor.Visit(methodCall.Object);
            return $"LOWER({instance})";
        }

        private string TranslateTrim(MethodCallExpression methodCall)
        {
            if (methodCall?.Object == null) throw new ArgumentException("TranslateTrim methodCall null ");
            var instance = _expressionVisitor.Visit(methodCall.Object);
            return $"TRIM({instance})";
        }

        private string TranslateTrimStart(MethodCallExpression methodCall)
        {
            if (methodCall?.Object == null) throw new ArgumentException("TranslateTrimStart methodCall null ");
            var instance = _expressionVisitor.Visit(methodCall.Object);
            return $"LTRIM({instance})";
        }

        private string TranslateTrimEnd(MethodCallExpression methodCall)
        {
            if (methodCall?.Object == null) throw new ArgumentException("TranslateTrimEnd methodCall null ");
            var instance = _expressionVisitor.Visit(methodCall.Object);
            return $"RTRIM({instance})";
        }

        private string TranslateSubstring(MethodCallExpression methodCall)
        {
            var instance = _expressionVisitor.Visit(methodCall.Object);
            var startIndex = _expressionVisitor.Visit(methodCall.Arguments[0]);
            
            if (methodCall.Arguments.Count == 2)
            {
                // Substring(startIndex, length)
                var length = _expressionVisitor.Visit(methodCall.Arguments[1]);
                return $"SUBSTRING({instance}, {startIndex} + 1, {length})";
            }
            
            // Substring(startIndex)
            return $"SUBSTRING({instance}, {startIndex} + 1)";
        }

        private string TranslateContains(MethodCallExpression methodCall)
        {
            if (methodCall == null) throw new InvalidOperationException("TranslateContains ");
            var instance = _expressionVisitor.Visit(methodCall!.Object);
            var value = _expressionVisitor.Visit(methodCall.Arguments[0]);
            return $"{instance} LIKE CONCAT('%', {value}, '%')";
        }

        private string TranslateCollectionContains(MethodCallExpression methodCall)
        {
            if (methodCall == null) throw new ArgumentNullException(nameof(methodCall));

            Expression collectionExpr;
            Expression valueExpr;

            if (methodCall.Method.IsStatic && methodCall.Arguments.Count == 2)
            {
                collectionExpr = methodCall.Arguments[0];
                valueExpr = methodCall.Arguments[1];
            }
            else
            {
                collectionExpr = methodCall.Object!;
                valueExpr = methodCall.Arguments[0];
            }

            var value = _expressionVisitor.Visit(valueExpr);

            if (collectionExpr is ConstantExpression constExpr && constExpr.Value is System.Collections.IEnumerable items && constExpr.Value is not string)
            {
                var list = new List<string>();
                foreach (var item in items)
                {
                    list.Add(KsqlTypeMapper.GetKsqlLiteral(item!));
                }
                return $"{value} IN ({string.Join(", ", list)})";
            }

            var collectionSql = _expressionVisitor.Visit(collectionExpr);
            return $"{value} IN {collectionSql}";
        }

        private string TranslateStartsWith(MethodCallExpression methodCall)
        {
            if (methodCall?.Object == null) throw new ArgumentException("TranslateStartsWith argument null");
            var instance = _expressionVisitor.Visit(methodCall!.Object);
            var value = _expressionVisitor.Visit(methodCall.Arguments[0]);
            return $"{instance} LIKE CONCAT({value}, '%')";
        }

        private string TranslateEndsWith(MethodCallExpression methodCall)
        {
            if (methodCall?.Object == null) throw new ArgumentException("TranslateEndsWith argument null");
            var instance = _expressionVisitor.Visit(methodCall.Object);
            var value = _expressionVisitor.Visit(methodCall.Arguments[0]);
            return $"{instance} LIKE CONCAT('%', {value})";
        }

        private string TranslateIndexOf(MethodCallExpression methodCall)
        {
            if (methodCall?.Object == null) throw new ArgumentException("TranslateIndexOf argument null");
            var instance = _expressionVisitor.Visit(methodCall.Object);
            var value = _expressionVisitor.Visit(methodCall.Arguments[0]);
            return $"STRPOS({instance}, {value}) - 1";
        }

        private string TranslateReplace(MethodCallExpression methodCall)
        {
            if (methodCall?.Object == null) throw new ArgumentException("TranslateReplace argument null");
            var instance = _expressionVisitor.Visit(methodCall.Object);
            var oldValue = _expressionVisitor.Visit(methodCall.Arguments[0]);
            var newValue = _expressionVisitor.Visit(methodCall.Arguments[1]);
            return $"REPLACE({instance}, {oldValue}, {newValue})";
        }

        private string TranslateLength(MethodCallExpression methodCall)
        {
            if (methodCall?.Object == null) throw new ArgumentException("TranslateLength argument null");
            var instance = _expressionVisitor.Visit(methodCall.Object);
            return $"LENGTH({instance})";
        }

        #endregion

        #region DateTime Method Translators

        private string TranslateAddDays(MethodCallExpression methodCall)
        {
            if (methodCall?.Object == null) throw new ArgumentException("TranslateAddDays argument null");
            var instance = _expressionVisitor.Visit(methodCall.Object);
            var days = _expressionVisitor.Visit(methodCall.Arguments[0]);
            return $"TIMESTAMPADD(DAY, {days}, {instance})";
        }

        private string TranslateAddHours(MethodCallExpression methodCall)
        {
            if (methodCall?.Object == null) throw new ArgumentException("TranslateAddHours argument null");
            var instance = _expressionVisitor.Visit(methodCall.Object);
            var hours = _expressionVisitor.Visit(methodCall.Arguments[0]);
            return $"TIMESTAMPADD(HOUR, {hours}, {instance})";
        }

        private string TranslateAddMinutes(MethodCallExpression methodCall)
        {
            if (methodCall?.Object == null) throw new ArgumentException("TranslateAddMinutes argument null");
            var instance = _expressionVisitor.Visit(methodCall.Object);
            var minutes = _expressionVisitor.Visit(methodCall.Arguments[0]);
            return $"TIMESTAMPADD(MINUTE, {minutes}, {instance})";
        }

        private string TranslateAddMonths(MethodCallExpression methodCall)
        {
            if (methodCall?.Object == null) throw new ArgumentException("TranslateAddMonths argument null");
            var instance = _expressionVisitor.Visit(methodCall.Object);
            var months = _expressionVisitor.Visit(methodCall.Arguments[0]);
            return $"TIMESTAMPADD(MONTH, {months}, {instance})";
        }

        private string TranslateAddYears(MethodCallExpression methodCall)
        {
            if (methodCall?.Object == null) throw new ArgumentException("TranslateAddYears argument null");
            var instance = _expressionVisitor.Visit(methodCall.Object);
            var years = _expressionVisitor.Visit(methodCall.Arguments[0]);
            return $"TIMESTAMPADD(YEAR, {years}, {instance})";
        }

        #endregion

        #region Math Method Translators

        private string TranslateAbs(MethodCallExpression methodCall)
        {
            var value = _expressionVisitor.Visit(methodCall.Arguments[0]);
            return $"ABS({value})";
        }

        private string TranslateCeiling(MethodCallExpression methodCall)
        {
            var value = _expressionVisitor.Visit(methodCall.Arguments[0]);
            return $"CEIL({value})";
        }

        private string TranslateFloor(MethodCallExpression methodCall)
        {
            var value = _expressionVisitor.Visit(methodCall.Arguments[0]);
            return $"FLOOR({value})";
        }

        private string TranslateRound(MethodCallExpression methodCall)
        {
            var value = _expressionVisitor.Visit(methodCall.Arguments[0]);
            
            if (methodCall.Arguments.Count == 2)
            {
                var digits = _expressionVisitor.Visit(methodCall.Arguments[1]);
                return $"ROUND({value}, {digits})";
            }
            
            return $"ROUND({value})";
        }

        private string TranslatePow(MethodCallExpression methodCall)
        {
            var x = _expressionVisitor.Visit(methodCall.Arguments[0]);
            var y = _expressionVisitor.Visit(methodCall.Arguments[1]);
            return $"POWER({x}, {y})";
        }

        #endregion

        #region Aggregate Function Translators

        private string TranslateCount(MethodCallExpression methodCall)
        {
            // Count() または Count(predicate)
            if (methodCall.Arguments.Count == 1)
                return "COUNT(*)";

            // 述語がある場合は WHERE 句として扱うべき
            throw new NotSupportedException("Count() with predicate is not directly supported in KSQL. Use Where() before Count().");
        }

        private string TranslateSum(MethodCallExpression methodCall)
        {
            // Sum(selector)
            if (methodCall.Arguments.Count == 2)
            {
                var selectorLambda = (LambdaExpression)StripQuotes(methodCall.Arguments[1]);
                var selectorBody = _expressionVisitor.Visit(selectorLambda.Body);
                return $"SUM({selectorBody})";
            }

            throw new NotSupportedException("Unsupported Sum() method signature.");
        }

        private string TranslateMin(MethodCallExpression methodCall)
        {
            // Min(selector)
            if (methodCall.Arguments.Count == 2)
            {
                var selectorLambda = (LambdaExpression)StripQuotes(methodCall.Arguments[1]);
                var selectorBody = _expressionVisitor.Visit(selectorLambda.Body);
                return $"MIN({selectorBody})";
            }

            throw new NotSupportedException("Unsupported Min() method signature.");
        }

        private string TranslateMax(MethodCallExpression methodCall)
        {
            // Max(selector)
            if (methodCall.Arguments.Count == 2)
            {
                var selectorLambda = (LambdaExpression)StripQuotes(methodCall.Arguments[1]);
                var selectorBody = _expressionVisitor.Visit(selectorLambda.Body);
                return $"MAX({selectorBody})";
            }

            throw new NotSupportedException("Unsupported Max() method signature.");
        }

        private string TranslateAverage(MethodCallExpression methodCall)
        {
            // Average(selector)
            if (methodCall.Arguments.Count == 2)
            {
                var selectorLambda = (LambdaExpression)StripQuotes(methodCall.Arguments[1]);
                var selectorBody = _expressionVisitor.Visit(selectorLambda.Body);
                return $"AVG({selectorBody})";
            }

            throw new NotSupportedException("Unsupported Average() method signature.");
        }

        #endregion

        #region Other Method Translators

        private string TranslateIsNullOrEmpty(MethodCallExpression methodCall)
        {
            var value = _expressionVisitor.Visit(methodCall.Arguments[0]);
            return $"({value} IS NULL OR {value} = '')";
        }

        #endregion

        #region Utilities

        /// <summary>
        /// 式からクォートを取り除きます
        /// </summary>
        private static Expression StripQuotes(Expression expression)
        {
            while (expression.NodeType == ExpressionType.Quote)
            {
                expression = ((UnaryExpression)expression).Operand;
            }
            return expression;
        }

        #endregion
    }
}