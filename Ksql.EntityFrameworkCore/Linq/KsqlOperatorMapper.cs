using System;
using System.Linq.Expressions;

namespace Ksql.EntityFramework.Query.Expressions
{
    /// <summary>
    /// C#の演算子をKSQLの演算子にマッピングするためのユーティリティクラス
    /// </summary>
    public static class KsqlOperatorMapper
    {
        /// <summary>
        /// ExpressionTypeからKSQL演算子文字列にマッピングします
        /// </summary>
        /// <param name="expressionType">C#の演算子の種類</param>
        /// <returns>KSQL演算子文字列</returns>
        public static string GetKsqlOperator(ExpressionType expressionType)
        {
            return expressionType switch
            {
                // 算術演算子
                ExpressionType.Add => "+",
                ExpressionType.Subtract => "-",
                ExpressionType.Multiply => "*",
                ExpressionType.Divide => "/",
                ExpressionType.Modulo => "%",

                // 比較演算子
                ExpressionType.Equal => "=",
                ExpressionType.NotEqual => "!=",
                ExpressionType.GreaterThan => ">",
                ExpressionType.GreaterThanOrEqual => ">=",
                ExpressionType.LessThan => "<",
                ExpressionType.LessThanOrEqual => "<=",

                // 論理演算子
                ExpressionType.AndAlso => "AND",
                ExpressionType.OrElse => "OR",
                ExpressionType.Not => "NOT",

                // ビット演算子
                ExpressionType.And => "AND",
                ExpressionType.Or => "OR",

                // その他のサポートしている演算子
                ExpressionType.Coalesce => "COALESCE",

                // サポートしていない演算子
                _ => throw new NotSupportedException($"演算子 {expressionType} はKSQLでサポートされていません。")
            };
        }

        /// <summary>
        /// 文字列演算子をKSQL関数に変換します
        /// </summary>
        /// <param name="expressionType">式の種類</param>
        /// <param name="left">左オペランド</param>
        /// <param name="right">右オペランド</param>
        /// <returns>KSQL関数呼び出し文字列</returns>
        public static string GetStringOperator(ExpressionType expressionType, string left, string right)
        {
            return expressionType switch
            {
                ExpressionType.Add => $"CONCAT({left}, {right})",
                _ => throw new NotSupportedException($"文字列演算子 {expressionType} はKSQLでサポートされていません。")
            };
        }

        /// <summary>
        /// 単項演算子をKSQL式に変換します
        /// </summary>
        /// <param name="expressionType">式の種類</param>
        /// <param name="operand">オペランド</param>
        /// <returns>KSQL単項演算子式</returns>
        public static string GetUnaryOperator(ExpressionType expressionType, string operand)
        {
            return expressionType switch
            {
                ExpressionType.Not => $"NOT {operand}",
                ExpressionType.Negate => $"-{operand}",
                _ => throw new NotSupportedException($"単項演算子 {expressionType} はKSQLでサポートされていません。")
            };
        }

        /// <summary>
        /// 演算子が文字列演算子かどうかを判定します
        /// </summary>
        /// <param name="expressionType">式の種類</param>
        /// <param name="leftType">左オペランドの型</param>
        /// <param name="rightType">右オペランドの型</param>
        /// <returns>文字列演算子かどうか</returns>
        public static bool IsStringOperator(ExpressionType expressionType, Type leftType, Type rightType)
        {
            if (expressionType != ExpressionType.Add)
                return false;

            return leftType == typeof(string) || rightType == typeof(string);
        }
    }
}