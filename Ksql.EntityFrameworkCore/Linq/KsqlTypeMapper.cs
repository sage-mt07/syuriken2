using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Ksql.EntityFramework.Attributes;

namespace Ksql.EntityFramework.Query.Translation
{
    /// <summary>
    /// .NET の型を KSQL の型にマッピングするためのユーティリティクラス
    /// </summary>
    public static class KsqlTypeMapper
    {
        /// <summary>
        /// .NET の型を KSQL の型名にマッピングします
        /// </summary>
        /// <param name="clrType">.NET の型</param>
        /// <returns>KSQL の型名</returns>
        public static string GetKsqlType(Type clrType)
        {
            if (clrType == null)
                throw new ArgumentNullException(nameof(clrType));

            // Nullable 型の場合は内部の型を取得
            Type nonNullableType = Nullable.GetUnderlyingType(clrType) ?? clrType;

            // 基本型のマッピング
            if (nonNullableType == typeof(bool))
                return "BOOLEAN";
            if (nonNullableType == typeof(byte) || nonNullableType == typeof(sbyte) || 
                nonNullableType == typeof(short) || nonNullableType == typeof(ushort))
                return "SMALLINT";
            if (nonNullableType == typeof(int) || nonNullableType == typeof(uint))
                return "INTEGER";
            if (nonNullableType == typeof(long) || nonNullableType == typeof(ulong))
                return "BIGINT";
            if (nonNullableType == typeof(float))
                return "REAL";
            if (nonNullableType == typeof(double))
                return "DOUBLE";
            if (nonNullableType == typeof(decimal))
                return GetDecimalType(clrType);
            if (nonNullableType == typeof(string))
                return "VARCHAR";
            if (nonNullableType == typeof(DateTime) || nonNullableType == typeof(DateTimeOffset))
                return "TIMESTAMP";
            if (nonNullableType == typeof(TimeSpan))
                return "TIME";
            if (nonNullableType == typeof(Guid))
                return "VARCHAR";
            if (nonNullableType.IsArray)
                return $"ARRAY<{GetKsqlType(nonNullableType.GetElementType())}>";
            if (IsGenericList(nonNullableType))
                return $"ARRAY<{GetKsqlType(nonNullableType.GetGenericArguments()[0])}>";
            if (nonNullableType.IsEnum)
                return "VARCHAR";

            // 複合型の場合はSTRUCT型にマッピング
            if (nonNullableType.IsClass && !nonNullableType.IsPrimitive)
                return GetStructType(nonNullableType);

            throw new NotSupportedException($"型 {clrType.FullName} はKSQLにマッピングできません。");
        }

        /// <summary>
        /// Decimal型のKSQL型を取得します
        /// </summary>
        /// <param name="propertyType">プロパティの型</param>
        /// <returns>KSQL decimal型定義</returns>
        private static string GetDecimalType(Type propertyType)
        {
            // デフォルトの精度とスケール
            int precision = 18;
            int scale = 2;

            // DecimalPrecision属性があれば、そこから値を取得
            if (propertyType.GetCustomAttribute<DecimalPrecisionAttribute>() is DecimalPrecisionAttribute attr)
            {
                precision = attr.Precision;
                scale = attr.Scale;
            }

            return $"DECIMAL({precision}, {scale})";
        }

        /// <summary>
        /// 型がジェネリックリスト型かどうかを判定します
        /// </summary>
        /// <param name="type">判定する型</param>
        /// <returns>ジェネリックリスト型かどうか</returns>
        private static bool IsGenericList(Type type)
        {
            if (type.IsGenericType)
            {
                Type genericDef = type.GetGenericTypeDefinition();
                return genericDef == typeof(List<>) || 
                       genericDef == typeof(IList<>) || 
                       genericDef == typeof(ICollection<>) || 
                       genericDef == typeof(IEnumerable<>);
            }
            return false;
        }

        /// <summary>
        /// クラス型からKSQLのSTRUCT型定義を生成します
        /// </summary>
        /// <param name="type">クラス型</param>
        /// <returns>KSQL STRUCT型定義</returns>
        private static string GetStructType(Type type)
        {
            var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                                .Where(p => p.CanRead);

            var fieldDefinitions = properties.Select(p => 
                $"{p.Name} {GetKsqlType(p.PropertyType)}");

            return $"STRUCT<{string.Join(", ", fieldDefinitions)}>";
        }

        /// <summary>
        /// .NET値をKSQL文字列リテラルに変換します
        /// </summary>
        /// <param name="value">変換する値</param>
        /// <returns>KSQL文字列リテラル</returns>
        public static string GetKsqlLiteral(object value)
        {
            if (value == null)
                return "NULL";

            var type = value.GetType();
            var nonNullableType = Nullable.GetUnderlyingType(type) ?? type;

            if (nonNullableType == typeof(bool))
                return ((bool)value) ? "TRUE" : "FALSE";
            
            if (nonNullableType == typeof(string) || nonNullableType == typeof(Guid))
                return $"'{value.ToString().Replace("'", "''")}'";
            
            if (nonNullableType == typeof(DateTime))
            {
                var dt = (DateTime)value;
                return $"TIMESTAMP '{dt:yyyy-MM-dd HH:mm:ss.fff}'";
            }
            
            if (nonNullableType == typeof(DateTimeOffset))
            {
                var dto = (DateTimeOffset)value;
                return $"TIMESTAMP '{dto:yyyy-MM-dd HH:mm:ss.fff}'";
            }

            if (nonNullableType == typeof(TimeSpan))
            {
                var ts = (TimeSpan)value;
                return $"TIME '{ts:hh\\:mm\\:ss}'";
            }

            if (nonNullableType.IsEnum)
                return $"'{value}'";

            // 数値型はそのまま文字列化
            if (nonNullableType.IsPrimitive || nonNullableType == typeof(decimal))
                return value.ToString();

            throw new NotSupportedException($"値 {value} はKSQLリテラルに変換できません。");
        }
    }
}