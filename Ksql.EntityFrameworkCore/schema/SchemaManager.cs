using System.Reflection;
using Confluent.SchemaRegistry;
using Ksql.EntityFramework.Attributes;
using Ksql.EntityFramework.Configuration;
using Ksql.EntityFramework.Models;

namespace Ksql.EntityFramework.Schema;

public class SchemaManager
{
   private readonly KsqlDbContextOptions _options;
   private readonly Dictionary<Type, TopicDescriptor> _topicDescriptors = new Dictionary<Type, TopicDescriptor>();

   public SchemaManager(KsqlDbContextOptions options)
   {
       _options = options ?? throw new ArgumentNullException(nameof(options));
   }

   public TopicDescriptor GetTopicDescriptor<T>() where T : class
   {
       var entityType = typeof(T);
       
       if (!_topicDescriptors.TryGetValue(entityType, out var descriptor))
       {
           descriptor = BuildTopicDescriptor(entityType);
           _topicDescriptors[entityType] = descriptor;
       }
       
       return descriptor;
   }

   public string ExtractKey<T>(T entity) where T : class
   {
       if (entity == null)
           throw new ArgumentNullException(nameof(entity));
           
       var entityType = typeof(T);
       var keyProperties = entityType.GetProperties()
           .Where(p => p.GetCustomAttribute<KeyAttribute>() != null)
           .OrderBy(p => p.GetCustomAttribute<KeyAttribute>()?.Order ?? 0)
           .ToList();
           
       if (keyProperties.Count == 0)
           return string.Empty;
           
       var keyValues = keyProperties.Select(p => p.GetValue(entity)?.ToString() ?? "null");
       return string.Join("_", keyValues);
   }

   public List<string> GetColumnDefinitions(Type entityType)
   {
       var properties = entityType.GetProperties()
           .Where(p => p.CanRead && p.CanWrite)
           .ToList();
           
       var columnDefinitions = new List<string>();
       
       foreach (var property in properties)
       {
           var columnName = property.Name;
           var columnType = GetKsqlType(property);
           var isKey = property.GetCustomAttribute<KeyAttribute>() != null;
           
           columnDefinitions.Add($"{columnName} {columnType}{(isKey ? " KEY" : "")}");
       }
       
       return columnDefinitions;
   }

   private TopicDescriptor BuildTopicDescriptor(Type entityType)
   {
       var topicAttribute = entityType.GetCustomAttribute<TopicAttribute>();
       
       var descriptor = new TopicDescriptor
       {
           Name = topicAttribute?.Name ?? entityType.Name.ToLowerInvariant(),
           EntityType = entityType,
           PartitionCount = topicAttribute?.PartitionCount ?? _options.DefaultPartitionCount,
           ReplicationFactor = topicAttribute?.ReplicationFactor ?? _options.DefaultReplicationFactor,
           ValueFormat = _options.DefaultValueFormat
       };
       
       // Find the key property
       var keyProperties = entityType.GetProperties()
           .Where(p => p.GetCustomAttribute<KeyAttribute>() != null)
           .OrderBy(p => p.GetCustomAttribute<KeyAttribute>()?.Order ?? 0)
           .ToList();
           
       if (keyProperties.Count > 0)
       {
           descriptor.KeyColumn = keyProperties[0].Name;
           
           foreach (var keyProperty in keyProperties)
           {
               descriptor.KeyColumns.Add(keyProperty.Name);
           }
       }
       
       // Find the timestamp property
       var timestampProperty = entityType.GetProperties()
           .FirstOrDefault(p => p.GetCustomAttribute<TimestampAttribute>() != null);
           
       if (timestampProperty != null)
       {
           var timestampAttr = timestampProperty.GetCustomAttribute<TimestampAttribute>();
           descriptor.TimestampColumn = timestampProperty.Name;
           descriptor.TimestampFormat = timestampAttr?.Format;
       }
       
       return descriptor;
   }

   private string GetKsqlType(PropertyInfo property)
   {
       var propertyType = property.PropertyType;
       var nullableType = Nullable.GetUnderlyingType(propertyType);
       var nonNullableType = nullableType ?? propertyType;
       
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
       {
           var decimalAttr = property.GetCustomAttribute<DecimalPrecisionAttribute>();
           if (decimalAttr != null)
               return $"DECIMAL({decimalAttr.Precision}, {decimalAttr.Scale})";
           return "DECIMAL(18, 2)";
       }
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
       if (nonNullableType.IsEnum)
           return "VARCHAR";
           
       throw new NotSupportedException($"Type {propertyType.FullName} is not supported in KSQL.");
   }
   
   private string GetKsqlType(Type type)
   {
       var nullableType = Nullable.GetUnderlyingType(type);
       var nonNullableType = nullableType ?? type;
       
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
           return "DECIMAL(18, 2)";
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
       if (nonNullableType.IsEnum)
           return "VARCHAR";
           
       throw new NotSupportedException($"Type {type.FullName} is not supported in KSQL.");
   }
}