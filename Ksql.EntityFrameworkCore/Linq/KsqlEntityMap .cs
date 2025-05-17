using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Ksql.EntityFramework.Attributes;
using Ksql.EntityFramework.Query.Translation;

namespace Ksql.EntityFramework.Schema
{
    public class KsqlEntityMap<T> where T : class
    {
        private readonly Dictionary<string, PropertyInfo> _properties;
        private readonly Dictionary<string, PropertyInfo> _keyProperties;
        private readonly PropertyInfo _timestampProperty;
        private readonly string _topicName;
        private readonly string _entityName;

        public KsqlEntityMap()
        {
            Type entityType = typeof(T);
            _entityName = entityType.Name;
            
            // トピック名を取得 (Topic属性から、または型名をデフォルトとして使用)
            var topicAttr = entityType.GetCustomAttribute<TopicAttribute>();
            _topicName = topicAttr?.Name ?? entityType.Name.ToLowerInvariant();
            
            // エンティティのプロパティを収集
            _properties = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .ToDictionary(p => p.Name);
            
            // キープロパティを特定
            _keyProperties = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.GetCustomAttribute<KeyAttribute>() != null)
                .ToDictionary(p => p.Name);
            
            // タイムスタンププロパティを特定
            _timestampProperty = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .FirstOrDefault(p => p.GetCustomAttribute<TimestampAttribute>() != null);
        }

        public string TopicName => _topicName;
        
        public string EntityName => _entityName;
        
        public IReadOnlyDictionary<string, PropertyInfo> Properties => _properties;
        
        public IReadOnlyDictionary<string, PropertyInfo> KeyProperties => _keyProperties;
        
        public PropertyInfo TimestampProperty => _timestampProperty;

        public string GetKsqlType(string propertyName)
        {
            if (!_properties.TryGetValue(propertyName, out var property))
                throw new ArgumentException($"Property {propertyName} not found on entity {_entityName}");
                
            return KsqlTypeMapper.GetKsqlType(property.PropertyType);
        }

        public string GetCreateStreamStatement()
        {
            List<string> columnDefinitions = new List<string>();
            
            foreach (var prop in _properties.Values)
            {
                string ksqlType = KsqlTypeMapper.GetKsqlType(prop.PropertyType);
                bool isKey = _keyProperties.ContainsKey(prop.Name);
                
                columnDefinitions.Add($"{prop.Name} {ksqlType}{(isKey ? " KEY" : "")}");
            }
            
            string columnsList = string.Join(", ", columnDefinitions);
            
            string timestampClause = "";
            if (_timestampProperty != null)
            {
                var timestampAttr = _timestampProperty.GetCustomAttribute<TimestampAttribute>();
                string format = !string.IsNullOrEmpty(timestampAttr.Format) 
                    ? $"FORMAT '{timestampAttr.Format}'" 
                    : "";
                
                timestampClause = $" WITH (TIMESTAMP='{_timestampProperty.Name}' {format})";
            }
            
            return $"CREATE STREAM {_entityName.ToLowerInvariant()} ({columnsList}) WITH (KAFKA_TOPIC='{_topicName}', VALUE_FORMAT='AVRO'){timestampClause};";
        }

        public string GetCreateTableStatement()
        {
            if (_keyProperties.Count == 0)
                throw new InvalidOperationException($"Entity {_entityName} must have at least one Key property to create a table");
                
            List<string> columnDefinitions = new List<string>();
            List<string> keyColumns = new List<string>();
            
            foreach (var prop in _properties.Values)
            {
                string ksqlType = KsqlTypeMapper.GetKsqlType(prop.PropertyType);
                bool isKey = _keyProperties.ContainsKey(prop.Name);
                
                columnDefinitions.Add($"{prop.Name} {ksqlType}");
                
                if (isKey)
                    keyColumns.Add(prop.Name);
            }
            
            string columnsList = string.Join(", ", columnDefinitions);
            string keysList = string.Join(", ", keyColumns);
            
            string timestampClause = "";
            if (_timestampProperty != null)
            {
                var timestampAttr = _timestampProperty.GetCustomAttribute<TimestampAttribute>();
                string format = !string.IsNullOrEmpty(timestampAttr.Format) 
                    ? $"FORMAT '{timestampAttr.Format}'" 
                    : "";
                
                timestampClause = $", TIMESTAMP='{_timestampProperty.Name}' {format}";
            }
            
            return $"CREATE TABLE {_entityName.ToLowerInvariant()} ({columnsList}) WITH (KAFKA_TOPIC='{_topicName}', VALUE_FORMAT='AVRO', KEY='{keysList}'{timestampClause});";
        }
    }
}