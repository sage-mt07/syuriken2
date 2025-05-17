using System.Linq.Expressions;
using Ksql.EntityFramework.Interfaces;
using Ksql.EntityFramework.Models;

namespace Ksql.EntityFramework.Configuration;

public class TableBuilder<T> where T : class
{
   private readonly TableOptions _options = new TableOptions();
   private string? _streamSource;
   private string? _topicSource;

   public string TableName { get; }

   public TableBuilder(string tableName)
   {
       TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
   }

   public TableBuilder<T> FromStream<TStream>(IKsqlStream<TStream> stream) where TStream : class
   {
       _streamSource = typeof(TStream).Name.ToLowerInvariant();
       _topicSource = null;
       return this;
   }

   public TableBuilder<T> FromTopic<TSource>(string topicName) where TSource : class
   {
       _topicSource = topicName;
       _streamSource = null;
       _options.WithTopic(topicName);
       return this;
   }

   public TableBuilder<T> WithKeyColumn<TKey>(Expression<Func<T, TKey>> keySelector)
   {
       var propertyName = ExtractPropertyName(keySelector);
       _options.KeyColumns.Add(propertyName);
       return this;
   }

   public TableBuilder<T> WithValueFormat(ValueFormat format)
   {
       _options.ValueFormat = format;
       return this;
   }

   public TableBuilder<T> WithTimestamp<TTimestamp>(Expression<Func<T, TTimestamp>> timestampSelector, string? format = null)
   {
       var propertyName = ExtractPropertyName(timestampSelector);
       _options.TimestampColumn = propertyName;
       _options.TimestampFormat = format;
       return this;
   }

   public TableOptions Build()
   {
       return _options;
   }

   public (string? StreamSource, string? TopicSource) GetSource()
   {
       return (_streamSource, _topicSource);
   }

   private static string ExtractPropertyName<TSource, TProperty>(Expression<Func<TSource, TProperty>> propertySelector)
   {
       if (propertySelector.Body is MemberExpression memberExpression)
       {
           return memberExpression.Member.Name;
       }

       throw new ArgumentException("The expression must be a property selector.", nameof(propertySelector));
   }
}