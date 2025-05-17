using System.Collections;
using System.Linq.Expressions;
using Ksql.EntityFramework.Interfaces;

namespace Ksql.EntityFramework.Windows;

internal class WindowedKsqlStream<T> : IWindowedKsqlStream<T> where T : class
{
   private readonly IKsqlStream<T> _stream;

   public WindowSpecification WindowSpecification { get; }

   public Type ElementType => typeof(T);

   public Expression Expression => Expression.Constant(this);

   public IQueryProvider Provider => new KsqlQueryProvider();

   public WindowedKsqlStream(IKsqlStream<T> stream, WindowSpecification windowSpecification)
   {
       _stream = stream ?? throw new ArgumentNullException(nameof(stream));
       WindowSpecification = windowSpecification ?? throw new ArgumentNullException(nameof(windowSpecification));
   }

   public IEnumerator<T> GetEnumerator()
   {
       return _stream.GetEnumerator();
   }

   IEnumerator IEnumerable.GetEnumerator()
   {
       return GetEnumerator();
   }
}