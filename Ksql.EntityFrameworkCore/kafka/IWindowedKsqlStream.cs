using Ksql.EntityFramework.Windows;

namespace Ksql.EntityFramework.Interfaces;

public interface IWindowedKsqlStream<T> : IQueryable<T> where T : class
{
   WindowSpecification WindowSpecification { get; }
}