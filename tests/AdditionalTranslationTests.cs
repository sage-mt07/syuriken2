using Xunit;
using System;
using System.Linq;
using Ksql.EntityFramework.Query.Translation;
using Ksql.EntityFramework.Windows;
using Ksql.EntityFramework;
using Ksql.EntityFramework.Interfaces;
using Ksql.EntityFramework.Configuration;

namespace Ksql.EntityFrameworkCore.Tests;

public class AdditionalTranslationTests
{
    private class TestKsqlDbContext : KsqlDbContext
    {
        public TestKsqlDbContext() : base(new KsqlDbContextOptions { ConnectionString = "http://localhost" }) {}
        public IKsqlStream<Order> Orders { get; set; }
    }

    private TestKsqlDbContext context = new TestKsqlDbContext();

    [Fact]
    public void Test_IsNull()
    {
        var query = context.Orders.Where(o => o.Region == null);
        var translator = new KsqlQueryTranslator();
        var expected = "SELECT * FROM orders WHERE region IS NULL;";
        var actual = translator.Translate(query);
        Assert.Equal(expected, actual, ignoreCase: true, ignoreLineEndingDifferences: true);
    }

    [Fact]
    public void Test_IsNotNull()
    {
        var query = context.Orders.Where(o => null != o.Region);
        var translator = new KsqlQueryTranslator();
        var expected = "SELECT * FROM orders WHERE region IS NOT NULL;";
        var actual = translator.Translate(query);
        Assert.Equal(expected, actual, ignoreCase: true, ignoreLineEndingDifferences: true);
    }

    [Fact]
    public void Test_InClause()
    {
        var ids = new[] { "A", "B" };
        var query = context.Orders.Where(o => ids.Contains(o.CustomerId));
        var translator = new KsqlQueryTranslator();
        var expected = "SELECT * FROM orders WHERE customerId IN ('A', 'B');";
        var actual = translator.Translate(query);
        Assert.Equal(expected, actual, ignoreCase: true, ignoreLineEndingDifferences: true);
    }

    [Fact]
    public void Test_Distinct()
    {
        var query = context.Orders.Select(o => o.CustomerId).Distinct();
        var translator = new KsqlQueryTranslator();
        var expected = "SELECT DISTINCT customerId FROM orders;";
        var actual = translator.Translate(query);
        Assert.Equal(expected, actual, ignoreCase: true, ignoreLineEndingDifferences: true);
    }

    [Fact]
    public void Test_Cast()
    {
        var query = context.Orders.Select(o => (double)o.Amount);
        var translator = new KsqlQueryTranslator();
        var expected = "SELECT CAST(amount AS DOUBLE) FROM orders;";
        var actual = translator.Translate(query);
        Assert.Equal(expected, actual, ignoreCase: true, ignoreLineEndingDifferences: true);
    }

    public class Order
    {
        public string OrderId { get; set; } = string.Empty;
        public string CustomerId { get; set; } = string.Empty;
        public decimal Amount { get; set; }
        public DateTime OrderTime { get; set; }
        public string Region { get; set; } = string.Empty;
    }
}
