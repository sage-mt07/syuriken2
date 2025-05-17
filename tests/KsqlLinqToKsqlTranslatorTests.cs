using Xunit;
using System;
using System.Linq;
using System.Linq.Expressions;
using Ksql.EntityFramework.Query.Translation;
using Ksql.EntityFramework.Windows;
using Ksql.EntityFramework;

namespace Ksql.EntityFrameworkCore.Tests
{
    public class KsqlLinqToKsqlTranslatorTests
    {
        // ダミーのDbContext/Entity
        private class TestKsqlDbContext : KsqlDbContext
        {
            public IQueryable<Order> Orders => CreateStream<Order>("orders");
            public IQueryable<Customer> Customers => CreateTable<Customer>("customers");
            public IQueryable<Product> Products => CreateTable<Product>("products");
        }

        private TestKsqlDbContext context = new TestKsqlDbContext();

        [Theory]
        [InlineData("Where", "SELECT * FROM orders WHERE amount > 1000;")]
        public void Test_SimpleWhere(string pattern, string expected)
        {
            var query = context.Orders.Where(o => o.Amount > 1000);
            var translator = new KsqlQueryTranslator();

            string actual = translator.Translate(query); // ←LINQ→KSQL変換メソッド
            Assert.Equal(expected, actual, ignoreLineEndingDifferences: true, ignoreCase: true);
        }

        [Fact]
        public void Test_Where_Select()
        {
            var query = context.Orders
                .Where(o => o.Amount > 1000)
                .Select(o => new { o.OrderId, o.Amount });
            var translator = new KsqlQueryTranslator();
            var expected = "SELECT orderId, amount FROM orders WHERE amount > 1000;";
            string actual = translator.Translate(query);
            Assert.Equal(expected, actual, ignoreLineEndingDifferences: true, ignoreCase: true);
        }

        [Fact]
        public void Test_GroupBy_Aggregate()
        {
            var query = context.Orders
                .GroupBy(o => o.CustomerId)
                .Select(g => new
                {
                    CustomerId = g.Key,
                    Total = g.Sum(x => x.Amount),
                    Count = g.Count()
                });

            var expected = "SELECT customerId, SUM(amount) AS total, COUNT(*) AS count FROM orders GROUP BY customerId;";
            var translator = new KsqlQueryTranslator();
            string actual = translator.Translate(query);
            Assert.Equal(expected, actual, ignoreLineEndingDifferences: true, ignoreCase: true);
        }

        //[Fact]
        //public void Test_LatestByOffset()
        //{
        //    var query = context.Orders
        //        .GroupBy(o => o.CustomerId)
        //        .Select(g => new
        //        {
        //            CustomerId = g.Key,
        //            LatestOrderTime = g.LatestByOffset(x => x.OrderTime)
        //        });

        //    var expected = "SELECT customerId, LATEST_BY_OFFSET(orderTime) AS latestOrderTime FROM orders GROUP BY customerId;";
        //    string actual = KsqlQueryTranslator.Translate(query);
        //    Assert.Equal(expected, actual, ignoreLineEndingDifferences: true, ignoreCase: true);
        //}

        [Fact]
        public void Test_TumblingWindow()
        {
            var query = context.Orders
                .Window(TumblingWindow.Of(TimeSpan.FromHours(1)))
                .GroupBy(o => o.CustomerId)
                .Select(g => new
                {
                    CustomerId = g.Key,
                    WindowStart = g.Window.Start,
                    Total = g.Sum(x => x.Amount)
                });

            var expected = @"SELECT customerId, WINDOWSTART AS windowStart, SUM(amount) AS total FROM orders WINDOW TUMBLING (SIZE 1 HOUR) GROUP BY customerId;";
            var translator = new KsqlQueryTranslator();

            string actual = translator.Translate(query);
            Assert.Equal(expected, actual, ignoreLineEndingDifferences: true, ignoreCase: true);
        }

        [Fact]
        public void Test_Join()
        {
            var query = from o in context.Orders
                        join c in context.Customers
                        on o.CustomerId equals c.CustomerId
                        select new { o.OrderId, c.CustomerName };

            var expected = @"SELECT o.orderId, c.customerName FROM orders o JOIN customers c ON o.customerId = c.customerId;";
            var translator = new KsqlQueryTranslator();

            string actual = translator.Translate(query);
            Assert.Equal(expected, actual, ignoreLineEndingDifferences: true, ignoreCase: true);
        }

        [Fact]
        public void Test_OrderBy_Limit()
        {
            var query = context.Orders
                .Where(o => o.Amount > 1000)
                .OrderByDescending(o => o.Amount)
                .Take(5);

            var expected = @"SELECT * FROM orders WHERE amount > 1000 ORDER BY amount DESC LIMIT 5;";
            var translator = new KsqlQueryTranslator();

            string actual = translator.Translate(query);
            Assert.Equal(expected, actual, ignoreLineEndingDifferences: true, ignoreCase: true);
        }

    
    }

    // ダミーPOCO
    public class Order
    {
        public string OrderId { get; set; }
        public string CustomerId { get; set; }
        public decimal Amount { get; set; }
        public DateTime OrderTime { get; set; }
        public string ProductId { get; set; }
        public string Region { get; set; }
    }
    public class Customer
    {
        public string CustomerId { get; set; }
        public string Region { get; set; }
        public string CustomerName { get; set; }
    }
    public class Product
    {
        public string ProductId { get; set; }
        public string ProductName { get; set; }
    }
}
