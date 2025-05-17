using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Ksql.EntityFramework.Configuration;

namespace Ksql.EntityFramework.Interfaces;
public interface IKsqlDatabase
{
    Task CreateTableAsync<T>(string tableName, Func<TableOptions, TableOptions> options) where T : class;
    Task DropTableAsync(string tableName);
    Task DropTopicAsync(string topicName);
    Task ExecuteKsqlAsync(string ksqlStatement);
}