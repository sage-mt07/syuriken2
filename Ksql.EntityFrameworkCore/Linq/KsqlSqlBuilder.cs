using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ksql.EntityFramework.Query.Builders
{
    public class KsqlSqlBuilder
    {
        private const string Select = "SELECT";
        private const string From = "FROM";
        private const string Where = "WHERE";
        private const string GroupBy = "GROUP BY";
        private const string Having = "HAVING";
        private const string OrderBy = "ORDER BY";
        private const string Limit = "LIMIT";
        private const string Offset = "OFFSET";
        private const string Join = "JOIN";
        private const string LeftJoin = "LEFT JOIN";
        private const string RightJoin = "RIGHT JOIN";
        private const string FullJoin = "FULL JOIN";
        private const string On = "ON";

        private readonly StringBuilder _sql;
        private string? _selectClause;
        private string? _fromClause;
        private string? _whereClause;
        private string? _groupByClause;
        private string? _havingClause;
        private string? _orderByClause;
        private string? _limitClause;
        private string? _offsetClause;
        private readonly List<string> _joinClauses;
        private bool _distinct;

        public KsqlSqlBuilder()
        {
            _sql = new StringBuilder();
            _joinClauses = new List<string>();
        }

        public KsqlSqlBuilder AppendSelect(string columns, bool distinct = false)
        {
            _selectClause = columns;
            _distinct = distinct;
            return this;
        }

        public KsqlSqlBuilder AppendFrom(string tableName, string? alias = null)
        {
            _fromClause = string.IsNullOrEmpty(alias)
                ? tableName
                : $"{tableName} AS {alias}";
            return this;
        }

        public KsqlSqlBuilder AppendWhere(string condition)
        {
            if (string.IsNullOrEmpty(condition))
                return this;

            if (string.IsNullOrEmpty(_whereClause))
            {
                _whereClause = condition;
            }
            else
            {
                _whereClause = $"({_whereClause}) AND ({condition})";
            }
            return this;
        }

        public KsqlSqlBuilder AppendGroupBy(string columns)
        {
            _groupByClause = columns;
            return this;
        }

        public KsqlSqlBuilder AppendHaving(string condition)
        {
            if (string.IsNullOrEmpty(condition))
                return this;

            if (string.IsNullOrEmpty(_havingClause))
            {
                _havingClause = condition;
            }
            else
            {
                _havingClause = $"({_havingClause}) AND ({condition})";
            }
            return this;
        }

        public KsqlSqlBuilder AppendOrderBy(string columns)
        {
            _orderByClause = columns;
            return this;
        }

        public KsqlSqlBuilder AppendLimit(int limit)
        {
            if (limit <= 0)
                throw new ArgumentOutOfRangeException(nameof(limit), "Limit must be greater than zero.");

            _limitClause = limit.ToString();
            return this;
        }

        public KsqlSqlBuilder AppendOffset(int offset)
        {
            if (offset < 0)
                throw new ArgumentOutOfRangeException(nameof(offset), "Offset must be non-negative.");

            _offsetClause = offset.ToString();
            return this;
        }

        public KsqlSqlBuilder AppendJoin(string tableName, string condition, string joinType = Join, string? alias = null)
        {
            string tableReference = string.IsNullOrEmpty(alias)
                ? tableName
                : $"{tableName} AS {alias}";

            _joinClauses.Add($"{joinType} {tableReference} {On} {condition}");
            return this;
        }

        public KsqlSqlBuilder AppendLeftJoin(string tableName, string condition, string? alias = null)
        {
            return AppendJoin(tableName, condition, LeftJoin, alias);
        }

        public KsqlSqlBuilder AppendRightJoin(string tableName, string condition, string? alias = null)
        {
            return AppendJoin(tableName, condition, RightJoin, alias);
        }

        public KsqlSqlBuilder AppendFullJoin(string tableName, string condition, string? alias = null)
        {
            return AppendJoin(tableName, condition, FullJoin, alias);
        }

        public string Build()
        {
            if (string.IsNullOrEmpty(_selectClause))
                throw new InvalidOperationException("Select clause must be specified.");

            if (string.IsNullOrEmpty(_fromClause))
                throw new InvalidOperationException("From clause must be specified.");

            _sql.Clear();

            _sql.Append(Select);
            
            if (_distinct)
                _sql.Append(" DISTINCT");
            
            _sql.Append(" ").Append(_selectClause);
            _sql.Append(" ").Append(From).Append(" ").Append(_fromClause);

            foreach (var joinClause in _joinClauses)
            {
                _sql.Append(" ").Append(joinClause);
            }

            if (!string.IsNullOrEmpty(_whereClause))
            {
                _sql.Append(" ").Append(Where).Append(" ").Append(_whereClause);
            }

            if (!string.IsNullOrEmpty(_groupByClause))
            {
                _sql.Append(" ").Append(GroupBy).Append(" ").Append(_groupByClause);
            }

            if (!string.IsNullOrEmpty(_havingClause))
            {
                _sql.Append(" ").Append(Having).Append(" ").Append(_havingClause);
            }

            if (!string.IsNullOrEmpty(_orderByClause))
            {
                _sql.Append(" ").Append(OrderBy).Append(" ").Append(_orderByClause);
            }

            if (!string.IsNullOrEmpty(_limitClause))
            {
                _sql.Append(" ").Append(Limit).Append(" ").Append(_limitClause);
            }

            if (!string.IsNullOrEmpty(_offsetClause))
            {
                _sql.Append(" ").Append(Offset).Append(" ").Append(_offsetClause);
            }

            return _sql.ToString();
        }

        public override string ToString()
        {
            return Build();
        }

        public KsqlSqlBuilder Clone()
        {
            var clone = new KsqlSqlBuilder();
            clone._selectClause = this._selectClause;
            clone._fromClause = this._fromClause;
            clone._whereClause = this._whereClause;
            clone._groupByClause = this._groupByClause;
            clone._havingClause = this._havingClause;
            clone._orderByClause = this._orderByClause;
            clone._limitClause = this._limitClause;
            clone._offsetClause = this._offsetClause;
            clone._joinClauses.AddRange(this._joinClauses);
            clone._distinct = this._distinct;
            return clone;
        }
    }
}