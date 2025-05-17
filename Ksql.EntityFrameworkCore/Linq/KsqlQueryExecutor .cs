using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Ksql.EntityFramework.Configuration;

namespace Ksql.EntityFramework.Query.Execution
{
    public class KsqlQueryExecutor
    {
        private readonly HttpClient _httpClient;
        private readonly KsqlDbContextOptions _options;

        public KsqlQueryExecutor(KsqlDbContextOptions options)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _httpClient = new HttpClient
            {
                BaseAddress = new Uri(_options.ConnectionString),
                Timeout = TimeSpan.FromSeconds(_options.ConnectionTimeoutSeconds)
            };
        }

        public async Task<KsqlQueryResult> ExecuteQueryAsync(string ksqlQuery)
        {
            try
            {
                var request = new
                {
                    ksql = ksqlQuery,
                    streamsProperties = new Dictionary<string, string>
                    {
                        { "ksql.streams.auto.offset.reset", "earliest" }
                    }
                };

                var requestJson = JsonSerializer.Serialize(request);
                var content = new StringContent(requestJson, Encoding.UTF8, "application/json");

                var response = await _httpClient.PostAsync("/ksql", content);
                response.EnsureSuccessStatusCode();

                var responseJson = await response.Content.ReadAsStringAsync();
                return new KsqlQueryResult(responseJson);
            }
            catch (HttpRequestException ex)
            {
                throw new KsqlQueryExecutionException($"Failed to execute KSQL query: {ex.Message}", ex);
            }
        }

        public async Task<KsqlQueryStreamResult<T>> ExecuteStreamQueryAsync<T>(string ksqlQuery) where T : class, new()
        {
            try
            {
                var request = new
                {
                    sql = ksqlQuery,
                    properties = new Dictionary<string, string>
                    {
                        { "ksql.streams.auto.offset.reset", "earliest" }
                    }
                };

                var requestJson = JsonSerializer.Serialize(request);
                var content = new StringContent(requestJson, Encoding.UTF8, "application/json");

                var response = await _httpClient.PostAsync("/query-stream", content);
                response.EnsureSuccessStatusCode();

                return new KsqlQueryStreamResult<T>(response);
            }
            catch (HttpRequestException ex)
            {
                throw new KsqlQueryExecutionException($"Failed to execute KSQL streaming query: {ex.Message}", ex);
            }
        }
    }

    public class KsqlQueryResult
    {
        private readonly string _rawResponse;

        public KsqlQueryResult(string rawResponse)
        {
            _rawResponse = rawResponse;
        }

        public string RawResponse => _rawResponse;

        public JsonElement GetResponseJson()
        {
            return JsonSerializer.Deserialize<JsonElement>(_rawResponse);
        }
    }

    public class KsqlQueryStreamResult<T> where T : class, new()
    {
        private readonly HttpResponseMessage _response;

        public KsqlQueryStreamResult(HttpResponseMessage response)
        {
            _response = response;
        }

        public async IAsyncEnumerable<T> ReadResultsAsync()
        {
            using var stream = await _response.Content.ReadAsStreamAsync();
            using var reader = new System.IO.StreamReader(stream);

            while (!reader.EndOfStream)
            {
                string line = await reader.ReadLineAsync();
                if (string.IsNullOrEmpty(line))
                    continue;

                var json = JsonSerializer.Deserialize<JsonElement>(line);

                // ヘッダー行はスキップ
                if (json.TryGetProperty("header", out _))
                    continue;

                // エラーがあれば例外をスロー
                if (json.TryGetProperty("errorMessage", out var errorMsg))
                {
                    throw new KsqlQueryExecutionException(errorMsg.GetString());
                }

                // 行データを解析
                if (json.TryGetProperty("row", out var row) &&
                    row.TryGetProperty("columns", out var columns))
                {
                    yield return DeserializeRow(columns);
                }

            }
        }

        private T DeserializeRow(JsonElement columns)
        {
            var entity = new T();
            var entityType = typeof(T);

            if (columns.ValueKind != JsonValueKind.Array)
                throw new KsqlQueryExecutionException("Expected columns to be an array");

            // Columnsの値を対応するプロパティに設定
            var properties = entityType.GetProperties();
            int index = 0;

            foreach (var column in columns.EnumerateArray())
            {
                if (index >= properties.Length)
                    break;

                var property = properties[index];
                if (property.CanWrite)
                {
                    try
                    {
                        var value = ConvertJsonValueToPropertyType(column, property.PropertyType);
                        property.SetValue(entity, value);
                    }
                    catch (Exception ex)
                    {
                        throw new KsqlQueryExecutionException(
                            $"Failed to set property {property.Name} with value: {ex.Message}", ex);
                    }
                }

                index++;
            }

            return entity;
        }

        private object ConvertJsonValueToPropertyType(JsonElement element, Type targetType)
        {
            if (element.ValueKind == JsonValueKind.Null)
                return null;

            Type nullableUnderlyingType = Nullable.GetUnderlyingType(targetType);
            Type nonNullableType = nullableUnderlyingType ?? targetType;

            if (nonNullableType == typeof(string))
            {
                return element.ValueKind == JsonValueKind.String
                    ? element.GetString()
                    : element.ToString();
            }
            else if (nonNullableType == typeof(int))
            {
                return element.GetInt32();
            }
            else if (nonNullableType == typeof(long))
            {
                return element.GetInt64();
            }
            else if (nonNullableType == typeof(double))
            {
                return element.GetDouble();
            }
            else if (nonNullableType == typeof(decimal))
            {
                return element.GetDecimal();
            }
            else if (nonNullableType == typeof(bool))
            {
                return element.GetBoolean();
            }
            else if (nonNullableType == typeof(DateTime) || nonNullableType == typeof(DateTimeOffset))
            {
                if (element.ValueKind == JsonValueKind.String)
                {
                    var dateString = element.GetString();
                    if (nonNullableType == typeof(DateTime))
                        return DateTime.Parse(dateString);
                    else
                        return DateTimeOffset.Parse(dateString);
                }
                else
                {
                    var timestamp = element.GetInt64();
                    if (nonNullableType == typeof(DateTime))
                        return DateTimeOffset.FromUnixTimeMilliseconds(timestamp).DateTime;
                    else
                        return DateTimeOffset.FromUnixTimeMilliseconds(timestamp);
                }
            }
            else if (nonNullableType.IsEnum)
            {
                return Enum.Parse(nonNullableType, element.GetString());
            }
            else if (nonNullableType == typeof(Guid))
            {
                return Guid.Parse(element.GetString());
            }

            // 他の型にも対応する場合はここに追加

            throw new NotSupportedException($"Type conversion not supported for {targetType.Name}");
        }
    }

    public class KsqlQueryExecutionException : Exception
    {
        public KsqlQueryExecutionException(string message) : base(message)
        {
        }

        public KsqlQueryExecutionException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}