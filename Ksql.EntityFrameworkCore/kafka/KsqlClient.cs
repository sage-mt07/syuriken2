using System.Text;
using System.Text.Json;

namespace Ksql.EntityFramework.Ksql;

internal class KsqlClient : IDisposable
{
   private readonly HttpClient _httpClient;
   private readonly string _ksqlServerUrl;
   private bool _disposed;

   public KsqlClient(string ksqlServerUrl)
   {
       _ksqlServerUrl = ksqlServerUrl ?? throw new ArgumentNullException(nameof(ksqlServerUrl));
       _httpClient = new HttpClient();
   }

   public async Task ExecuteKsqlAsync(string ksqlStatement)
   {
       var requestObject = new
       {
           ksql = ksqlStatement,
           streamsProperties = new Dictionary<string, string>
           {
               { "ksql.streams.auto.offset.reset", "earliest" }
           }
       };

       var requestJson = JsonSerializer.Serialize(requestObject);
       var content = new StringContent(requestJson, Encoding.UTF8, "application/json");

       try
       {
           var response = await _httpClient.PostAsync($"{_ksqlServerUrl}/ksql", content);

           if (!response.IsSuccessStatusCode)
           {
               var error = await response.Content.ReadAsStringAsync();
               throw new KsqlException($"Failed to execute KSQL statement. Status code: {response.StatusCode}. Error: {error}");
           }

           var responseJson = await response.Content.ReadAsStringAsync();
           Console.WriteLine($"KSQL response: {responseJson}");
       }
       catch (HttpRequestException ex)
       {
           throw new KsqlException($"Failed to connect to KSQL server: {ex.Message}", ex);
       }
   }

   public void Dispose()
   {
       Dispose(true);
       GC.SuppressFinalize(this);
   }

   protected virtual void Dispose(bool disposing)
   {
       if (!_disposed)
       {
           if (disposing)
           {
               _httpClient.Dispose();
           }

           _disposed = true;
       }
   }
}

public class KsqlException : Exception
{
   public KsqlException(string message) : base(message)
   {
   }

   public KsqlException(string message, Exception innerException) : base(message, innerException)
   {
   }
}