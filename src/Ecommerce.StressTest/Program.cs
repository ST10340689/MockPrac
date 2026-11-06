using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.Http.Json;
using System.Text.Json;

namespace Ecommerce.StressTest;

/// <summary>
/// AgriStream IoT — High-Concurrency Load Testing Engine.
/// Simulates peak daylight operational hours where commercial farm soil sensors,
/// automated irrigation controllers, and concurrent users access the MVC App Service.
/// </summary>
public class Program
{
    private static readonly string[] FarmZones =
    {
        "Sector-North-A1",
        "Sector-South-B2",
        "Sector-East-C3",
        "Sector-West-D4"
    };

    private static readonly string[] EventTypes =
    {
        "SoilSensorReading",
        "IrrigationEvent",
        "EnvironmentalLog"
    };

    public static async Task Main(string[] args)
    {
        string targetUrl = args.Length > 0 && args[0].StartsWith("http", StringComparison.OrdinalIgnoreCase)
            ? args[0]
            : "";

        if (string.IsNullOrWhiteSpace(targetUrl) && !Console.IsInputRedirected)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("AgriStream IoT Target MVC App Service Endpoint:");
            Console.WriteLine(" - Press [Enter] for local testing: http://localhost:5000/api/telemetry");
            Console.WriteLine(" - Or paste your live Azure URL:    https://<your-app>.azurewebsites.net/api/telemetry");
            Console.Write("Enter URL: ");
            Console.ResetColor();
            var input = Console.ReadLine();
            targetUrl = string.IsNullOrWhiteSpace(input) ? "http://localhost:5000/api/telemetry" : input.Trim();
        }
        else if (string.IsNullOrWhiteSpace(targetUrl))
        {
            targetUrl = "http://localhost:5000/api/telemetry";
        }

        int totalRequests = args.Length > 1 && int.TryParse(args[1], out var tr) ? tr : 1000;
        int concurrency = args.Length > 2 && int.TryParse(args[2], out var c) ? c : 50;

        Console.ForegroundColor = ConsoleColor.Cyan;
        Console.WriteLine("\n======================================================================");
        Console.WriteLine("   AGRISTREAM IoT — HIGH-CONCURRENCY LOAD TESTING ENGINE              ");
        Console.WriteLine("======================================================================");
        Console.ResetColor();
        Console.WriteLine($"Target MVC Endpoint: {targetUrl}");
        Console.WriteLine($"Total Requests:      {totalRequests:N0}");
        Console.WriteLine($"Concurrency Degree:  {concurrency} concurrent workers");
        Console.WriteLine($"Simulation Profile:  Peak Daylight Soil Sensor & Drone Telemetry Surge");
        Console.WriteLine($"Test Started At:     {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
        Console.WriteLine("======================================================================\n");

        var handler = new SocketsHttpHandler
        {
            PooledConnectionLifetime = TimeSpan.FromMinutes(5),
            PooledConnectionIdleTimeout = TimeSpan.FromMinutes(2),
            MaxConnectionsPerServer = concurrency * 2,
            EnableMultipleHttp2Connections = true
        };

        using var httpClient = new HttpClient(handler)
        {
            Timeout = TimeSpan.FromSeconds(15)
        };

        var latencies = new ConcurrentBag<double>();
        var statusCodes = new ConcurrentDictionary<int, int>();
        int completedCount = 0;
        var overallSw = Stopwatch.StartNew();

        var requestIndexes = Enumerable.Range(1, totalRequests).ToArray();

        await Parallel.ForEachAsync(requestIndexes, new ParallelOptions { MaxDegreeOfParallelism = concurrency }, async (index, ct) =>
        {
            var payload = GenerateSensorPayload(index);
            var reqSw = Stopwatch.StartNew();
            int statusCode = 0;

            try
            {
                var response = await httpClient.PostAsJsonAsync(targetUrl, payload, ct);
                statusCode = (int)response.StatusCode;
            }
            catch (Exception)
            {
                statusCode = 0;
            }
            reqSw.Stop();

            latencies.Add(reqSw.Elapsed.TotalMilliseconds);
            statusCodes.AddOrUpdate(statusCode, 1, (_, count) => count + 1);

            int current = Interlocked.Increment(ref completedCount);
            if (current % Math.Max(1, totalRequests / 10) == 0 || current == totalRequests)
            {
                double elapsedSec = overallSw.Elapsed.TotalSeconds;
                double speed = elapsedSec > 0 ? current / elapsedSec : 0;
                double pct = (double)current / totalRequests * 100.0;
                Console.WriteLine($" Progress: [{current,5}/{totalRequests}] ({pct,5:F1}%) | Speed: {speed,6:F1} req/s | Elapsed: {elapsedSec,4:F1}s");
            }
        });

        overallSw.Stop();
        double totalDurationSec = overallSw.Elapsed.TotalSeconds;
        double throughputRps = totalDurationSec > 0 ? totalRequests / totalDurationSec : 0;

        var latencyList = latencies.OrderBy(x => x).ToList();
        double minLatency = latencyList.FirstOrDefault();
        double maxLatency = latencyList.LastOrDefault();
        double avgLatency = latencyList.Any() ? latencyList.Average() : 0;
        double p50 = latencyList.Any() ? latencyList[(int)(latencyList.Count * 0.50)] : 0;
        double p90 = latencyList.Any() ? latencyList[(int)(latencyList.Count * 0.90)] : 0;
        double p95 = latencyList.Any() ? latencyList[(int)(latencyList.Count * 0.95)] : 0;
        double p99 = latencyList.Any() ? latencyList[(int)(latencyList.Count * 0.99)] : 0;

        int successCount = statusCodes.Where(kvp => kvp.Key >= 200 && kvp.Key < 300).Sum(kvp => kvp.Value);
        int failedCount = totalRequests - successCount;
        double successRate = (double)successCount / totalRequests * 100.0;

        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine("\n======================================================================");
        Console.WriteLine("               AGRISTREAM IoT LOAD TEST RESULTS SUMMARY               ");
        Console.WriteLine("======================================================================");
        Console.ResetColor();
        Console.WriteLine($"Total Duration:       {totalDurationSec:F2} seconds");
        Console.WriteLine($"Throughput:           {throughputRps:F2} requests/second");
        Console.WriteLine($"Successful Requests:  {successCount:N0} ({successRate:F2}%)");
        Console.WriteLine($"Failed Requests:      {failedCount:N0} ({100.0 - successRate:F2}%)");
        Console.WriteLine("----------------------------------------------------------------------");
        Console.WriteLine("HTTP Status Code Breakdown:");
        foreach (var kvp in statusCodes.OrderBy(x => x.Key))
        {
            string label = kvp.Key == 0 ? "TIMEOUT / ERR" : $"HTTP {kvp.Key}";
            Console.WriteLine($"   {label,-15}: {kvp.Value,6} ({(double)kvp.Value / totalRequests * 100,5:F1}%)");
        }
        Console.WriteLine("----------------------------------------------------------------------");
        Console.WriteLine("Latency Distribution (Round-Trip):");
        Console.WriteLine($"   Min Latency:       {minLatency:F2} ms");
        Console.WriteLine($"   Average Latency:   {avgLatency:F2} ms");
        Console.WriteLine($"   Median (P50):      {p50:F2} ms");
        Console.WriteLine($"   90th Percentile:   {p90:F2} ms");
        Console.WriteLine($"   95th Percentile:   {p95:F2} ms");
        Console.WriteLine($"   99th Percentile:   {p99:F2} ms");
        Console.WriteLine($"   Max Latency:       {maxLatency:F2} ms");
        Console.WriteLine("======================================================================\n");

        var report = new
        {
            platform = "AgriStream IoT Real-Time Farm Processing System",
            engine = "C# Parallel Load Testing Engine",
            timestamp = DateTime.UtcNow.ToString("o"),
            target_url = targetUrl,
            configuration = new { total_requests = totalRequests, concurrency = concurrency },
            performance = new
            {
                duration_seconds = Math.Round(totalDurationSec, 2),
                throughput_rps = Math.Round(throughputRps, 2),
                success_rate_percent = Math.Round(successRate, 2),
                status_codes = statusCodes,
                latency_ms = new
                {
                    min = Math.Round(minLatency, 2),
                    avg = Math.Round(avgLatency, 2),
                    p50 = Math.Round(p50, 2),
                    p90 = Math.Round(p90, 2),
                    p95 = Math.Round(p95, 2),
                    p99 = Math.Round(p99, 2),
                    max = Math.Round(maxLatency, 2)
                }
            }
        };

        var jsonOptions = new JsonSerializerOptions { WriteIndented = true };
        string jsonReport = JsonSerializer.Serialize(report, jsonOptions);
        await File.WriteAllTextAsync("csharp_stress_test_report.json", jsonReport);
        Console.WriteLine("Detailed performance report saved to: csharp_stress_test_report.json\n");
    }

    private static object GenerateSensorPayload(int index)
    {
        var rnd = Random.Shared;
        string zone = FarmZones[index % FarmZones.Length];
        double moisture = Math.Round(16.0 + rnd.NextDouble() * 55.0, 1);
        double temp = Math.Round(20.0 + rnd.NextDouble() * 14.0, 1);

        return new
        {
            telemetry_id = $"TEL-LOAD-{index:D5}-{Guid.NewGuid().ToString()[..4].ToUpper()}",
            field_zone = zone,
            sensor_id = $"SENS-SOIL-{(index % 200) + 100}",
            event_type = EventTypes[index % EventTypes.Length],
            soil_moisture_percent = moisture,
            temperature_c = temp,
            humidity_percent = Math.Round(35.0 + rnd.NextDouble() * 40.0, 1),
            ph_level = Math.Round(6.0 + rnd.NextDouble() * 1.2, 2),
            nitrogen_ppm = Math.Round(32.0 + rnd.NextDouble() * 25.0, 1),
            status = moisture < 25.0 ? $"Low Moisture Alert ({moisture}%)" : "Optimal Soil Moisture",
            timestamp = DateTime.UtcNow.ToString("o")
        };
    }
}
