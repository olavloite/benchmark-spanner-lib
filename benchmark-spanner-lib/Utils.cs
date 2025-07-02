using System.Data.Common;
using System.Text;

namespace benchmark_spanner_lib;

public static class Utils
{
    public static string ResultsToString(Dictionary<MeasurementKey, Measurement> results)
    {
        var builder = new StringBuilder();
        builder.Append($";;;SpannerLib;ClientLib;SpannerLib;ClientLib;SpannerLib;ClientLib;SpannerLib;ClientLib;SpannerLib;ClientLib;SpannerLib;ClientLib;SpannerLib;ClientLib{Environment.NewLine}");
        builder.Append($"NumThreads;Qps;NumRows;P50;P50;P90;P90;P95;P95;P99;P99;Min;Min;Max;Max;Avg;Avg{Environment.NewLine}");
        foreach (var key in results.Keys.Order())
        {
            if (key.Library == Library.SpannerLib)
            {
                var spannerLibResult = results[key];
                var clientLibKey = key;
                clientLibKey.Library = Library.ClientLib;
                var clientLibResult = results[clientLibKey];
                builder.Append($"{key.NumThreads};{key.Qps};{key.NumRows};" +
                               $"{(long)spannerLibResult.P50.TotalMicroseconds};" +
                               $"{(long)clientLibResult.P50.TotalMicroseconds};" +
                               $"{(long)spannerLibResult.P90.TotalMicroseconds};" +
                               $"{(long)clientLibResult.P90.TotalMicroseconds};" +
                               $"{(long)spannerLibResult.P95.TotalMicroseconds};" +
                               $"{(long)clientLibResult.P95.TotalMicroseconds};" +
                               $"{(long)spannerLibResult.P99.TotalMicroseconds};" +
                               $"{(long)clientLibResult.P99.TotalMicroseconds};" +
                               $"{(long)spannerLibResult.Min.TotalMicroseconds};" +
                               $"{(long)clientLibResult.Min.TotalMicroseconds};" +
                               $"{(long)spannerLibResult.Max.TotalMicroseconds};" +
                               $"{(long)clientLibResult.Max.TotalMicroseconds};" +
                               $"{(long)spannerLibResult.Avg.TotalMicroseconds};" +
                               $"{(long)clientLibResult.Avg.TotalMicroseconds}" +
                               $"{Environment.NewLine}");
            }
        }
        return builder.ToString();
    }

    public static Measurement ExtractMeasurement(List<TimeSpan> measurements)
    {
        measurements.Sort();
        var result = new Measurement
        {
            Num = measurements.Count,
            Max = measurements.Max(),
            Min = measurements.Min(),
            Avg = new TimeSpan((long) measurements.Select(m => m.Ticks).Average()),
            P50 = GetPercentile(measurements, 50),
            P90 = GetPercentile(measurements, 90),
            P95 = GetPercentile(measurements, 95),
            P99 = GetPercentile(measurements, 99),
        };
        return result;
    }

    private static TimeSpan GetPercentile(List<TimeSpan> measurements, int percentile)
    {
        var index = measurements.Count * percentile / 100;
        return measurements[index];
    }
    
    public static TimeSpan Measure(Action action)
    {
        var watch = System.Diagnostics.Stopwatch.StartNew();
        action.Invoke();
        watch.Stop();
        return watch.Elapsed;
    }

    public static async Task<TimeSpan> MeasureAsync(Func<Task> action)
    {
        var watch = System.Diagnostics.Stopwatch.StartNew();
        await action.Invoke();
        watch.Stop();
        return watch.Elapsed;
    }
    
    public static async Task<int> ConsumeReader(DbDataReader reader)
    {
        // var columnsBuilder = new StringBuilder();
        // for (var col = 0; col < reader.FieldCount; col++)
        // {
        //     columnsBuilder.Append(reader.GetName(col));
        //     columnsBuilder.Append(",");
        // }
        // columnsBuilder.Append(Environment.NewLine);
        // var columns = columnsBuilder.ToString();
        var rowCount = 0;
        while (await reader.ReadAsync())
        {
            rowCount++;
            // var lineBuilder = new StringBuilder();
            // for (var i = 0; i < reader.FieldCount; i++)
            // {
            //     lineBuilder.Append(reader.GetValue(i));
            //     lineBuilder.Append(",");
            // }
            // lineBuilder.Append(Environment.NewLine);
            // var line = lineBuilder.ToString();
        }
        return rowCount;
    }
}