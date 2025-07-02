// See https://aka.ms/new-console-template for more information

using System.Data;
using benchmark_spanner_lib;
using Google.Cloud.Spanner.DataProvider;

var connectionString = "projects/appdev-soda-spanner-staging/instances/knut-test-probers/databases/prober";

var spannerLibRunner = new BenchmarkRunner(() =>
{
    var connection = new SpannerConnection { ConnectionString = connectionString };
    connection.Open();
    return connection;
});
var clientLibRunner = new BenchmarkRunner(() =>
{
    var connection = new Google.Cloud.Spanner.Data.SpannerConnection();
    connection.ConnectionString = $"Data Source={connectionString}";
    connection.Open();
    return connection;
});

var latencies = TestLatency();
Console.WriteLine(Utils.ResultsToString(latencies));

// var decodeTimes = TestQuery();
// Console.WriteLine(Utils.ResultsToString(decodeTimes));
//
// var transactionTimes = TestTransaction();
// Console.WriteLine(Utils.ResultsToString(transactionTimes));

Dictionary<MeasurementKey, Measurement> TestTransaction()
{
    try
    {
        var results = new Dictionary<MeasurementKey, Measurement>();
        foreach (var numThreads in new [] { 1, 5, 10, 50, 100, 200 })
        //foreach (var numThreads in new [] { 10 })
        {
            foreach (var tpsPerThread in new[] { 1, 10, 50 })
            //foreach (var tpsPerThread in new[] { 10 })
            {
                Console.WriteLine($"Testing transactions for ClientLib with {tpsPerThread} TPS and {numThreads} threads...");
                var clientLibLatency = clientLibRunner.MeasureTransaction(TimeSpan.FromSeconds(60), tpsPerThread, numThreads);
                Console.WriteLine($"ClientLib Transaction Latency:{Environment.NewLine}{clientLibLatency}");
                results[new MeasurementKey
                {
                    Library = Library.ClientLib,
                    NumThreads = numThreads,
                    Qps = tpsPerThread,
                }] = clientLibLatency;
                
                Console.WriteLine($"Testing transactions for SpannerLib with {tpsPerThread} TPS and {numThreads} threads...");
                var spannerLibLatency = spannerLibRunner.MeasureTransaction(TimeSpan.FromSeconds(60), tpsPerThread, numThreads);
                Console.WriteLine($"SpannerLib Transaction Latency:{Environment.NewLine}{spannerLibLatency}");
                results[new MeasurementKey
                {
                    Library = Library.SpannerLib,
                    NumThreads = numThreads,
                    Qps = tpsPerThread,
                }] = spannerLibLatency;
            }
        }
        return results;
    }
    catch (Exception e)
    {
        Console.WriteLine("Benchmark failed");
        Console.WriteLine(e);
        throw;
    }
}


Dictionary<MeasurementKey, Measurement> TestLatency()
{
    try
    {
        var results = new Dictionary<MeasurementKey, Measurement>();
        // foreach (var numThreads in new [] { 1, 2, 5, 10 })
        foreach (var numThreads in new [] { 1 })
        {
            // foreach (var qpsPerThread in new[] { 1, 2, 5, 10, 20, 100 })
            foreach (var qpsPerThread in new[] { 1 })
            {
                Console.WriteLine($"Testing latency for SpannerLib with {qpsPerThread} QPS and {numThreads} threads...");
                var spannerLibLatency = spannerLibRunner.MeasureLatency(TimeSpan.FromSeconds(30), qpsPerThread, numThreads);
                Console.WriteLine($"SpannerLib Latency:{Environment.NewLine}{spannerLibLatency}");
                results[new MeasurementKey
                {
                    Library = Library.SpannerLib,
                    NumThreads = numThreads,
                    Qps = qpsPerThread,
                    NumRows = 1,
                }] = spannerLibLatency;

                Console.WriteLine($"Testing latency for ClientLib with {qpsPerThread} QPS and {numThreads} threads...");
                var clientLibLatency = clientLibRunner.MeasureLatency(TimeSpan.FromSeconds(30), qpsPerThread, numThreads);
                Console.WriteLine($"ClientLib Latency:{Environment.NewLine}{clientLibLatency}");
                results[new MeasurementKey
                {
                    Library = Library.ClientLib,
                    NumThreads = numThreads,
                    Qps = qpsPerThread,
                    NumRows = 1,
                }] = clientLibLatency;
            }
        }
        return results;
    }
    catch (Exception e)
    {
        Console.WriteLine("Benchmark failed");
        Console.WriteLine(e);
        throw;
    }
}

Dictionary<MeasurementKey, Measurement> TestQuery()
{
    try
    {
        var results = new Dictionary<MeasurementKey, Measurement>();
        foreach (var numRows in new []{1, 5, 20, 100, 1000, 10000, 50000, 100000, 500000, 1000000})
        //foreach (var numRows in new []{1, 5, 20, 1000, 500000})
        {
            var numIterations = 50;
            if (numRows >= 500000)
            {
                numIterations = 5;
            }

            Console.WriteLine($"Benchmarking Spanner Lib ExecuteQuery with {numRows} rows");
            var measurement = spannerLibRunner.MeasureExecuteQuery(numIterations, numRows);
            Console.WriteLine($"Elapsed time:{Environment.NewLine}{measurement}");
            results[new MeasurementKey
            {
                Library = Library.SpannerLib,
                NumThreads = 1,
                NumRows = numRows,
                Qps = 0,
            }] = measurement;

            Console.WriteLine($"Benchmarking Client Lib ExecuteQuery with {numRows} rows");
            measurement = clientLibRunner.MeasureExecuteQuery(numIterations, numRows);
            Console.WriteLine($"Elapsed time:{Environment.NewLine}{measurement}");
            results[new MeasurementKey
            {
                Library = Library.ClientLib,
                NumThreads = 1,
                NumRows = numRows,
                Qps = 0,
            }] = measurement;
        }
        return results;
    }
    catch (Exception e)
    {
        Console.WriteLine("Benchmark failed");
        Console.WriteLine(e);
        throw;
    }
}

