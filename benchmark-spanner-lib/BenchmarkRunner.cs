using System.Data;
using System.Data.Common;
using Google.Api.Gax;

namespace benchmark_spanner_lib;

public class BenchmarkRunner
{
    private readonly Func<DbConnection> _connectionFactory;

    public BenchmarkRunner(Func<DbConnection> connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

    private static void Warmup(DbConnection connection)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "select 1";
        using var reader = cmd.ExecuteReader();
        Utils.ConsumeReader(reader).WaitWithUnwrappedExceptions();
    }

    public Measurement MeasureTransaction(TimeSpan duration, int tpsPerThread, int numThreads)
    {
        var threads = new Thread[numThreads];
        var runners = new TransactionRunner[numThreads];
        for (var i = 0; i < numThreads; i++)
        {
            var connection = _connectionFactory();
            runners[i] = new TransactionRunner(connection, duration, tpsPerThread);
            threads[i] = new Thread(runners[i].RunTransaction);
            threads[i].Start();
        }
        for (var i = 0; i < numThreads; i++)
        {
            threads[i].Join();
        }
        List<TimeSpan> allMeasurements = new List<TimeSpan>();
        foreach (var runner in runners)
        {
            runner.Close();
            allMeasurements.AddRange(runner.GetMeasurements());
        }
        return Utils.ExtractMeasurement(allMeasurements);
    }

    public Measurement MeasureLatency(TimeSpan duration, int qpsPerThread, int numThreads)
    {
        var threads = new Thread[numThreads];
        var runners = new PointSelectRunner[numThreads];
        for (var i = 0; i < numThreads; i++)
        {
            var connection = _connectionFactory();
            runners[i] = new PointSelectRunner(connection, duration, qpsPerThread);
            threads[i] = new Thread(runners[i].RunPointSelect);
            threads[i].Start();
        }
        for (var i = 0; i < numThreads; i++)
        {
            threads[i].Join();
        }
        List<TimeSpan> allMeasurements = new List<TimeSpan>();
        foreach (var runner in runners)
        {
            runner.Close();
            allMeasurements.AddRange(runner.GetMeasurements());
        }
        return Utils.ExtractMeasurement(allMeasurements);
    }

    private class PointSelectRunner
    {
        private readonly DbConnection _connection;
        private readonly List<TimeSpan> _measurements = new ();
        private readonly TimeSpan _duration;
        private readonly int _qps;

        public PointSelectRunner(DbConnection connection, TimeSpan duration, int qps)
        {
            _connection = connection;
            _duration = duration;
            _qps = qps;
        }

        public void Close()
        {
            _connection.Close();
        }

        public List<TimeSpan> GetMeasurements()
        {
            return _measurements;
        }

        public void RunPointSelect()
        {
            Warmup(_connection);
            var waitTime = TimeSpan.FromSeconds(1).Ticks / _qps;
            var start = DateTime.Now;
            while (DateTime.Now - start < _duration)
            {
                var randomWait = Random.Shared.NextInt64(2 * waitTime);
                Thread.Sleep(TimeSpan.FromTicks(randomWait));
                _measurements.Add(MeasurePointSelect().Result);
            }
        }

        private async Task<TimeSpan> MeasurePointSelect()
        {
            return await Utils.MeasureAsync(async () => await PointSelect());
        }

        private async Task PointSelect()
        {
            var id = Random.Shared.NextInt64(1, 10000001);
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = "select col_varchar from all_types where col_bigint=$1";
            var param = cmd.CreateParameter();
            param.ParameterName = "p1";
            param.Value = id;
            cmd.Parameters.Add(param);
            await using var reader = cmd.ExecuteReader();
            if (await reader.ReadAsync())
            {
                var value = reader.GetString(0);
                if (value == null)
                {
                    throw new DataException("got unexpected null value");
                }
            }
            else
            {
                throw new DataException("no row found");
            }
        }
    }

    public Measurement MeasureExecuteQuery(int numIterations, int numRows)
    {
        using var connection = _connectionFactory();
        Warmup(connection);
        var durations = new List<TimeSpan>(numIterations);
        for (var i = 0; i < numIterations; i++)
        {
            durations.Add(MeasureExecuteQuery(connection, numRows).Result);
        }
        return Utils.ExtractMeasurement(durations);
    }

    public Task<TimeSpan> MeasureExecuteQuery(DbConnection connection, int numRows = 1000)
    {
        return Utils.MeasureAsync(async () => await ExecuteQuery(connection, numRows));
    }

    public async Task ExecuteQuery(DbConnection connection, int numRows = 1000)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"select * from all_types limit {numRows}";
        await using var reader = cmd.ExecuteReader();
        await Utils.ConsumeReader(reader);
    }
}