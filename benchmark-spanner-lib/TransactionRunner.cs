using System.Data;
using System.Data.Common;

namespace benchmark_spanner_lib;

public class TransactionRunner
{
    private readonly DbConnection _connection;
    private readonly List<TimeSpan> _measurements = new ();
    private readonly TimeSpan _duration;
    private readonly int _tps;

    public TransactionRunner(DbConnection connection, TimeSpan duration, int tps)
    {
        _connection = connection;
        _duration = duration;
        _tps = tps;
    }

    public void RunTransaction()
    {
        var waitTime = TimeSpan.FromSeconds(1).Ticks / _tps;
        var start = DateTime.Now;
        while (DateTime.Now - start < _duration)
        {
            try
            {
                var randomWait = Random.Shared.NextInt64(2 * waitTime);
                Thread.Sleep(TimeSpan.FromTicks(randomWait));
                _measurements.Add(MeasureTransaction());
            }
            catch (Exception e)
            {
                Console.WriteLine($"Transaction failed: {e.Message}");
            }
        }
    }

    private TimeSpan MeasureTransaction()
    {
        return Utils.Measure(Transaction);
    }

    private void Transaction()
    {
        using var transaction = _connection.BeginTransaction();
        try
        {
            var numUpdates = Random.Shared.Next(5, 15);
            for (var i = 0; i < numUpdates; i++)
            {
                var id = Random.Shared.NextInt64(1, 10000001);
                using var cmd = _connection.CreateCommand();
                cmd.Transaction = transaction;
                cmd.CommandText = "select col_varchar, col_float8 from all_types where col_bigint=$1";
                var param = cmd.CreateParameter();
                param.ParameterName = "p1";
                param.Value = id;
                cmd.Parameters.Add(param);
                using var reader = cmd.ExecuteReader();
                if (reader.Read())
                {
                    var value = reader.GetString("col_varchar");
                    if (string.IsNullOrEmpty(value))
                    {
                        throw new DataException("got unexpected empty or null value");
                    }

                    var randomBit = Random.Shared.Next(0, 2);
                    var randomMod = value[0] % 2;
                    if (randomMod == randomBit)
                    {
                        UpdateRow(id, transaction);
                    }
                }
                else
                {
                    throw new DataException("no row found");
                }
            }

            transaction.Commit();
        }
        catch (Exception)
        {
            try
            {
                transaction.Rollback();
            }
            catch (Exception)
            {
                // ignore
            }
            throw;
        }
    }

    private void UpdateRow(long id, DbTransaction transaction)
    {
        using var cmd = _connection.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = "update all_types set col_float8=$1 where col_bigint=$2";
        var valueParam = cmd.CreateParameter();
        valueParam.ParameterName = "p1";
        valueParam.Value = Random.Shared.NextDouble();
        cmd.Parameters.Add(valueParam);
        var idParam = cmd.CreateParameter();
        idParam.ParameterName = "p2";
        idParam.Value = id;
        cmd.Parameters.Add(idParam);
        var affected = cmd.ExecuteNonQuery();
        if (affected != 1)
        {
            throw new DataException($"unexpected affected row count: {affected}");
        }
    }

    public void Close()
    {
        _connection.Close();
    }

    public List<TimeSpan> GetMeasurements()
    {
        return _measurements;
    }
    
}