namespace benchmark_spanner_lib;

public struct MeasurementKey : IEquatable<MeasurementKey>, IComparable<MeasurementKey>
{
    public Library Library;
    public int NumThreads;
    public int Qps;
    public int NumRows;

    public bool Equals(MeasurementKey other)
    {
        return Library == other.Library && NumThreads == other.NumThreads && Qps == other.Qps && NumRows == other.NumRows;
    }

    public int CompareTo(MeasurementKey other)
    {
        if (NumThreads != other.NumThreads)
        {
            return NumThreads.CompareTo(other.NumThreads);
        }
        if (Qps != other.Qps)
        {
            return Qps.CompareTo(other.Qps);
        }
        if (NumRows != other.NumRows)
        {
            return NumRows.CompareTo(other.NumRows);
        }
        return Library.CompareTo(other.Library);
    }

    public override bool Equals(object? obj)
    {
        return obj is MeasurementKey other && Equals(other);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine((int)Library, NumThreads, Qps, NumRows);
    }
}
