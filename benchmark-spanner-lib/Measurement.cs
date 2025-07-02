namespace benchmark_spanner_lib;

public struct Measurement
{
    public int Num;
    public TimeSpan Min;
    public TimeSpan Max;
    public TimeSpan Avg;
    public TimeSpan P50;
    public TimeSpan P90;
    public TimeSpan P95;
    public TimeSpan P99;

    public override string ToString()
    {
        return $"Num: {Num}{Environment.NewLine}" +
               $"Min: {Min}{Environment.NewLine}" +
               $"Max: {Max}{Environment.NewLine}" +
               $"Avg: {Avg}{Environment.NewLine}" +
               $"P50: {P50}{Environment.NewLine}" +
               $"P90: {P90}{Environment.NewLine}" +
               $"P95: {P95}{Environment.NewLine}" +
               $"P99: {P99}{Environment.NewLine}";
    }
}
