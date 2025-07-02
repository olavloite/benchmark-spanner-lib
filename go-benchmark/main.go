package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

func main() {
	client, err := spanner.NewClient(context.Background(), "projects/appdev-soda-spanner-staging/instances/knut-test-probers/databases/prober")
	if err != nil {
		panic(err)
	}
	defer client.Close()

	itWarmup := client.Single().Query(context.Background(), spanner.NewStatement("select 1"))
	consumeIterator(itWarmup)

	durations := make([]time.Duration, 0, 50)
	for range 50 {
		startTime := time.Now()
		it := client.Single().Query(context.Background(), spanner.NewStatement("select * from all_types limit 10000"))
		consumeIterator(it)
		endTime := time.Now()
		duration := endTime.Sub(startTime)
		durations = append(durations, duration)
		fmt.Printf("Duration %s\n", duration)
	}
	fmt.Printf("Avg %s\n", avg(durations))
}

func avg(durations []time.Duration) time.Duration {
	sum := time.Duration(0)
	for _, d := range durations {
		sum += d
	}
	return sum / time.Duration(len(durations))
}

func consumeIterator(it *spanner.RowIterator) {
	defer it.Stop()
	for {
		row, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			panic(err)
		}
		cols := row.Size()
		for i := 0; i < cols; i++ {
			_ = row.ColumnValue(i)
		}
	}
}
