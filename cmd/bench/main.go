// Concurrent benchmark: N goroutine workers each acquire/release a shared lock
// repeatedly and report latency statistics.
//
// Usage:
//
//	go run ./cmd/bench [--workers 10] [--rounds 50] [--key bench] \
//	    [--servers host1:port1,host2:port2]
package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mtingers/dflockd/client"
)

func main() {
	workers := flag.Int("workers", 10, "number of concurrent workers")
	rounds := flag.Int("rounds", 50, "acquire/release rounds per worker")
	key := flag.String("key", "bench", "lock key prefix")
	timeout := flag.Int("timeout", 30, "acquire timeout in seconds")
	servers := flag.String("servers", "127.0.0.1:6388", "comma-separated host:port pairs")
	leaseTTL := flag.Int("lease", 10, "lease TTL in seconds")
	flag.Parse()

	addrs := strings.Split(*servers, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}

	fmt.Printf("bench: %d workers x %d rounds (key_prefix=%q)\n\n", *workers, *rounds, *key)

	type result struct {
		latencies []float64
		err       error
	}

	results := make([]result, *workers)
	var wg sync.WaitGroup

	wallStart := time.Now()

	for i := range *workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			workerKey := fmt.Sprintf("%s_%d", *key, rand.IntN(9900000)+100000)
			lats, err := worker(workerKey, *rounds, *timeout, *leaseTTL, addrs)
			results[id] = result{latencies: lats, err: err}
		}(i)
	}

	wg.Wait()
	wall := time.Since(wallStart).Seconds()

	var all []float64
	for i, r := range results {
		if r.err != nil {
			fmt.Fprintf(os.Stderr, "worker %d error: %v\n", i, r.err)
			os.Exit(1)
		}
		all = append(all, r.latencies...)
	}

	totalOps := len(all)
	sort.Float64s(all)

	mean := mean(all)
	mn := all[0]
	mx := all[totalOps-1]
	p50 := percentile(all, 50)
	p99 := percentile(all, 99)
	sd := stdev(all, mean)

	fmt.Printf("  total ops : %d\n", totalOps)
	fmt.Printf("  wall time : %.3fs\n", wall)
	fmt.Printf("  throughput: %.1f ops/s\n", float64(totalOps)/wall)
	fmt.Println()
	fmt.Printf("  mean      : %.3f ms\n", mean*1000)
	fmt.Printf("  min       : %.3f ms\n", mn*1000)
	fmt.Printf("  max       : %.3f ms\n", mx*1000)
	fmt.Printf("  p50       : %.3f ms\n", p50*1000)
	fmt.Printf("  p99       : %.3f ms\n", p99*1000)
	fmt.Printf("  stdev     : %.3f ms\n", sd*1000)
}

func worker(key string, rounds, timeoutSec, leaseTTL int, servers []string) ([]float64, error) {
	latencies := make([]float64, 0, rounds)
	for range rounds {
		t0 := time.Now()
		l := &client.Lock{
			Key:            key,
			AcquireTimeout: time.Duration(timeoutSec) * time.Second,
			LeaseTTL:       leaseTTL,
			Servers:        servers,
		}
		ok, err := l.Acquire(context.Background())
		if err != nil {
			return nil, fmt.Errorf("acquire: %w", err)
		}
		if !ok {
			return nil, fmt.Errorf("acquire timed out")
		}
		if err := l.Release(context.Background()); err != nil {
			return nil, fmt.Errorf("release: %w", err)
		}
		latencies = append(latencies, time.Since(t0).Seconds())
	}
	return latencies, nil
}

func mean(data []float64) float64 {
	var sum float64
	for _, v := range data {
		sum += v
	}
	return sum / float64(len(data))
}

func stdev(data []float64, mean float64) float64 {
	if len(data) < 2 {
		return 0
	}
	var sum float64
	for _, v := range data {
		d := v - mean
		sum += d * d
	}
	return math.Sqrt(sum / float64(len(data)-1))
}

func percentile(sorted []float64, pct float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := pct / 100.0 * float64(len(sorted)-1)
	lo := int(rank)
	hi := lo + 1
	if hi >= len(sorted) {
		return sorted[lo]
	}
	frac := rank - float64(lo)
	return sorted[lo]*(1-frac) + sorted[hi]*frac
}
