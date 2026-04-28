// Concurrent benchmark: N goroutine workers each acquire/release locks
// repeatedly and report latency statistics. By default each worker uses a
// unique key; --shared-key makes all workers contend on one lock.
//
// Each worker dials a persistent TCP connection and uses the low-level
// Acquire/Release protocol, so the benchmark measures lock latency rather
// than TCP connection overhead.
//
// Usage:
//
//	go run ./cmd/bench [--workers 10] [--rounds 50] [--key bench] \
//	    [--servers host1:port1,host2:port2] [--connections 0]
package main

import (
	"flag"
	"fmt"
	"math"
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
	key := flag.String("key", "bench", "lock key (used as a prefix unless --shared-key is set)")
	timeout := flag.Int("timeout", 30, "acquire timeout in seconds")
	servers := flag.String("servers", "127.0.0.1:6388", "comma-separated host:port pairs")
	leaseTTL := flag.Int("lease", 10, "lease TTL in seconds")
	connections := flag.Int("connections", 0, "connections per worker (0 = 1 persistent conn per worker)")
	warmup := flag.Int("warmup", 10, "warmup rounds per worker (not measured)")
	sharedKey := flag.Bool("shared-key", false, "all workers contend on the literal --key value (measures single-key throughput). "+
		"Default is to append the worker ID, so workers use unique keys and throughput scales with sharding.")
	flag.Parse()

	addrs, connsPerWorker, err := validateBenchFlags(*workers, *rounds, *timeout, *leaseTTL, *connections, *warmup, *servers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench: %v\n", err)
		os.Exit(2)
	}

	mode := "unique per-worker keys"
	displayKey := *key + "_<id>"
	if *sharedKey {
		mode = "shared key (contended)"
		displayKey = *key
	}
	fmt.Printf("bench: %d workers x %d rounds (key=%q, conns/worker=%d, %s)\n\n",
		*workers, *rounds, displayKey, connsPerWorker, mode)

	type result struct {
		latencies []float64
		err       error
	}

	results := make([]result, *workers)
	var wg sync.WaitGroup
	var warmupWg sync.WaitGroup    // tracks warmup completion
	startCh := make(chan struct{}) // closed to signal "go measure"

	warmupWg.Add(*workers)

	for i := range *workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			workerKey := workerKeyFor(*key, id, *sharedKey)
			addr := addrs[id%len(addrs)]
			lats, err := worker(workerKey, addr, *rounds, *timeout, *leaseTTL, connsPerWorker, *warmup, &warmupWg, startCh)
			results[id] = result{latencies: lats, err: err}
		}(i)
	}

	// Wait for all workers to finish warmup, then start measurement.
	warmupWg.Wait()
	wallStart := time.Now()
	close(startCh)

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
	if totalOps == 0 {
		fmt.Fprintln(os.Stderr, "no operations completed")
		os.Exit(1)
	}
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

// workerKeyFor picks the lock key for a given worker id. When shared is
// true every worker contends on the same literal key (measures single-key
// throughput). Otherwise the worker id is appended so each worker has its
// own key and throughput scales with the server's shard count.
func workerKeyFor(baseKey string, id int, shared bool) string {
	if shared {
		return baseKey
	}
	return fmt.Sprintf("%s_%d", baseKey, id)
}

func validateBenchFlags(workers, rounds, timeoutSec, leaseTTL, connections, warmup int, servers string) ([]string, int, error) {
	if workers <= 0 {
		return nil, 0, fmt.Errorf("--workers must be > 0")
	}
	if rounds <= 0 {
		return nil, 0, fmt.Errorf("--rounds must be > 0")
	}
	if timeoutSec < 0 {
		return nil, 0, fmt.Errorf("--timeout must be >= 0")
	}
	if leaseTTL < 0 {
		return nil, 0, fmt.Errorf("--lease must be >= 0")
	}
	if connections < 0 {
		return nil, 0, fmt.Errorf("--connections must be >= 0")
	}
	if warmup < 0 {
		return nil, 0, fmt.Errorf("--warmup must be >= 0")
	}

	addrs := strings.Split(servers, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
		if addrs[i] == "" {
			return nil, 0, fmt.Errorf("--servers must not contain empty addresses")
		}
	}

	connsPerWorker := connections
	if connsPerWorker == 0 {
		connsPerWorker = 1
	}
	return addrs, connsPerWorker, nil
}

func worker(key, addr string, rounds, timeoutSec, leaseTTL, numConns, warmupRounds int, warmupWg *sync.WaitGroup, startCh <-chan struct{}) ([]float64, error) {
	// Open persistent connection(s) up front.
	conns := make([]*client.Conn, 0, numConns)
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	for i := 0; i < numConns; i++ {
		c, err := client.Dial(addr)
		if err != nil {
			warmupWg.Done() // unblock barrier so main doesn't hang
			return nil, fmt.Errorf("dial: %w", err)
		}
		conns = append(conns, c)
	}

	acquireTimeout := time.Duration(timeoutSec) * time.Second
	var opts []client.Option
	if leaseTTL > 0 {
		opts = append(opts, client.WithLeaseTTL(leaseTTL))
	}

	// Warmup: run unmeasured rounds to let the server and runtime stabilize.
	for i := range warmupRounds {
		c := conns[i%len(conns)]
		token, _, err := client.Acquire(c, key, acquireTimeout, opts...)
		if err != nil {
			warmupWg.Done()
			return nil, fmt.Errorf("warmup acquire: %w", err)
		}
		if token == "" {
			warmupWg.Done()
			return nil, fmt.Errorf("warmup acquire timed out")
		}
		if err := client.Release(c, key, token); err != nil {
			warmupWg.Done()
			return nil, fmt.Errorf("warmup release: %w", err)
		}
	}

	// Signal warmup done and wait for all workers to be ready.
	warmupWg.Done()
	<-startCh

	latencies := make([]float64, 0, rounds)
	for i := range rounds {
		c := conns[i%len(conns)]
		t0 := time.Now()
		token, _, err := client.Acquire(c, key, acquireTimeout, opts...)
		if err != nil {
			return nil, fmt.Errorf("acquire: %w", err)
		}
		if token == "" {
			return nil, fmt.Errorf("acquire timed out")
		}
		if err := client.Release(c, key, token); err != nil {
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
