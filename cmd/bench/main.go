// Concurrent benchmark for dflockd features.
//
// Supports multiple modes: lock (acquire/release), counter (incr/decr),
// kv (set/get/del), signal (emit), and list (push/pop).
//
// Each worker dials persistent TCP connections and uses the low-level
// client protocol, so the benchmark measures operation latency rather
// than TCP connection overhead.
//
// Usage:
//
//	go run ./cmd/bench [--mode lock] [--workers 10] [--rounds 50] [--key bench] \
//	    [--servers host1:port1,host2:port2] [--connections 0]
package main

import (
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
	mode := flag.String("mode", "lock", "benchmark mode: lock, counter, kv, signal, list")
	workers := flag.Int("workers", 10, "number of concurrent workers")
	rounds := flag.Int("rounds", 50, "operations per worker")
	key := flag.String("key", "bench", "key/channel prefix")
	timeout := flag.Int("timeout", 30, "acquire timeout in seconds (lock mode)")
	servers := flag.String("servers", "127.0.0.1:6388", "comma-separated host:port pairs")
	leaseTTL := flag.Int("lease", 10, "lease TTL in seconds (lock mode)")
	connections := flag.Int("connections", 0, "connections per worker (0 = 1 persistent conn)")
	flag.Parse()

	addrs := strings.Split(*servers, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}

	connsPerWorker := *connections
	if connsPerWorker <= 0 {
		connsPerWorker = 1
	}

	fmt.Printf("bench: mode=%s, %d workers x %d rounds (key_prefix=%q, conns/worker=%d)\n\n",
		*mode, *workers, *rounds, *key, connsPerWorker)

	type result struct {
		latencies []float64
		err       error
	}

	// Validate mode.
	var workerFn func(key, addr string, rounds, connsPerWorker int, extra workerExtra) ([]float64, error)
	extra := workerExtra{timeoutSec: *timeout, leaseTTL: *leaseTTL}

	switch *mode {
	case "lock":
		workerFn = workerLock
	case "counter":
		workerFn = workerCounter
	case "kv":
		workerFn = workerKV
	case "signal":
		workerFn = workerSignal
	case "list":
		workerFn = workerList
	default:
		fmt.Fprintf(os.Stderr, "unknown mode: %s (valid: lock, counter, kv, signal, list)\n", *mode)
		os.Exit(1)
	}

	results := make([]result, *workers)
	var wg sync.WaitGroup

	wallStart := time.Now()

	for i := range *workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			workerKey := fmt.Sprintf("%s_%d", *key, rand.IntN(9900000)+100000)
			addr := addrs[id%len(addrs)]
			lats, err := workerFn(workerKey, addr, *rounds, connsPerWorker, extra)
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

	mn := mean(all)
	minimum := all[0]
	maximum := all[totalOps-1]
	p50 := percentile(all, 50)
	p99 := percentile(all, 99)
	sd := stdev(all, mn)

	fmt.Printf("  total ops : %d\n", totalOps)
	fmt.Printf("  wall time : %.3fs\n", wall)
	fmt.Printf("  throughput: %.1f ops/s\n", float64(totalOps)/wall)
	fmt.Println()
	fmt.Printf("  mean      : %.3f ms\n", mn*1000)
	fmt.Printf("  min       : %.3f ms\n", minimum*1000)
	fmt.Printf("  max       : %.3f ms\n", maximum*1000)
	fmt.Printf("  p50       : %.3f ms\n", p50*1000)
	fmt.Printf("  p99       : %.3f ms\n", p99*1000)
	fmt.Printf("  stdev     : %.3f ms\n", sd*1000)
}

// workerExtra holds mode-specific parameters.
type workerExtra struct {
	timeoutSec int
	leaseTTL   int
}

// dialConns opens numConns persistent connections to addr.
func dialConns(addr string, numConns int) ([]*client.Conn, error) {
	conns := make([]*client.Conn, numConns)
	for i := range conns {
		c, err := client.Dial(addr)
		if err != nil {
			// Close any already-opened connections.
			for j := range i {
				conns[j].Close()
			}
			return nil, fmt.Errorf("dial: %w", err)
		}
		conns[i] = c
	}
	return conns, nil
}

// closeConns closes all connections.
func closeConns(conns []*client.Conn) {
	for _, c := range conns {
		c.Close()
	}
}

// ---------------------------------------------------------------------------
// Lock mode: acquire + release
// ---------------------------------------------------------------------------

func workerLock(key, addr string, rounds, numConns int, extra workerExtra) ([]float64, error) {
	conns, err := dialConns(addr, numConns)
	if err != nil {
		return nil, err
	}
	defer closeConns(conns)

	acquireTimeout := time.Duration(extra.timeoutSec) * time.Second
	var opts []client.Option
	if extra.leaseTTL > 0 {
		opts = append(opts, client.WithLeaseTTL(extra.leaseTTL))
	}

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

// ---------------------------------------------------------------------------
// Counter mode: incr + decr (net zero per round)
// ---------------------------------------------------------------------------

func workerCounter(key, addr string, rounds, numConns int, _ workerExtra) ([]float64, error) {
	conns, err := dialConns(addr, numConns)
	if err != nil {
		return nil, err
	}
	defer closeConns(conns)

	latencies := make([]float64, 0, rounds)
	for i := range rounds {
		c := conns[i%len(conns)]
		t0 := time.Now()
		if _, err := client.Incr(c, key, 1); err != nil {
			return nil, fmt.Errorf("incr: %w", err)
		}
		if _, err := client.Decr(c, key, 1); err != nil {
			return nil, fmt.Errorf("decr: %w", err)
		}
		latencies = append(latencies, time.Since(t0).Seconds())
	}
	return latencies, nil
}

// ---------------------------------------------------------------------------
// KV mode: set + get + delete per round
// ---------------------------------------------------------------------------

func workerKV(key, addr string, rounds, numConns int, _ workerExtra) ([]float64, error) {
	conns, err := dialConns(addr, numConns)
	if err != nil {
		return nil, err
	}
	defer closeConns(conns)

	latencies := make([]float64, 0, rounds)
	for i := range rounds {
		c := conns[i%len(conns)]
		value := fmt.Sprintf("val_%d", i)
		t0 := time.Now()
		if err := client.KVSet(c, key, value, 0); err != nil {
			return nil, fmt.Errorf("kset: %w", err)
		}
		if _, err := client.KVGet(c, key); err != nil {
			return nil, fmt.Errorf("kget: %w", err)
		}
		if err := client.KVDel(c, key); err != nil {
			return nil, fmt.Errorf("kdel: %w", err)
		}
		latencies = append(latencies, time.Since(t0).Seconds())
	}
	return latencies, nil
}

// ---------------------------------------------------------------------------
// Signal mode: emit to a channel per round (fire-and-forget, no listeners)
// ---------------------------------------------------------------------------

func workerSignal(key, addr string, rounds, numConns int, _ workerExtra) ([]float64, error) {
	conns, err := dialConns(addr, numConns)
	if err != nil {
		return nil, err
	}
	defer closeConns(conns)

	latencies := make([]float64, 0, rounds)
	for i := range rounds {
		c := conns[i%len(conns)]
		payload := fmt.Sprintf("bench_%d", i)
		t0 := time.Now()
		if _, err := client.Emit(c, key, payload); err != nil {
			return nil, fmt.Errorf("signal: %w", err)
		}
		latencies = append(latencies, time.Since(t0).Seconds())
	}
	return latencies, nil
}

// ---------------------------------------------------------------------------
// List mode: rpush + lpop per round (FIFO queue pattern)
// ---------------------------------------------------------------------------

func workerList(key, addr string, rounds, numConns int, _ workerExtra) ([]float64, error) {
	conns, err := dialConns(addr, numConns)
	if err != nil {
		return nil, err
	}
	defer closeConns(conns)

	latencies := make([]float64, 0, rounds)
	for i := range rounds {
		c := conns[i%len(conns)]
		value := fmt.Sprintf("item_%d", i)
		t0 := time.Now()
		if _, err := client.RPush(c, key, value); err != nil {
			return nil, fmt.Errorf("rpush: %w", err)
		}
		if _, err := client.LPop(c, key); err != nil {
			return nil, fmt.Errorf("lpop: %w", err)
		}
		latencies = append(latencies, time.Since(t0).Seconds())
	}
	return latencies, nil
}

// ---------------------------------------------------------------------------
// Stats helpers
// ---------------------------------------------------------------------------

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
	return math.Sqrt(sum / float64(len(data) - 1))
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
