// Concurrent benchmark: N goroutine workers each acquire/release locks
// repeatedly and report latency statistics. By default each worker uses a
// unique key; --shared-key makes all workers contend on one lock.
//
// Two transports are supported:
//
//   - TCP (default): each worker dials a persistent TCP connection and
//     uses the low-level Acquire/Release protocol.
//
//   - HTTP (--http): each worker creates one HTTP session, reuses an
//     http.Client (keep-alive), and runs acquire/release via the REST
//     endpoints. Use --servers with http://host:port URLs.
//
// Usage:
//
//	go run ./cmd/bench [--workers 10] [--rounds 50] [--key bench] \
//	    [--servers host1:port1,host2:port2] [--connections 0]
//	go run ./cmd/bench --http --servers http://127.0.0.1:6389 [...]
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mtingers/dflockd/client"
)

type benchFlags struct {
	workers, rounds, timeout, leaseTTL, connections, warmup int
	key, servers, authToken                                 string
	sharedKey, httpMode                                     bool
}

func parseBenchFlags() benchFlags {
	var f benchFlags
	flag.IntVar(&f.workers, "workers", 10, "number of concurrent workers")
	flag.IntVar(&f.rounds, "rounds", 50, "acquire/release rounds per worker")
	flag.StringVar(&f.key, "key", "bench", "lock key (used as a prefix unless --shared-key is set)")
	flag.IntVar(&f.timeout, "timeout", 30, "acquire timeout in seconds")
	flag.StringVar(&f.servers, "servers", "127.0.0.1:6388", "comma-separated host:port pairs")
	flag.IntVar(&f.leaseTTL, "lease", 10, "lease TTL in seconds")
	flag.IntVar(&f.connections, "connections", 0, "connections per worker (0 = 1 persistent conn per worker)")
	flag.IntVar(&f.warmup, "warmup", 10, "warmup rounds per worker (not measured)")
	flag.BoolVar(&f.sharedKey, "shared-key", false, "all workers contend on the literal --key value (measures single-key throughput). "+
		"Default is to append the worker ID, so workers use unique keys and throughput scales with sharding.")
	flag.BoolVar(&f.httpMode, "http", false, "drive the HTTP REST API instead of TCP. --servers entries must be http(s)://host:port.")
	flag.StringVar(&f.authToken, "auth-token", "", "shared secret for authentication (TCP: protocol auth; HTTP: Bearer header)")
	flag.Parse()
	return f
}

type benchResult struct {
	latencies []float64
	err       error
}

func main() {
	f := parseBenchFlags()
	addrs, connsPerWorker := mustResolveBenchAddrs(f)
	printBenchHeader(f, connsPerWorker)
	results, wall := runBenchWorkers(f, addrs, connsPerWorker)
	all := mustCollectLatencies(results)
	printBenchStats(all, wall)
}

// mustResolveBenchAddrs validates flags and (for --http) requires HTTP
// URLs; exits the process with status 2 on failure.
func mustResolveBenchAddrs(f benchFlags) ([]string, int) {
	addrs, connsPerWorker, err := validateBenchFlags(f.workers, f.rounds, f.timeout, f.leaseTTL, f.connections, f.warmup, f.servers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench: %v\n", err)
		os.Exit(2)
	}
	if f.httpMode {
		if err := validateHTTPAddrs(addrs); err != nil {
			fmt.Fprintf(os.Stderr, "bench: %v\n", err)
			os.Exit(2)
		}
	}
	return addrs, connsPerWorker
}

func printBenchHeader(f benchFlags, connsPerWorker int) {
	mode := "unique per-worker keys"
	displayKey := f.key + "_<id>"
	if f.sharedKey {
		mode = "shared key (contended)"
		displayKey = f.key
	}
	transport := "TCP"
	if f.httpMode {
		transport = "HTTP"
	}
	fmt.Printf("bench: %d workers x %d rounds (key=%q, conns/worker=%d, transport=%s, %s)\n\n",
		f.workers, f.rounds, displayKey, connsPerWorker, transport, mode)
}

// runBenchWorkers spawns workers, waits for warmup completion, opens
// the start gate, joins, and returns the per-worker results plus the
// measured wall time.
func runBenchWorkers(f benchFlags, addrs []string, connsPerWorker int) ([]benchResult, float64) {
	results := make([]benchResult, f.workers)
	var wg, warmupWg sync.WaitGroup
	startCh := make(chan struct{})
	warmupWg.Add(f.workers)

	for i := range f.workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			results[id] = runOneWorker(f, addrs, connsPerWorker, id, &warmupWg, startCh)
		}(i)
	}

	warmupWg.Wait()
	wallStart := time.Now()
	close(startCh)
	wg.Wait()
	return results, time.Since(wallStart).Seconds()
}

func runOneWorker(f benchFlags, addrs []string, connsPerWorker, id int, warmupWg *sync.WaitGroup, startCh <-chan struct{}) benchResult {
	workerKey := workerKeyFor(f.key, id, f.sharedKey)
	addr := addrs[id%len(addrs)]
	if f.httpMode {
		lats, err := httpWorker(workerKey, addr, f.authToken, f.rounds, f.timeout, f.leaseTTL, f.warmup, warmupWg, startCh)
		return benchResult{latencies: lats, err: err}
	}
	lats, err := worker(workerKey, addr, f.authToken, f.rounds, f.timeout, f.leaseTTL, connsPerWorker, f.warmup, warmupWg, startCh)
	return benchResult{latencies: lats, err: err}
}

func mustCollectLatencies(results []benchResult) []float64 {
	var all []float64
	for i, r := range results {
		if r.err != nil {
			fmt.Fprintf(os.Stderr, "worker %d error: %v\n", i, r.err)
			os.Exit(1)
		}
		all = append(all, r.latencies...)
	}
	if len(all) == 0 {
		fmt.Fprintln(os.Stderr, "no operations completed")
		os.Exit(1)
	}
	sort.Float64s(all)
	return all
}

func printBenchStats(all []float64, wall float64) {
	totalOps := len(all)
	mean := mean(all)
	fmt.Printf("  total ops : %d\n", totalOps)
	fmt.Printf("  wall time : %.3fs\n", wall)
	fmt.Printf("  throughput: %.1f ops/s\n", float64(totalOps)/wall)
	fmt.Println()
	fmt.Printf("  mean      : %.3f ms\n", mean*1000)
	fmt.Printf("  min       : %.3f ms\n", all[0]*1000)
	fmt.Printf("  max       : %.3f ms\n", all[totalOps-1]*1000)
	fmt.Printf("  p50       : %.3f ms\n", percentile(all, 50)*1000)
	fmt.Printf("  p99       : %.3f ms\n", percentile(all, 99)*1000)
	fmt.Printf("  stdev     : %.3f ms\n", stdev(all, mean)*1000)
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

func worker(key, addr, authToken string, rounds, timeoutSec, leaseTTL, numConns, warmupRounds int, warmupWg *sync.WaitGroup, startCh <-chan struct{}) ([]float64, error) {
	conns, err := dialBenchConns(addr, authToken, numConns)
	if err != nil {
		warmupWg.Done() // unblock barrier so main doesn't hang
		return nil, err
	}
	defer closeConns(conns)

	acquireTimeout := time.Duration(timeoutSec) * time.Second
	opts := leaseTTLOpts(leaseTTL)

	if err := warmupLoop(conns, key, acquireTimeout, opts, warmupRounds); err != nil {
		warmupWg.Done()
		return nil, err
	}
	warmupWg.Done()
	<-startCh
	return measuredLoop(conns, key, acquireTimeout, opts, rounds)
}

func dialBenchConns(addr, authToken string, numConns int) ([]*client.Conn, error) {
	conns := make([]*client.Conn, 0, numConns)
	for i := 0; i < numConns; i++ {
		c, err := client.Dial(addr)
		if err != nil {
			closeConns(conns)
			return nil, fmt.Errorf("dial: %w", err)
		}
		if authToken != "" {
			if err := client.Authenticate(c, authToken); err != nil {
				c.Close()
				closeConns(conns)
				return nil, fmt.Errorf("auth: %w", err)
			}
		}
		conns = append(conns, c)
	}
	return conns, nil
}

func closeConns(conns []*client.Conn) {
	for _, c := range conns {
		c.Close()
	}
}

func leaseTTLOpts(leaseTTL int) []client.Option {
	if leaseTTL <= 0 {
		return nil
	}
	return []client.Option{client.WithLeaseTTL(leaseTTL)}
}

func warmupLoop(conns []*client.Conn, key string, acquireTimeout time.Duration, opts []client.Option, rounds int) error {
	for i := range rounds {
		if err := acquireReleaseOnce(conns[i%len(conns)], key, acquireTimeout, opts); err != nil {
			return fmt.Errorf("warmup: %w", err)
		}
	}
	return nil
}

func measuredLoop(conns []*client.Conn, key string, acquireTimeout time.Duration, opts []client.Option, rounds int) ([]float64, error) {
	latencies := make([]float64, 0, rounds)
	for i := range rounds {
		t0 := time.Now()
		if err := acquireReleaseOnce(conns[i%len(conns)], key, acquireTimeout, opts); err != nil {
			return nil, err
		}
		latencies = append(latencies, time.Since(t0).Seconds())
	}
	return latencies, nil
}

func acquireReleaseOnce(c *client.Conn, key string, acquireTimeout time.Duration, opts []client.Option) error {
	token, _, err := client.Acquire(c, key, acquireTimeout, opts...)
	if err != nil {
		return fmt.Errorf("acquire: %w", err)
	}
	if token == "" {
		return fmt.Errorf("acquire timed out")
	}
	if err := client.Release(c, key, token); err != nil {
		return fmt.Errorf("release: %w", err)
	}
	return nil
}

// validateHTTPAddrs rejects --servers entries that aren't http(s) URLs.
func validateHTTPAddrs(addrs []string) error {
	for _, a := range addrs {
		if !strings.HasPrefix(a, "http://") && !strings.HasPrefix(a, "https://") {
			return fmt.Errorf("--http requires http(s):// URLs, got %q", a)
		}
	}
	return nil
}

// httpWorker is the HTTP equivalent of worker. Each worker creates one
// session up front, reuses an http.Client (with keep-alive) for the
// acquire/release rounds, and deletes the session on exit.
func httpWorker(key, base, authToken string, rounds, timeoutSec, leaseTTL, warmupRounds int, warmupWg *sync.WaitGroup, startCh <-chan struct{}) ([]float64, error) {
	hc, tr := buildBenchHTTPClient(timeoutSec)
	authHdr := bearerHeader(authToken)

	sessionID, err := httpCreateSession(hc, base, authHdr)
	if err != nil {
		warmupWg.Done()
		return nil, fmt.Errorf("session create: %w", err)
	}
	defer func() {
		_ = httpDeleteSession(hc, base, authHdr, sessionID)
		tr.CloseIdleConnections()
	}()

	acquireBody, err := encodeAcquireBody(timeoutSec, leaseTTL)
	if err != nil {
		warmupWg.Done()
		return nil, err
	}

	if err := httpWarmupLoop(hc, base, authHdr, sessionID, key, acquireBody, warmupRounds); err != nil {
		warmupWg.Done()
		return nil, err
	}
	warmupWg.Done()
	<-startCh
	return httpMeasuredLoop(hc, base, authHdr, sessionID, key, acquireBody, rounds)
}

// buildBenchHTTPClient returns an HTTP client tuned for per-worker
// keep-alive reuse — http.DefaultTransport evicts idle conns at our
// request rate, which inflates latency.
func buildBenchHTTPClient(timeoutSec int) (*http.Client, *http.Transport) {
	tr := &http.Transport{
		MaxIdleConns:        1,
		MaxIdleConnsPerHost: 1,
		IdleConnTimeout:     90 * time.Second,
	}
	return &http.Client{Transport: tr, Timeout: time.Duration(timeoutSec+30) * time.Second}, tr
}

func bearerHeader(authToken string) string {
	if authToken == "" {
		return ""
	}
	return "Bearer " + authToken
}

func encodeAcquireBody(timeoutSec, leaseTTL int) ([]byte, error) {
	return json.Marshal(map[string]int{
		"acquire_timeout_s": timeoutSec,
		"lease_ttl_s":       leaseTTL,
	})
}

func httpAcquireReleaseOnce(hc *http.Client, base, authHdr, sessionID, key string, acquireBody []byte) error {
	token, err := httpAcquire(hc, base, authHdr, sessionID, key, acquireBody)
	if err != nil {
		return fmt.Errorf("acquire: %w", err)
	}
	if err := httpRelease(hc, base, authHdr, sessionID, key, token); err != nil {
		return fmt.Errorf("release: %w", err)
	}
	return nil
}

func httpWarmupLoop(hc *http.Client, base, authHdr, sessionID, key string, acquireBody []byte, rounds int) error {
	for i := 0; i < rounds; i++ {
		if err := httpAcquireReleaseOnce(hc, base, authHdr, sessionID, key, acquireBody); err != nil {
			return fmt.Errorf("warmup: %w", err)
		}
	}
	return nil
}

func httpMeasuredLoop(hc *http.Client, base, authHdr, sessionID, key string, acquireBody []byte, rounds int) ([]float64, error) {
	latencies := make([]float64, 0, rounds)
	for i := 0; i < rounds; i++ {
		t0 := time.Now()
		if err := httpAcquireReleaseOnce(hc, base, authHdr, sessionID, key, acquireBody); err != nil {
			return nil, err
		}
		latencies = append(latencies, time.Since(t0).Seconds())
	}
	return latencies, nil
}

// httpCreateSession POSTs /v1/sessions and returns the session ID.
func httpCreateSession(hc *http.Client, base, authHdr string) (string, error) {
	req, err := http.NewRequest("POST", base+"/v1/sessions", nil)
	if err != nil {
		return "", err
	}
	if authHdr != "" {
		req.Header.Set("Authorization", authHdr)
	}
	resp, err := hc.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("status %d: %s", resp.StatusCode, body)
	}
	var v struct {
		SessionID string `json:"session_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return "", err
	}
	return v.SessionID, nil
}

func httpDeleteSession(hc *http.Client, base, authHdr, sessionID string) error {
	req, err := http.NewRequest("DELETE", base+"/v1/sessions/"+sessionID, nil)
	if err != nil {
		return err
	}
	if authHdr != "" {
		req.Header.Set("Authorization", authHdr)
	}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return nil
}

// httpAcquire posts the acquire request and returns the granted token.
// Errors out if the server returned timeout — under unique keys this
// should never happen at any concurrency level.
func httpAcquire(hc *http.Client, base, authHdr, sessionID, key string, body []byte) (string, error) {
	req, err := http.NewRequest("POST", base+"/v1/locks/"+key, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Dflockd-Session", sessionID)
	if authHdr != "" {
		req.Header.Set("Authorization", authHdr)
	}
	resp, err := hc.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		respBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("status %d: %s", resp.StatusCode, respBody)
	}
	var v struct {
		Status string `json:"status"`
		Token  string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return "", err
	}
	if v.Status != "ok" {
		return "", fmt.Errorf("unexpected status %q", v.Status)
	}
	return v.Token, nil
}

func httpRelease(hc *http.Client, base, authHdr, sessionID, key, token string) error {
	body, err := json.Marshal(map[string]string{"token": token})
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", base+"/v1/locks/"+key+"/release", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Dflockd-Session", sessionID)
	if authHdr != "" {
		req.Header.Set("Authorization", authHdr)
	}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != 204 {
		return fmt.Errorf("release status %d", resp.StatusCode)
	}
	return nil
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
