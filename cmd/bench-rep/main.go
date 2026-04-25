// Replication soak harness: spins up a primary + secondary pair of
// in-process dflockd Servers connected via the replication link,
// hammers the primary with concurrent acquire/release mutations, then
// asserts the secondary's lock manager state is the empty
// post-release set (i.e. every mutation that happened on the primary
// converged on the secondary).
//
// This is not a microbench — it's a correctness-under-load harness.
// Use cmd/bench against a standalone server for raw throughput
// numbers; this tool is here to surface replication-specific bugs
// (drift, ack stalls, snapshot races, etc.) under sustained traffic.
//
// Usage:
//
//	go run ./cmd/bench-rep [--workers 50] [--rounds 1000] [--duration 0s]
//	    [--keys 100] [--sustained-failover false]
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mtingers/dflockd/client"
	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/replication"
	"github.com/mtingers/dflockd/internal/server"
)

func main() {
	workers := flag.Int("workers", 50, "concurrent worker goroutines")
	rounds := flag.Int("rounds", 1000, "acquire/release rounds per worker")
	duration := flag.Duration("duration", 0, "if >0, run for this long instead of fixed rounds")
	keys := flag.Int("keys", 100, "number of distinct lock keys to round-robin across")
	verbose := flag.Bool("verbose", false, "log every state change instead of just warnings")
	flag.Parse()

	if *workers <= 0 || *rounds <= 0 || *keys <= 0 {
		fmt.Fprintln(os.Stderr, "bench-rep: --workers, --rounds, --keys all must be > 0")
		os.Exit(2)
	}

	logLevel := slog.LevelWarn
	if *verbose {
		logLevel = slog.LevelInfo
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	priAddr, _, priLM, secLM, shutdown, err := startReplicaPair(ctx, log)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench-rep: setup failed: %v\n", err)
		os.Exit(1)
	}
	defer shutdown()

	// Spawn workers.
	var wg sync.WaitGroup
	var totalOps atomic.Uint64
	var failures atomic.Uint64
	stop := make(chan struct{})
	wallStart := time.Now()

	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, err := client.Dial(priAddr)
			if err != nil {
				failures.Add(1)
				return
			}
			defer conn.Close()
			for round := 0; round < *rounds; round++ {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprintf("k%d", (id*1000+round)%*keys)
				tok, _, err := client.Acquire(conn, key, 5*time.Second, client.WithLeaseTTL(30))
				if err != nil || tok == "" {
					failures.Add(1)
					continue
				}
				if err := client.Release(conn, key, tok); err != nil {
					failures.Add(1)
					continue
				}
				totalOps.Add(1)
			}
		}(w)
	}

	// Optional duration cap.
	if *duration > 0 {
		go func() {
			t := time.NewTimer(*duration)
			defer t.Stop()
			<-t.C
			close(stop)
		}()
	}

	wg.Wait()
	wall := time.Since(wallStart)

	totalOpsCount := totalOps.Load()
	failuresCount := failures.Load()
	throughput := float64(totalOpsCount) / wall.Seconds()

	fmt.Printf("\nbench-rep results:\n")
	fmt.Printf("  duration       : %s\n", wall.Truncate(time.Millisecond))
	fmt.Printf("  total ops      : %d\n", totalOpsCount)
	fmt.Printf("  failures       : %d\n", failuresCount)
	fmt.Printf("  throughput     : %.0f ops/s\n", throughput)

	// Convergence check: after all clients release, both lock managers
	// should agree on "no holders." The replicated holder count should
	// have converged. Allow a brief settle for any in-flight acks.
	time.Sleep(200 * time.Millisecond)
	priHolders := countHolders(priLM)
	secHolders := countHolders(secLM)
	fmt.Printf("\nconvergence check (post-settle):\n")
	fmt.Printf("  primary holders   : %d\n", priHolders)
	fmt.Printf("  secondary holders : %d\n", secHolders)
	if priHolders != secHolders {
		fmt.Printf("\nFAIL: holders diverge between primary and secondary\n")
		os.Exit(1)
	}
	if priHolders != 0 {
		fmt.Printf("\nFAIL: primary still has holders after release-on-success workload\n")
		os.Exit(1)
	}
	if failuresCount > 0 && failuresCount > totalOpsCount/100 {
		fmt.Printf("\nWARN: failure rate %.2f%% > 1%%\n",
			100*float64(failuresCount)/float64(totalOpsCount+failuresCount))
	}
	fmt.Println("\nPASS: primary and secondary converged at zero holders")
}

// startReplicaPair brings up an in-process primary + secondary pair
// connected by the replication link. Returns both client addresses,
// both lock managers, and a shutdown closure. Errors propagate to
// the caller. The shutdown closure cancels the supplied parent ctx
// before tearing the rest down so the servers' accept loops exit
// cleanly instead of spinning on closed-listener errors.
func startReplicaPair(parentCtx context.Context, log *slog.Logger) (priAddr, secAddr string, priLM, secLM *lock.LockManager, shutdown func(), err error) {
	ctx, cancel := context.WithCancel(parentCtx)
	priCfg := defaultCfg()
	secCfg := defaultCfg()

	priLM = lock.NewLockManager(priCfg, log)
	secLM = lock.NewLockManager(secCfg, log)

	priClientLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		cancel()
		return "", "", nil, nil, nil, fmt.Errorf("primary listen: %w", err)
	}
	priAddr = priClientLn.Addr().String()
	secClientLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		cancel()
		priClientLn.Close()
		return "", "", nil, nil, nil, fmt.Errorf("secondary listen: %w", err)
	}
	secAddr = secClientLn.Addr().String()

	// Pick a free address for the replication peer link.
	rl, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		cancel()
		priClientLn.Close()
		secClientLn.Close()
		return "", "", nil, nil, nil, fmt.Errorf("replication addr pick: %w", err)
	}
	repPeerAddr := rl.Addr().String()
	rl.Close()

	priSrv := server.New(priLM, priCfg, log)
	secSrv := server.New(secLM, secCfg, log)

	priRep := replication.NewReplicator(replication.Config{
		Role:        replication.RolePrimary,
		NodeID:      "pri",
		PeerAddr:    repPeerAddr,
		Apply:       priLM,
		Snapshotter: snapshotAdapter{priLM},
		Log:         log.With("node", "pri"),
	})
	priLM.SetReplicationHook(priRep)
	priSrv.SetReplicator(priRep)

	secRep := replication.NewReplicator(replication.Config{
		Role:        replication.RoleSecondary,
		NodeID:      "sec",
		ListenAddr:  repPeerAddr,
		Apply:       secLM,
		Snapshotter: snapshotAdapter{secLM},
		Log:         log.With("node", "sec"),
	})
	secSrv.SetReplicator(secRep)

	if err := secRep.Start(ctx); err != nil {
		cancel()
		priClientLn.Close()
		secClientLn.Close()
		return "", "", nil, nil, nil, fmt.Errorf("sec rep start: %w", err)
	}
	if err := priRep.Start(ctx); err != nil {
		cancel()
		secRep.Stop()
		priClientLn.Close()
		secClientLn.Close()
		return "", "", nil, nil, nil, fmt.Errorf("pri rep start: %w", err)
	}

	priDone := make(chan struct{})
	secDone := make(chan struct{})
	go func() { defer close(priDone); _ = priSrv.RunOnListener(ctx, priClientLn) }()
	go func() { defer close(secDone); _ = secSrv.RunOnListener(ctx, secClientLn) }()

	// Wait for primary to reach Active (handshake + initial snapshot done).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && priRep.State() != replication.StateActive {
		time.Sleep(20 * time.Millisecond)
	}
	if priRep.State() != replication.StateActive {
		cancel()
		secRep.Stop()
		priRep.Stop()
		priClientLn.Close()
		secClientLn.Close()
		return "", "", nil, nil, nil, fmt.Errorf("primary did not reach Active: %s", priRep.State())
	}

	shutdown = func() {
		// Cancel ctx FIRST so server accept loops exit cleanly
		// instead of spinning on closed-listener errors.
		cancel()
		priRep.Stop()
		secRep.Stop()
		priClientLn.Close()
		secClientLn.Close()
		<-priDone
		<-secDone
	}
	return priAddr, secAddr, priLM, secLM, shutdown, nil
}

func defaultCfg() *config.Config {
	return &config.Config{
		Host:                    "127.0.0.1",
		Port:                    0,
		DefaultLeaseTTL:         33 * time.Second,
		LeaseSweepInterval:      time.Hour, // suppress to keep replication noise focused
		GCInterval:              time.Hour,
		GCMaxIdleTime:           time.Hour,
		MaxLocks:                10000,
		ReadTimeout:             30 * time.Second,
		WriteTimeout:            5 * time.Second,
		AutoReleaseOnDisconnect: true,
	}
}

// countHolders sums the number of held tokens across all keys in the
// lock manager. Uses the public Stats() snapshot (lock-internal).
func countHolders(lm *lock.LockManager) int {
	stats := lm.Stats(0)
	n := 0
	for _, l := range stats.Locks {
		// LockInfo carries OwnerConnID for held locks; waiter-only entries set it to 0.
		if l.OwnerConnID != 0 || l.LeaseExpiresInS > 0 {
			n++
		}
	}
	for _, s := range stats.Semaphores {
		n += s.Holders
	}
	return n
}

// snapshotAdapter bridges *lock.LockManager.Snapshot() (returns
// []lock.SnapshotEntry) to replication.Snapshotter (expects
// []replication.SnapshotEntry). Same pattern used in cmd/dflockd.
type snapshotAdapter struct{ lm *lock.LockManager }

func (s snapshotAdapter) Snapshot() []replication.SnapshotEntry {
	in := s.lm.Snapshot()
	out := make([]replication.SnapshotEntry, len(in))
	for i, e := range in {
		out[i] = replication.SnapshotEntry{Key: e.Key, Limit: e.Limit}
		if len(e.Holders) > 0 {
			out[i].Holders = make([]replication.SnapshotHolder, len(e.Holders))
			for j, h := range e.Holders {
				out[i].Holders[j] = replication.SnapshotHolder{
					Token: h.Token, ConnID: h.ConnID, LeaseExpiresUnixNS: h.LeaseExpiresUnixNS,
				}
			}
		}
		if len(e.Enqueued) > 0 {
			out[i].Enqueued = make([]replication.SnapshotEnqueued, len(e.Enqueued))
			for j, q := range e.Enqueued {
				out[i].Enqueued[j] = replication.SnapshotEnqueued{
					ConnID: q.ConnID, Token: q.Token, LeaseTTLNS: q.LeaseTTLNS,
				}
			}
		}
	}
	return out
}

// silence unused-import warning if any helper above gets cut.
var _ = strings.TrimSpace
