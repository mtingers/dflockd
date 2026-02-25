// Long-running soak test for dflockd.
//
// Exercises every feature (locks, semaphores, two-phase, counters, KV, signals,
// signal queue groups, and lists) in a loop, checking for correctness after each
// round and querying stats to detect leaked state. Runs until interrupted.
//
// Usage:
//
//	go run ./cmd/soak [--server 127.0.0.1:6388] [--workers 4] [--rounds-per-cycle 20]
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/mtingers/dflockd/client"
)

func main() {
	addr := flag.String("server", "127.0.0.1:6388", "dflockd server address")
	workers := flag.Int("workers", 4, "concurrent workers per feature test")
	roundsPerCycle := flag.Int("rounds-per-cycle", 20, "operations per worker per cycle")
	flag.Parse()

	log.SetFlags(log.Ltime | log.Lmicroseconds)
	log.Printf("soak: server=%s workers=%d rounds/cycle=%d", *addr, *workers, *roundsPerCycle)
	log.Printf("soak: press Ctrl-C to stop")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	var cycle int
	for {
		select {
		case <-stop:
			log.Printf("soak: stopped after %d cycles", cycle)
			return
		default:
		}

		cycle++
		t0 := time.Now()

		// Each test uses a unique prefix to avoid collisions between cycles.
		prefix := fmt.Sprintf("soak_%d_%d", cycle, rand.IntN(999999))

		runTest("locks", func() error {
			return testLocks(*addr, prefix, *workers, *roundsPerCycle)
		})
		runTest("semaphores", func() error {
			return testSemaphores(*addr, prefix, *workers, *roundsPerCycle)
		})
		runTest("two-phase", func() error {
			return testTwoPhase(*addr, prefix, *workers, *roundsPerCycle)
		})
		runTest("counters", func() error {
			return testCounters(*addr, prefix, *workers, *roundsPerCycle)
		})
		runTest("kv", func() error {
			return testKV(*addr, prefix, *workers, *roundsPerCycle)
		})
		runTest("kv-ttl", func() error {
			return testKVTTL(*addr, prefix)
		})
		runTest("signals", func() error {
			return testSignals(*addr, prefix, *roundsPerCycle)
		})
		runTest("signal-queue-groups", func() error {
			return testSignalQueueGroups(*addr, prefix, *roundsPerCycle)
		})
		runTest("lists", func() error {
			return testLists(*addr, prefix, *workers, *roundsPerCycle)
		})

		// Stats check: after all cleanup, verify nothing leaked.
		runTest("stats-check", func() error {
			return checkStats(*addr, prefix)
		})

		log.Printf("cycle %d complete (%.1fs)", cycle, time.Since(t0).Seconds())
	}
}

func runTest(name string, fn func() error) {
	if err := fn(); err != nil {
		log.Fatalf("FAIL [%s]: %v", name, err)
	}
}

// ---------------------------------------------------------------------------
// Locks: acquire + renew + release (concurrent workers, unique keys)
// ---------------------------------------------------------------------------

func testLocks(addr, prefix string, workers, rounds int) error {
	var wg sync.WaitGroup
	errs := make([]error, workers)

	for w := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c, err := client.Dial(addr)
			if err != nil {
				errs[id] = fmt.Errorf("dial: %w", err)
				return
			}
			defer c.Close()

			key := fmt.Sprintf("%s_lock_%d", prefix, id)
			for r := range rounds {
				token, _, err := client.Acquire(c, key, 5*time.Second, client.WithLeaseTTL(10))
				if err != nil {
					errs[id] = fmt.Errorf("acquire round %d: %w", r, err)
					return
				}
				if _, err := client.Renew(c, key, token, client.WithLeaseTTL(10)); err != nil {
					errs[id] = fmt.Errorf("renew round %d: %w", r, err)
					return
				}
				if err := client.Release(c, key, token); err != nil {
					errs[id] = fmt.Errorf("release round %d: %w", r, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Semaphores: concurrent acquire (limit=3) + release
// ---------------------------------------------------------------------------

func testSemaphores(addr, prefix string, workers, rounds int) error {
	key := fmt.Sprintf("%s_sem", prefix)
	limit := 3
	var wg sync.WaitGroup
	errs := make([]error, workers)

	for w := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c, err := client.Dial(addr)
			if err != nil {
				errs[id] = fmt.Errorf("dial: %w", err)
				return
			}
			defer c.Close()

			for r := range rounds {
				token, _, err := client.SemAcquire(c, key, 10*time.Second, limit, client.WithLeaseTTL(10))
				if err != nil {
					errs[id] = fmt.Errorf("sem acquire round %d: %w", r, err)
					return
				}
				// Hold briefly to create contention.
				time.Sleep(time.Duration(rand.IntN(2)) * time.Millisecond)
				if err := client.SemRelease(c, key, token); err != nil {
					errs[id] = fmt.Errorf("sem release round %d: %w", r, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Two-phase: enqueue + wait + release
// ---------------------------------------------------------------------------

func testTwoPhase(addr, prefix string, workers, rounds int) error {
	var wg sync.WaitGroup
	errs := make([]error, workers)

	for w := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("%s_2phase_%d", prefix, id)

			for r := range rounds {
				c, err := client.Dial(addr)
				if err != nil {
					errs[id] = fmt.Errorf("dial round %d: %w", r, err)
					return
				}

				status, token, _, err := client.Enqueue(c, key, client.WithLeaseTTL(10))
				if err != nil {
					c.Close()
					errs[id] = fmt.Errorf("enqueue round %d: %w", r, err)
					return
				}

				if status == "queued" {
					token, _, err = client.Wait(c, key, 5*time.Second)
					if err != nil {
						c.Close()
						errs[id] = fmt.Errorf("wait round %d: %w", r, err)
						return
					}
				}

				if err := client.Release(c, key, token); err != nil {
					c.Close()
					errs[id] = fmt.Errorf("release round %d: %w", r, err)
					return
				}
				c.Close()
			}
		}(w)
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Counters: incr/decr/get/cset (verify net-zero)
// ---------------------------------------------------------------------------

func testCounters(addr, prefix string, workers, rounds int) error {
	var wg sync.WaitGroup
	errs := make([]error, workers)

	for w := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c, err := client.Dial(addr)
			if err != nil {
				errs[id] = fmt.Errorf("dial: %w", err)
				return
			}
			defer c.Close()

			key := fmt.Sprintf("%s_ctr_%d", prefix, id)
			for r := range rounds {
				if _, err := client.Incr(c, key, 1); err != nil {
					errs[id] = fmt.Errorf("incr round %d: %w", r, err)
					return
				}
				if _, err := client.Decr(c, key, 1); err != nil {
					errs[id] = fmt.Errorf("decr round %d: %w", r, err)
					return
				}
			}

			// Verify net-zero.
			val, err := client.GetCounter(c, key)
			if err != nil {
				errs[id] = fmt.Errorf("get counter: %w", err)
				return
			}
			if val != 0 {
				errs[id] = fmt.Errorf("counter %s = %d, want 0", key, val)
				return
			}

			// Clean up.
			if err := client.SetCounter(c, key, 0); err != nil {
				errs[id] = fmt.Errorf("cset cleanup: %w", err)
			}
		}(w)
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// KV: set + get (verify value) + del
// ---------------------------------------------------------------------------

func testKV(addr, prefix string, workers, rounds int) error {
	var wg sync.WaitGroup
	errs := make([]error, workers)

	for w := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c, err := client.Dial(addr)
			if err != nil {
				errs[id] = fmt.Errorf("dial: %w", err)
				return
			}
			defer c.Close()

			key := fmt.Sprintf("%s_kv_%d", prefix, id)
			for r := range rounds {
				val := fmt.Sprintf("value_%d_%d", id, r)
				if err := client.KVSet(c, key, val, 0); err != nil {
					errs[id] = fmt.Errorf("kset round %d: %w", r, err)
					return
				}
				got, err := client.KVGet(c, key)
				if err != nil {
					errs[id] = fmt.Errorf("kget round %d: %w", r, err)
					return
				}
				if got != val {
					errs[id] = fmt.Errorf("kget round %d: got %q, want %q", r, got, val)
					return
				}
			}
			// Clean up.
			if err := client.KVDel(c, key); err != nil {
				errs[id] = fmt.Errorf("kdel cleanup: %w", err)
			}
		}(w)
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// KV TTL: set with short TTL, verify expiry
// ---------------------------------------------------------------------------

func testKVTTL(addr, prefix string) error {
	c, err := client.Dial(addr)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer c.Close()

	key := fmt.Sprintf("%s_kvttl", prefix)
	if err := client.KVSet(c, key, "ephemeral", 1); err != nil {
		return fmt.Errorf("kset: %w", err)
	}

	// Verify it exists.
	val, err := client.KVGet(c, key)
	if err != nil {
		return fmt.Errorf("kget: %w", err)
	}
	if val != "ephemeral" {
		return fmt.Errorf("kget: got %q, want %q", val, "ephemeral")
	}

	// Wait for TTL expiry.
	time.Sleep(1500 * time.Millisecond)

	_, err = client.KVGet(c, key)
	if err == nil {
		return fmt.Errorf("expected ErrNotFound after TTL expiry")
	}
	return nil
}

// ---------------------------------------------------------------------------
// Signals: fan-out delivery + unlisten cleanup
// ---------------------------------------------------------------------------

func testSignals(addr, prefix string, rounds int) error {
	channel := fmt.Sprintf("%s_sig", prefix)

	// Set up 3 listeners.
	listeners := make([]*client.SignalConn, 3)
	for i := range listeners {
		c, err := client.Dial(addr)
		if err != nil {
			return fmt.Errorf("listener %d dial: %w", i, err)
		}
		sc := client.NewSignalConn(c)
		if err := sc.Listen(channel); err != nil {
			sc.Close()
			return fmt.Errorf("listener %d listen: %w", i, err)
		}
		listeners[i] = sc
	}

	// Emitter.
	ec, err := client.Dial(addr)
	if err != nil {
		return fmt.Errorf("emitter dial: %w", err)
	}
	defer ec.Close()

	for r := range rounds {
		n, err := client.Emit(ec, channel, fmt.Sprintf("msg_%d", r))
		if err != nil {
			return fmt.Errorf("emit round %d: %w", r, err)
		}
		if n != 3 {
			return fmt.Errorf("emit round %d: delivered to %d, want 3", r, n)
		}
	}

	// Drain and verify each listener got all messages.
	for i, sc := range listeners {
		count := 0
		timeout := time.After(2 * time.Second)
	drain:
		for {
			select {
			case _, ok := <-sc.Signals():
				if !ok {
					break drain
				}
				count++
				if count == rounds {
					break drain
				}
			case <-timeout:
				break drain
			}
		}
		if count != rounds {
			return fmt.Errorf("listener %d received %d/%d signals", i, count, rounds)
		}
	}

	// Unlisten and close all listeners.
	for i, sc := range listeners {
		if err := sc.Unlisten(channel); err != nil {
			return fmt.Errorf("listener %d unlisten: %w", i, err)
		}
		sc.Close()
	}

	// Verify no listeners remain.
	n, err := client.Emit(ec, channel, "after-cleanup")
	if err != nil {
		return fmt.Errorf("emit after cleanup: %w", err)
	}
	if n != 0 {
		return fmt.Errorf("signal after cleanup: delivered to %d, want 0", n)
	}

	return nil
}

// ---------------------------------------------------------------------------
// Signal queue groups: round-robin + mixed delivery + cleanup
// ---------------------------------------------------------------------------

func testSignalQueueGroups(addr, prefix string, rounds int) error {
	channel := fmt.Sprintf("%s_qg", prefix)

	// 2 workers in "workers" group.
	workerSCs := make([]*client.SignalConn, 2)
	for i := range workerSCs {
		c, err := client.Dial(addr)
		if err != nil {
			return fmt.Errorf("worker %d dial: %w", i, err)
		}
		sc := client.NewSignalConn(c)
		if err := sc.Listen(channel, client.WithGroup("workers")); err != nil {
			sc.Close()
			return fmt.Errorf("worker %d listen: %w", i, err)
		}
		workerSCs[i] = sc
	}

	// 1 audit logger in "audit" group.
	ac, err := client.Dial(addr)
	if err != nil {
		return fmt.Errorf("audit dial: %w", err)
	}
	auditSC := client.NewSignalConn(ac)
	if err := auditSC.Listen(channel, client.WithGroup("audit")); err != nil {
		auditSC.Close()
		return fmt.Errorf("audit listen: %w", err)
	}

	// 1 non-grouped individual listener.
	ic, err := client.Dial(addr)
	if err != nil {
		return fmt.Errorf("individual dial: %w", err)
	}
	indivSC := client.NewSignalConn(ic)
	if err := indivSC.Listen(channel); err != nil {
		indivSC.Close()
		return fmt.Errorf("individual listen: %w", err)
	}

	// Emitter.
	ec, err := client.Dial(addr)
	if err != nil {
		return fmt.Errorf("emitter dial: %w", err)
	}
	defer ec.Close()

	// Each signal should deliver to: individual + one worker + audit = 3
	for r := range rounds {
		n, err := client.Emit(ec, channel, fmt.Sprintf("job_%d", r))
		if err != nil {
			return fmt.Errorf("emit round %d: %w", r, err)
		}
		if n != 3 {
			return fmt.Errorf("emit round %d: delivered to %d, want 3", r, n)
		}
	}

	// Verify individual got all.
	indivCount := drainCount(indivSC, rounds, 2*time.Second)
	if indivCount != rounds {
		return fmt.Errorf("individual received %d/%d", indivCount, rounds)
	}

	// Verify audit got all.
	auditCount := drainCount(auditSC, rounds, 2*time.Second)
	if auditCount != rounds {
		return fmt.Errorf("audit received %d/%d", auditCount, rounds)
	}

	// Verify workers got rounds total, roughly evenly split.
	var workerCounts [2]int
	for i, sc := range workerSCs {
		workerCounts[i] = drainCount(sc, rounds, 2*time.Second)
	}
	total := workerCounts[0] + workerCounts[1]
	if total != rounds {
		return fmt.Errorf("workers received %d total, want %d (split: %d/%d)",
			total, rounds, workerCounts[0], workerCounts[1])
	}

	// Both workers should have gotten at least some signals (round-robin).
	if workerCounts[0] == 0 || workerCounts[1] == 0 {
		return fmt.Errorf("round-robin imbalance: %d/%d (one got nothing)",
			workerCounts[0], workerCounts[1])
	}

	// Clean up.
	for _, sc := range workerSCs {
		sc.Unlisten(channel, client.WithGroup("workers"))
		sc.Close()
	}
	auditSC.Unlisten(channel, client.WithGroup("audit"))
	auditSC.Close()
	indivSC.Unlisten(channel)
	indivSC.Close()

	// Verify cleanup.
	n, err := client.Emit(ec, channel, "after-cleanup")
	if err != nil {
		return fmt.Errorf("emit after cleanup: %w", err)
	}
	if n != 0 {
		return fmt.Errorf("signal after cleanup: delivered to %d, want 0", n)
	}

	return nil
}

func drainCount(sc *client.SignalConn, max int, timeout time.Duration) int {
	count := 0
	deadline := time.After(timeout)
	for {
		select {
		case _, ok := <-sc.Signals():
			if !ok {
				return count
			}
			count++
			if count >= max {
				return count
			}
		case <-deadline:
			return count
		}
	}
}

// ---------------------------------------------------------------------------
// Lists: rpush + llen + lrange + lpop (verify FIFO order and cleanup)
// ---------------------------------------------------------------------------

func testLists(addr, prefix string, workers, rounds int) error {
	var wg sync.WaitGroup
	errs := make([]error, workers)

	for w := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c, err := client.Dial(addr)
			if err != nil {
				errs[id] = fmt.Errorf("dial: %w", err)
				return
			}
			defer c.Close()

			key := fmt.Sprintf("%s_list_%d", prefix, id)

			// Push items.
			for r := range rounds {
				val := fmt.Sprintf("item_%d", r)
				n, err := client.RPush(c, key, val)
				if err != nil {
					errs[id] = fmt.Errorf("rpush round %d: %w", r, err)
					return
				}
				if n != r+1 {
					errs[id] = fmt.Errorf("rpush round %d: length %d, want %d", r, n, r+1)
					return
				}
			}

			// Verify length.
			length, err := client.LLen(c, key)
			if err != nil {
				errs[id] = fmt.Errorf("llen: %w", err)
				return
			}
			if length != rounds {
				errs[id] = fmt.Errorf("llen: got %d, want %d", length, rounds)
				return
			}

			// Verify range.
			items, err := client.LRange(c, key, 0, -1)
			if err != nil {
				errs[id] = fmt.Errorf("lrange: %w", err)
				return
			}
			if len(items) != rounds {
				errs[id] = fmt.Errorf("lrange: got %d items, want %d", len(items), rounds)
				return
			}

			// Pop all items (FIFO order).
			for r := range rounds {
				val, err := client.LPop(c, key)
				if err != nil {
					errs[id] = fmt.Errorf("lpop round %d: %w", r, err)
					return
				}
				want := fmt.Sprintf("item_%d", r)
				if val != want {
					errs[id] = fmt.Errorf("lpop round %d: got %q, want %q", r, val, want)
					return
				}
			}

			// Verify empty.
			length, err = client.LLen(c, key)
			if err != nil {
				errs[id] = fmt.Errorf("llen after pop: %w", err)
				return
			}
			if length != 0 {
				errs[id] = fmt.Errorf("llen after pop: got %d, want 0", length)
				return
			}
		}(w)
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Stats check: verify no leaked signal listeners or other state from our prefix
// ---------------------------------------------------------------------------

// statsResponse mirrors the JSON returned by the stats command.
type statsResponse struct {
	Connections    int64 `json:"connections"`
	Locks          []struct {
		Key string `json:"key"`
	} `json:"locks"`
	Semaphores []struct {
		Key string `json:"key"`
	} `json:"semaphores"`
	IdleLocks []struct {
		Key string `json:"key"`
	} `json:"idle_locks"`
	IdleSemaphores []struct {
		Key string `json:"key"`
	} `json:"idle_semaphores"`
	SignalChannels []struct {
		Pattern   string `json:"pattern"`
		Group     string `json:"group,omitempty"`
		Listeners int    `json:"listeners"`
	} `json:"signal_channels"`
	Counters []struct {
		Key   string `json:"key"`
		Value int64  `json:"value"`
	} `json:"counters"`
	KVEntries []struct {
		Key string `json:"key"`
	} `json:"kv_entries"`
	Lists []struct {
		Key string `json:"key"`
		Len int    `json:"len"`
	} `json:"lists"`
}

func checkStats(addr, prefix string) error {
	// Use raw TCP for stats since the client package doesn't expose a Stats helper.
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := fmt.Fprintf(conn, "stats\n_\n\n"); err != nil {
		return fmt.Errorf("stats write: %w", err)
	}

	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("stats read: %w", err)
		}
		return fmt.Errorf("stats: empty response")
	}
	resp := scanner.Text()

	if !strings.HasPrefix(resp, "ok ") {
		return fmt.Errorf("stats response: %q", resp)
	}
	jsonStr := resp[3:]

	var stats statsResponse
	if err := json.Unmarshal([]byte(jsonStr), &stats); err != nil {
		return fmt.Errorf("stats JSON: %w", err)
	}

	// Check for leaked signal channels from this cycle.
	for _, ch := range stats.SignalChannels {
		if strings.HasPrefix(ch.Pattern, prefix) {
			return fmt.Errorf("leaked signal channel: pattern=%q group=%q listeners=%d",
				ch.Pattern, ch.Group, ch.Listeners)
		}
	}

	// Check for leaked locks.
	for _, l := range stats.Locks {
		if strings.HasPrefix(l.Key, prefix) {
			return fmt.Errorf("leaked lock: key=%q", l.Key)
		}
	}

	// Check for leaked semaphores.
	for _, s := range stats.Semaphores {
		if strings.HasPrefix(s.Key, prefix) {
			return fmt.Errorf("leaked semaphore: key=%q", s.Key)
		}
	}

	// Check for leaked lists.
	for _, l := range stats.Lists {
		if strings.HasPrefix(l.Key, prefix) && l.Len > 0 {
			return fmt.Errorf("leaked list: key=%q len=%d", l.Key, l.Len)
		}
	}

	return nil
}
