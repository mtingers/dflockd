// Package main implements cluster-soak. Its default mode runs an
// in-process N-node Raft cluster with periodic leader termination. Its
// external mode drives real cluster members and delegates partitions,
// restarts, and process-local clock offsets to an operator-supplied hook.
// Both modes assert fence monotonicity and token uniqueness.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/mtingers/dflockd/client"
	"github.com/mtingers/dflockd/internal/cluster"
	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/raft"
)

// soakOpts are the parsed CLI flags. Defaults match the most useful
// "smoke before release" invocation: a 3-node cluster with 4 writers
// for 30 s, killing the leader every 5 s.
type soakOpts struct {
	Nodes        int
	Workers      int
	Duration     time.Duration
	KillInterval time.Duration
	Seed         int64

	Targets        string
	AuthTokenFile  string
	FaultHook      string
	FaultInterval  time.Duration
	FaultHold      time.Duration
	ClockSkew      time.Duration
	LeaseTTL       time.Duration
	RedirectBudget int
	Keys           int
	HistoryLimit   int
}

const maxExternalClockSkew = 24 * time.Hour

// soakReport is the harness's output. Violations is the list of
// invariant failures; empty means a clean run.
type soakReport struct {
	Writes     int
	Successes  int
	NotLeader  int
	Killed     int
	Failures   int
	Partitions int
	Restarts   int
	Skews      int
	HistoryOps int
	Violations []string
	Duration   time.Duration
}

// runSoak runs the cluster-soak workload to completion and returns a
// report. It does not call os.Exit — callers (main / tests) decide.
func runSoak(ctx context.Context, opts soakOpts, _ io.Writer, log *slog.Logger) (soakReport, error) {
	if err := validateSoakOpts(opts); err != nil {
		return soakReport{}, err
	}
	if opts.Targets != "" {
		return runExternalSoak(ctx, opts, log)
	}
	h, err := newSoakHarness(opts, log)
	if err != nil {
		return soakReport{}, err
	}
	defer h.shutdown()
	if !h.waitForLeader(ctx, 2*time.Second) {
		return soakReport{}, errors.New("soak: no leader after 2s — cluster failed to bootstrap")
	}
	start := time.Now()
	deadline := start.Add(opts.Duration)
	runCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < opts.Workers; i++ {
		wg.Add(1)
		go func(id int) { defer wg.Done(); h.workerLoop(runCtx, id) }(i)
	}
	if opts.KillInterval > 0 {
		wg.Add(1)
		go func() { defer wg.Done(); h.killLoop(runCtx, opts.KillInterval) }()
	}
	wg.Wait()
	rep := h.report()
	rep.Duration = time.Since(start)
	return rep, nil
}

// soakHarness owns the in-process cluster + the violation log.
type soakHarness struct {
	opts soakOpts
	log  *slog.Logger

	mu       sync.Mutex
	net      *raft.MemNetwork
	nodes    map[raft.NodeID]*cluster.Node
	lms      map[raft.NodeID]*lock.LockManager
	trs      map[raft.NodeID]*raft.MemTransport
	dead     map[raft.NodeID]bool
	ids      []raft.NodeID
	tokens   map[string]bool
	maxByKey map[string]uint64

	writes    atomic.Int64
	successes atomic.Int64
	notLeader atomic.Int64
	killed    atomic.Int64

	viol   []string
	violMu sync.Mutex
}

func newSoakHarness(o soakOpts, log *slog.Logger) (*soakHarness, error) {
	if o.Nodes < 1 || o.Workers < 1 {
		return nil, fmt.Errorf("soak: nodes=%d workers=%d both must be >=1", o.Nodes, o.Workers)
	}
	h := &soakHarness{
		opts:     o,
		log:      log,
		net:      raft.NewMemNetwork(),
		nodes:    map[raft.NodeID]*cluster.Node{},
		lms:      map[raft.NodeID]*lock.LockManager{},
		trs:      map[raft.NodeID]*raft.MemTransport{},
		dead:     map[raft.NodeID]bool{},
		tokens:   map[string]bool{},
		maxByKey: map[string]uint64{},
	}
	for i := 0; i < o.Nodes; i++ {
		h.ids = append(h.ids, raft.NodeID(fmt.Sprintf("n%d", i+1)))
	}
	members := h.buildMembers()
	for _, id := range h.ids {
		if err := h.startNode(id, members); err != nil {
			return nil, err
		}
	}
	return h, nil
}

func (h *soakHarness) buildMembers() map[raft.NodeID]cluster.Member {
	m := map[raft.NodeID]cluster.Member{}
	for _, id := range h.ids {
		m[id] = cluster.Member{
			RaftAddr:   "raft-" + string(id),
			ClientAddr: "client-" + string(id),
		}
	}
	return m
}

func (h *soakHarness) startNode(id raft.NodeID, members map[raft.NodeID]cluster.Member) error {
	lm, err := lock.NewLockManager(&config.Config{MaxLocks: 1024, DefaultLeaseTTL: 5 * time.Second, GCMaxIdleTime: 60 * time.Second}, h.log)
	if err != nil {
		return fmt.Errorf("soak: NewLockManager(%s): %w", id, err)
	}
	tr := h.net.Transport(id)
	st := raft.NewMemStorage()
	rcfg := raft.DefaultConfig()
	rcfg.ID = id
	rcfg.HeartbeatInterval = 8 * time.Millisecond
	rcfg.ElectionTimeoutMin = 50 * time.Millisecond
	rcfg.ElectionTimeoutMax = 100 * time.Millisecond
	ccfg := cluster.Config{Raft: rcfg, Members: members, AdvertiseAddr: members[id].ClientAddr}
	n, err := cluster.NewNode(ccfg, lm, st, tr, h.log)
	if err != nil {
		_ = lm.Close()
		return fmt.Errorf("soak: NewNode(%s): %w", id, err)
	}
	n.Start()
	h.mu.Lock()
	h.nodes[id], h.lms[id], h.trs[id] = n, lm, tr
	h.mu.Unlock()
	return nil
}

// leader returns the current leader's node and id, or (nil, "", false)
// if no node believes itself leader yet.
func (h *soakHarness) leader() (*cluster.Node, raft.NodeID, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, id := range h.ids {
		if h.dead[id] {
			continue
		}
		n := h.nodes[id]
		if n != nil && n.IsLeader() {
			return n, id, true
		}
	}
	return nil, "", false
}

func (h *soakHarness) waitForLeader(ctx context.Context, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return false
		}
		if _, _, ok := h.leader(); ok {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// workerLoop runs one writer's acquire/release loop until ctx is done.
// Each worker carries its own *rand.Rand (rand.Rand is NOT safe for
// concurrent use); the per-worker seed is the harness seed mixed with
// the worker id so runs remain reproducible.
func (h *soakHarness) workerLoop(ctx context.Context, id int) {
	key := fmt.Sprintf("soak:%d", id)
	connID := uint64(id) + 1
	rng := rand.New(rand.NewSource(h.opts.Seed + int64(id) + 1))
	for {
		if ctx.Err() != nil {
			return
		}
		h.writes.Add(1)
		ok := h.attemptAcquireRelease(ctx, key, connID, rng)
		if !ok {
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// attemptAcquireRelease does one Acquire+Release against the current
// leader. Returns true on a fully-successful cycle, false otherwise.
func (h *soakHarness) attemptAcquireRelease(ctx context.Context, key string, connID uint64, rng *rand.Rand) bool {
	node, _, ok := h.leader()
	if !ok {
		h.notLeader.Add(1)
		return false
	}
	var salt [8]byte
	_, _ = rng.Read(salt[:])
	opCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	res, err := node.ProposeAcquire(opCtx, key, 1, fmt.Sprintf("w%d", connID), connID, 2*time.Second, salt)
	if err != nil || res.Status != lock.StatusOK {
		return false
	}
	if !h.recordGrant(key, res.Token) {
		return false
	}
	h.successes.Add(1)
	relCtx, cancel2 := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel2()
	_, _ = node.ProposeRelease(relCtx, key, res.Token)
	return true
}

// recordGrant checks the invariants and returns false (recording a
// violation) on a breach. Invariants:
//   - No duplicate token across all workers and all time (Raft + FSM
//     determinism guarantee unique fences).
//   - Per-key fence values are monotonic. Because each key is a mutex
//     held by one worker at a time, that worker observes its own
//     successive Acquire fences in mint order, so a regression here
//     would mean either FSM divergence or a duplicate grant on a
//     still-held key.
func (h *soakHarness) recordGrant(key, token string) bool {
	fence, err := client.FenceFromToken(token)
	if err != nil {
		h.addViolation(fmt.Sprintf("bad token format: %q (%v)", token, err))
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.tokens[token] {
		h.addViolationLocked("token reused: " + token)
		return false
	}
	if prev, ok := h.maxByKey[key]; ok && fence <= prev {
		h.addViolationLocked(fmt.Sprintf("per-key fence regression on %s: got %d, prior %d", key, fence, prev))
		return false
	}
	h.tokens[token] = true
	h.maxByKey[key] = fence
	return true
}

// addViolationLocked appends a violation; caller holds h.mu (the
// violation log uses a separate mutex so it can be hit from paths
// where h.mu is not held).
func (h *soakHarness) addViolationLocked(s string) {
	h.violMu.Lock()
	defer h.violMu.Unlock()
	h.viol = append(h.viol, s)
}

func (h *soakHarness) addViolation(s string) {
	h.violMu.Lock()
	defer h.violMu.Unlock()
	h.viol = append(h.viol, s)
}

// killLoop periodically kills the current leader. It does not bring
// the killed node back — the soak measures behavior under shrinking
// (then recovering via re-election) quorum, not under restart.
func (h *soakHarness) killLoop(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			h.killLeaderOnce()
		}
	}
}

func (h *soakHarness) killLeaderOnce() {
	_, id, ok := h.leader()
	if !ok {
		return
	}
	if !h.aliveQuorumAfterKill() {
		return // refuse to drop below quorum
	}
	h.mu.Lock()
	if h.dead[id] || h.nodes[id] == nil {
		h.mu.Unlock()
		return
	}
	n := h.nodes[id]
	tr := h.trs[id]
	lm := h.lms[id]
	h.dead[id] = true
	h.mu.Unlock()
	_ = n.Close()
	_ = tr.Close()
	_ = lm.Close()
	h.killed.Add(1)
	h.log.Info("soak: killed leader", "id", id)
}

func (h *soakHarness) aliveQuorumAfterKill() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	alive := 0
	for _, id := range h.ids {
		if !h.dead[id] {
			alive++
		}
	}
	majority := len(h.ids)/2 + 1
	return alive-1 >= majority
}

func (h *soakHarness) shutdown() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, id := range h.ids {
		if h.dead[id] {
			continue
		}
		if n := h.nodes[id]; n != nil {
			_ = n.Close()
		}
		if tr := h.trs[id]; tr != nil {
			_ = tr.Close()
		}
		if lm := h.lms[id]; lm != nil {
			_ = lm.Close()
		}
		h.dead[id] = true
	}
}

func (h *soakHarness) report() soakReport {
	h.violMu.Lock()
	defer h.violMu.Unlock()
	out := soakReport{
		Writes:    int(h.writes.Load()),
		Successes: int(h.successes.Load()),
		NotLeader: int(h.notLeader.Load()),
		Killed:    int(h.killed.Load()),
	}
	if len(h.viol) > 0 {
		out.Violations = append(out.Violations, h.viol...)
	}
	return out
}

func parseSoakFlags(args []string) (soakOpts, error) {
	fs := flag.NewFlagSet("cluster-soak", flag.ContinueOnError)
	o := soakOpts{
		Nodes: 3, Workers: 4, Duration: 30 * time.Second, KillInterval: 5 * time.Second, Seed: 1,
		FaultInterval: 2 * time.Minute, FaultHold: 30 * time.Second,
		ClockSkew: 2 * time.Second, LeaseTTL: 10 * time.Second, RedirectBudget: 6,
		Keys: 1, HistoryLimit: maxHistoryLimit,
	}
	fs.IntVar(&o.Nodes, "nodes", o.Nodes, "Number of raft nodes")
	fs.IntVar(&o.Workers, "workers", o.Workers, "Number of writer goroutines")
	fs.DurationVar(&o.Duration, "duration", o.Duration, "Total soak run time")
	fs.DurationVar(&o.KillInterval, "kill-interval", o.KillInterval, "How often to kill the leader (0 disables)")
	fs.Int64Var(&o.Seed, "seed", o.Seed, "Random seed for workload generation")
	fs.StringVar(&o.Targets, "targets", "", "Real cluster members as id=clientHost:port,... (enables external mode)")
	fs.StringVar(&o.AuthTokenFile, "auth-token-file", "", "Client auth token file for external mode")
	fs.StringVar(&o.FaultHook, "fault-hook", "", "Executable implementing partition/heal/restart/skew/unskew")
	fs.DurationVar(&o.FaultInterval, "fault-interval", o.FaultInterval, "Delay between external fault phases (0 disables)")
	fs.DurationVar(&o.FaultHold, "fault-hold", o.FaultHold, "How long to hold each Raft partition")
	fs.DurationVar(&o.ClockSkew, "clock-skew", o.ClockSkew, "Absolute external node clock offset")
	fs.DurationVar(&o.LeaseTTL, "lease-ttl", o.LeaseTTL, "Lease TTL used by external workers")
	fs.IntVar(&o.RedirectBudget, "redirect-budget", o.RedirectBudget, "External client attempts per operation")
	fs.IntVar(&o.Keys, "keys", o.Keys, "Contended keys shared by external workers")
	fs.IntVar(&o.HistoryLimit, "history-limit", o.HistoryLimit, "Initial recorded invocations per external key")
	if err := fs.Parse(args); err != nil {
		return soakOpts{}, err
	}
	if err := validateSoakOpts(o); err != nil {
		return soakOpts{}, err
	}
	return o, nil
}

func validateSoakOpts(o soakOpts) error {
	if o.Duration <= 0 || o.Workers < 1 {
		return fmt.Errorf("soak: duration must be >0 and workers must be >=1")
	}
	if o.Targets == "" {
		return validateInProcessSoakOpts(o)
	}
	return validateExternalSoakOpts(o)
}

func validateInProcessSoakOpts(o soakOpts) error {
	if o.Nodes < 1 || o.KillInterval < 0 {
		return fmt.Errorf("soak: nodes must be >=1 and kill-interval must be >=0")
	}
	return nil
}

func validateExternalSoakOpts(o soakOpts) error {
	if _, err := parseExternalMembers(o.Targets); err != nil {
		return err
	}
	if !externalRangesValid(o) {
		return fmt.Errorf("soak: external durations/budget are out of range")
	}
	if o.LeaseTTL%time.Second != 0 {
		return fmt.Errorf("soak: lease-ttl must be a whole number of seconds")
	}
	if err := validateExternalHistoryOpts(o); err != nil {
		return err
	}
	if o.FaultInterval > 0 && o.FaultHook == "" {
		return fmt.Errorf("soak: --fault-hook is required when --fault-interval is enabled")
	}
	return nil
}

func validateExternalHistoryOpts(o soakOpts) error {
	if o.Keys < 1 || o.Keys > o.Workers {
		return fmt.Errorf("soak: keys must be between 1 and workers")
	}
	if o.HistoryLimit < 1 || o.HistoryLimit > maxHistoryLimit {
		return fmt.Errorf("soak: history-limit must be between 1 and %d", maxHistoryLimit)
	}
	if o.LeaseTTL <= 2*externalHistoryClockSkew(o) {
		return fmt.Errorf("soak: lease-ttl must exceed twice clock-skew for history checking")
	}
	return nil
}

func externalHistoryClockSkew(o soakOpts) time.Duration {
	if o.FaultInterval == 0 {
		return 0
	}
	return o.ClockSkew
}

func externalRangesValid(o soakOpts) bool {
	return o.FaultInterval >= 0 && o.FaultHold >= 0 &&
		o.ClockSkew >= 0 && o.ClockSkew <= maxExternalClockSkew &&
		o.LeaseTTL >= time.Second && o.RedirectBudget >= 1
}

func main() {
	opts, err := parseSoakFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(signalCtx, opts.Duration+opts.FaultHold+30*time.Second)
	defer cancel()
	report, err := runSoak(ctx, opts, os.Stdout, log)
	if err != nil {
		fmt.Fprintln(os.Stderr, "soak: error:", err)
		os.Exit(1)
	}
	if len(report.Violations) > 0 {
		fmt.Fprintf(os.Stderr, "soak: %d violations:\n", len(report.Violations))
		for _, v := range report.Violations {
			fmt.Fprintln(os.Stderr, "  -", v)
		}
		os.Exit(1)
	}
	fmt.Printf("soak: clean run: writes=%d successes=%d failures=%d not_leader=%d killed=%d partitions=%d restarts=%d skews=%d history_ops=%d duration=%s\n",
		report.Writes, report.Successes, report.Failures, report.NotLeader, report.Killed,
		report.Partitions, report.Restarts, report.Skews, report.HistoryOps, report.Duration)
}
