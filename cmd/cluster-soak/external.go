package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mtingers/dflockd/client"
)

const externalOpTimeout = 5 * time.Second

type externalMember struct {
	ID   string
	Addr string
}

type externalHarness struct {
	opts    soakOpts
	log     logger
	members []externalMember
	addrs   []string
	auth    string
	hook    faultController
	probe   leaderProbe
	runID   string
	cancel  context.CancelFunc
	ledger  grantLedger

	writes     atomic.Int64
	successes  atomic.Int64
	failures   atomic.Int64
	notLeader  atomic.Int64
	partitions atomic.Int64
	restarts   atomic.Int64
	skews      atomic.Int64

	partitioned string
	skewed      string
}

// logger is the subset of slog.Logger used by the external harness.
type logger interface {
	Info(msg string, args ...any)
}

type leaderProbe func(context.Context, []externalMember, string) (string, error)

type faultController interface {
	Run(context.Context, string, string, string) error
}

type execFaultController struct {
	path string
}

func (c execFaultController) Run(ctx context.Context, action, node, arg string) error {
	args := []string{action, node}
	if arg != "" {
		args = append(args, arg)
	}
	out, err := exec.CommandContext(ctx, c.path, args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("fault hook %s %s: %w: %s", action, node, err, strings.TrimSpace(string(out)))
	}
	return nil
}

func runExternalSoak(ctx context.Context, opts soakOpts, log logger) (soakReport, error) {
	h, err := newExternalHarness(opts, log, nil)
	if err != nil {
		return soakReport{}, err
	}
	if _, err := h.probe(ctx, h.members, h.auth); err != nil {
		return soakReport{}, fmt.Errorf("soak: initial writable leader probe: %w", err)
	}
	start := time.Now()
	runCtx, cancel := context.WithTimeout(ctx, opts.Duration)
	h.cancel = cancel
	defer cancel()

	var wg sync.WaitGroup
	for worker := 0; worker < opts.Workers; worker++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			h.workerLoop(runCtx, id)
		}(worker)
	}
	if h.hook != nil && opts.FaultInterval > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.faultLoop(runCtx)
		}()
	}
	wg.Wait()
	h.verifyFinalState()
	report := h.report()
	report.Duration = time.Since(start)
	return report, nil
}

func newExternalHarness(opts soakOpts, log logger, hook faultController) (*externalHarness, error) {
	members, err := parseExternalMembers(opts.Targets)
	if err != nil {
		return nil, err
	}
	auth, err := readOptionalSecret(opts.AuthTokenFile)
	if err != nil {
		return nil, err
	}
	if hook == nil && opts.FaultHook != "" {
		path, err := exec.LookPath(opts.FaultHook)
		if err != nil {
			return nil, fmt.Errorf("soak: fault hook: %w", err)
		}
		hook = execFaultController{path: path}
	}
	addrs := make([]string, len(members))
	for i := range members {
		addrs[i] = members[i].Addr
	}
	return &externalHarness{
		opts: opts, log: log, members: members, addrs: addrs, auth: auth,
		hook: hook, probe: probeWritableLeader, runID: strconv.FormatInt(time.Now().UnixNano(), 36),
		ledger: grantLedger{tokens: map[string]grantRecord{}, maxByKey: map[string]uint64{}},
	}, nil
}

func parseExternalMembers(spec string) ([]externalMember, error) {
	parts := strings.Split(spec, ",")
	if len(parts) < 3 || len(parts)%2 == 0 {
		return nil, fmt.Errorf("soak: external targets require an odd cluster of at least 3 members")
	}
	seenID, seenAddr := map[string]bool{}, map[string]bool{}
	out := make([]externalMember, 0, len(parts))
	for _, part := range parts {
		member, err := parseExternalMember(strings.TrimSpace(part))
		if err != nil {
			return nil, err
		}
		if seenID[member.ID] || seenAddr[member.Addr] {
			return nil, fmt.Errorf("soak: duplicate external target %q", part)
		}
		seenID[member.ID], seenAddr[member.Addr] = true, true
		out = append(out, member)
	}
	return out, nil
}

func parseExternalMember(part string) (externalMember, error) {
	id, addr, ok := strings.Cut(part, "=")
	if !ok || !validExternalNodeID(id) {
		return externalMember{}, fmt.Errorf("soak: invalid target %q (want id=host:port)", part)
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" || port == "" {
		return externalMember{}, fmt.Errorf("soak: invalid target address %q", addr)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil {
		return externalMember{}, fmt.Errorf("soak: invalid target port %q", port)
	}
	if portNumber < 1 || portNumber > 65535 {
		return externalMember{}, fmt.Errorf("soak: invalid target port %q", port)
	}
	return externalMember{ID: id, Addr: addr}, nil
}

func validExternalNodeID(id string) bool {
	if id == "" {
		return false
	}
	for _, r := range id {
		if !(r == '-' || r == '_' || r == '.' || r >= '0' && r <= '9' ||
			r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z') {
			return false
		}
	}
	return true
}

func readOptionalSecret(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("soak: read auth token file: %w", err)
	}
	secret := strings.TrimSpace(string(data))
	if secret == "" {
		return "", errors.New("soak: auth token file is empty")
	}
	if strings.ContainsAny(secret, "\r\n") {
		return "", errors.New("soak: auth token file contains a newline")
	}
	return secret, nil
}

func (h *externalHarness) workerLoop(ctx context.Context, worker int) {
	ref := fmt.Sprintf("soak-%s-w%d", h.runID, worker)
	key := fmt.Sprintf("soak:%s:%d", h.runID, worker)
	options := []client.ClusterOption{
		client.WithClusterRedirectBudget(h.opts.RedirectBudget),
		client.WithClusterStableRef(ref),
	}
	if h.auth != "" {
		options = append(options, client.WithClusterAuthToken(h.auth))
	}
	cl, err := client.NewCluster(h.addrs, options...)
	if err != nil {
		h.addViolation(err.Error())
		return
	}
	h.runWorkerState(ctx, worker, key, cl)
}

func (h *externalHarness) runWorkerState(ctx context.Context, worker int, key string, cl *client.Cluster) {
	var token string
	var acquired time.Time
	for ctx.Err() == nil {
		if token == "" {
			token, acquired = h.acquireOnce(ctx, worker, key, cl)
		} else if h.releaseOnce(ctx, key, token, cl) {
			token = ""
		} else if time.Since(acquired) > h.abandonAfter() {
			token = ""
		}
		if ctx.Err() == nil {
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func (h *externalHarness) acquireOnce(ctx context.Context, worker int, key string, cl *client.Cluster) (string, time.Time) {
	h.writes.Add(1)
	opCtx, cancel := context.WithTimeout(ctx, externalOpTimeout)
	defer cancel()
	token, _, err := cl.Acquire(opCtx, key, time.Second,
		client.WithLeaseTTL(int(h.opts.LeaseTTL/time.Second)))
	if err != nil {
		h.recordOperationError(ctx, err)
		return "", time.Time{}
	}
	if violation := h.ledger.recordGrant(worker, key, token); violation != "" {
		h.addViolation(violation)
		return "", time.Time{}
	}
	return token, time.Now()
}

func (h *externalHarness) releaseOnce(ctx context.Context, key, token string, cl *client.Cluster) bool {
	opCtx, cancel := context.WithTimeout(ctx, externalOpTimeout)
	defer cancel()
	if err := cl.Release(opCtx, key, token); err != nil {
		h.recordOperationError(ctx, err)
		return false
	}
	h.ledger.recordRelease(token)
	h.successes.Add(1)
	return true
}

func (h *externalHarness) recordOperationError(ctx context.Context, err error) {
	if ctx.Err() != nil {
		return
	}
	h.failures.Add(1)
	if errors.Is(err, client.ErrTooManyRedirects) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		h.notLeader.Add(1)
	}
}

func (h *externalHarness) abandonAfter() time.Duration {
	return 2*h.opts.LeaseTTL + 2*h.opts.ClockSkew + h.opts.FaultHold + externalOpTimeout
}

func (h *externalHarness) addViolation(message string) {
	h.ledger.addViolation(message)
	if h.cancel != nil {
		h.cancel()
	}
}

func (h *externalHarness) verifyFinalState() {
	if h.successes.Load() == 0 {
		h.ledger.addViolation("soak: workload completed no acquire/release cycles")
	}
	ctx, cancel := context.WithTimeout(context.Background(), externalOpTimeout)
	defer cancel()
	if _, err := h.probe(ctx, h.members, h.auth); err != nil {
		h.ledger.addViolation(fmt.Sprintf("soak: final writable leader probe: %v", err))
	}
}

func (h *externalHarness) report() soakReport {
	return soakReport{
		Writes: int(h.writes.Load()), Successes: int(h.successes.Load()),
		Failures: int(h.failures.Load()), NotLeader: int(h.notLeader.Load()),
		Partitions: int(h.partitions.Load()), Restarts: int(h.restarts.Load()),
		Skews: int(h.skews.Load()), Violations: h.ledger.violationsCopy(),
	}
}

type grantRecord struct {
	worker  int
	key     string
	retired bool
}

type grantLedger struct {
	mu         sync.Mutex
	tokens     map[string]grantRecord
	maxByKey   map[string]uint64
	violations []string
}

func (l *grantLedger) recordGrant(worker int, key, token string) string {
	fence, err := client.FenceFromToken(token)
	if err != nil {
		return fmt.Sprintf("bad token format: %q (%v)", token, err)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if previous, ok := l.tokens[token]; ok {
		if !previous.retired && previous.worker == worker && previous.key == key {
			return ""
		}
		return "token reused: " + token
	}
	if previous, ok := l.maxByKey[key]; ok && fence <= previous {
		return fmt.Sprintf("per-key fence regression on %s: got %d, prior %d", key, fence, previous)
	}
	l.tokens[token] = grantRecord{worker: worker, key: key}
	l.maxByKey[key] = fence
	return ""
}

func (l *grantLedger) recordRelease(token string) {
	l.mu.Lock()
	record := l.tokens[token]
	record.retired = true
	l.tokens[token] = record
	l.mu.Unlock()
}

func (l *grantLedger) addViolation(message string) {
	l.mu.Lock()
	l.violations = append(l.violations, message)
	l.mu.Unlock()
}

func (l *grantLedger) violationsCopy() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.violations...)
}

func probeWritableLeader(ctx context.Context, members []externalMember, auth string) (string, error) {
	probeCtx, cancel := context.WithTimeout(ctx, externalOpTimeout)
	defer cancel()
	results := make(chan string, len(members))
	for _, member := range members {
		go func(m externalMember) {
			if probeBarrier(probeCtx, m.Addr, auth) == nil {
				results <- m.ID
			} else {
				results <- ""
			}
		}(member)
	}
	for range members {
		if id := <-results; id != "" {
			cancel()
			return id, nil
		}
	}
	return "", errors.New("soak: no writable leader")
}

func probeBarrier(ctx context.Context, addr, auth string) error {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	reader := bufio.NewReader(conn)
	if auth != "" {
		if err := probeRPC(conn, reader, "auth", "_", auth, "ok"); err != nil {
			return err
		}
	}
	return probeRPC(conn, reader, "barrier", "_", "", "ok")
}

func probeRPC(conn net.Conn, reader *bufio.Reader, cmd, key, arg, want string) error {
	if _, err := fmt.Fprintf(conn, "%s\n%s\n%s\n", cmd, key, arg); err != nil {
		return err
	}
	response, err := reader.ReadString('\n')
	if err != nil {
		return err
	}
	if got := strings.TrimSpace(response); got != want {
		return fmt.Errorf("%s response = %q", cmd, got)
	}
	return nil
}
