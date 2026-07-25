package client

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// ErrTooManyRedirects is returned when a Cluster operation exhausted
// its attempt budget without finding the leader. The returned error
// also wraps the final dial or NotLeaderError cause for diagnostics.
// This bounds an attacker-controlled redirect loop and a long partition.
var ErrTooManyRedirects = errors.New("dflockd: too many leader redirects")

// ErrNoMembers is returned by NewCluster when the members slice is
// empty. At least one member is required to bootstrap the leader hunt.
var ErrNoMembers = errors.New("dflockd: cluster: no members configured")

// defaultRedirectBudget bounds the total number of dial+op attempts
// per Cluster operation. With 3 it allows: 1 to the cached leader,
// then 1 each to the two other members of a typical 3-node cluster.
const defaultRedirectBudget = 3

// dialFunc is the connection factory injected at construction. The
// default is the package-level Dial. Tests inject a fake.
type dialFunc func(addr string) (*Conn, error)

// clusterCfg is the resolved set of options for a Cluster.
type clusterCfg struct {
	budget    int
	dial      dialFunc
	authToken string
	stableRef string
}

// ClusterOption configures NewCluster.
type ClusterOption func(*clusterCfg)

// WithClusterRedirectBudget caps the consecutive dial+op attempts a
// single Cluster operation may make before returning
// ErrTooManyRedirects. Default 3.
func WithClusterRedirectBudget(n int) ClusterOption {
	return func(c *clusterCfg) {
		if n > 0 {
			c.budget = n
		}
	}
}

// WithClusterAuthToken makes every dialed Conn authenticate with the
// given session token before the first operation. Empty token means
// no authentication step (the default).
func WithClusterAuthToken(tok string) ClusterOption {
	return func(c *clusterCfg) { c.authToken = tok }
}

// WithClusterStableRef makes every dialed Conn send a `stable-ref`
// command with ref before the first operation. With a stable ref set,
// a Cluster operation's underlying acquire/enqueue/wait carries this
// identity across a leader failover — a reconnect re-attaches to the
// existing FSM slot (preserving FIFO position) rather than starting
// from the back of the queue.
//
// ref must be non-empty and ≤ 64 ASCII bytes; longer or empty values
// are silently ignored (the Cluster falls back to the default
// connID-derived ref).
//
// A ref names one client session, so a Cluster configured with one is
// no longer safe to drive concurrently on the same key: each call
// dials its own connection, and two of them under one ref are two
// claims on the same FSM slot. Give each concurrent worker its own
// Cluster (or its own ref). Refs are also unauthenticated identifiers
// — generate them randomly and treat them like session tokens.
func WithClusterStableRef(ref string) ClusterOption {
	return func(c *clusterCfg) {
		if ref != "" && len(ref) <= 64 {
			c.stableRef = ref
		}
	}
}

// withClusterDial is an internal option used by tests to inject a
// fake dialer. Not exported.
func withClusterDial(d dialFunc) ClusterOption {
	return func(c *clusterCfg) { c.dial = d }
}

// Cluster is a failover-aware dflockd client. It keeps a process-local
// leader hint and, on each operation, dials the cached leader (or the
// first member if no cache), follows *NotLeaderError redirects up to
// the configured budget, and surfaces ErrTooManyRedirects once
// exhausted.
//
// Cluster is safe for concurrent use. Each operation dials a fresh
// connection — connection pooling is a deliberate non-goal for v1.
type Cluster struct {
	members []string
	cfg     clusterCfg
	leader  atomic.Pointer[string]
}

// NewCluster returns a failover-aware client over the given member
// list. Members are client-facing host:port pairs for every node in
// the cluster (the same value each node was started with as
// --advertise-addr, or --host:--port if unset). At least one member
// is required.
func NewCluster(members []string, opts ...ClusterOption) (*Cluster, error) {
	if len(members) == 0 {
		return nil, ErrNoMembers
	}
	cfg := clusterCfg{budget: defaultRedirectBudget, dial: Dial}
	for _, opt := range opts {
		opt(&cfg)
	}
	mem := make([]string, len(members))
	copy(mem, members)
	return &Cluster{members: mem, cfg: cfg}, nil
}

// LeaderHint returns the current leader cache (empty if none has
// been observed). Exposed for diagnostics — production callers
// should not depend on this returning the truly-current leader.
func (cl *Cluster) LeaderHint() string {
	p := cl.leader.Load()
	if p == nil {
		return ""
	}
	return *p
}

// dispatch dials a member (preferring the leader cache) and runs op
// against it. On *NotLeaderError it records the hint (only if the
// hinted address is in the operator-supplied members list — an
// attacker-controlled server cannot point us at an arbitrary host)
// and retries against the new target. Returns an error wrapping both
// ErrTooManyRedirects and the final dial/redirect cause once the budget
// is exhausted.
func (cl *Cluster) dispatch(ctx context.Context, op func(c *Conn) error) error {
	addr := cl.firstAddr()
	var lastErr error
	for attempt := 0; attempt < cl.cfg.budget; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		conn, err := cl.cfg.dial(addr)
		if err != nil {
			lastErr = fmt.Errorf("dflockd: cluster: dial %s: %w", addr, err)
			addr = cl.nextAddr(addr)
			continue
		}
		opErr := cl.runOnConn(ctx, conn, op)
		if redirected, target := redirectTarget(opErr); redirected {
			lastErr = opErr
			if cl.updateLeaderHint(target) && target != addr {
				addr = target
			} else {
				addr = cl.nextAddr(addr)
			}
			continue
		}
		return opErr
	}
	if lastErr == nil {
		return ErrTooManyRedirects
	}
	return fmt.Errorf("%w: last attempt: %w", ErrTooManyRedirects, lastErr)
}

// updateLeaderHint records a hint only if it names an address in the
// operator-supplied members list. An attacker-controlled response
// pointing at `evil.example.com:6388` is rejected — we'll keep
// rotating through the known members instead. The return reports
// whether target was accepted.
func (cl *Cluster) updateLeaderHint(target string) bool {
	if target == "" || !cl.isKnownMember(target) {
		cl.leader.Store(nil)
		return false
	}
	cl.leader.Store(&target)
	return true
}

// isKnownMember reports whether addr appears in the configured
// members list.
func (cl *Cluster) isKnownMember(addr string) bool {
	for _, m := range cl.members {
		if m == addr {
			return true
		}
	}
	return false
}

// runOnConn handles auth+stable-ref+op+close so dispatch stays under
// the cyclo ceiling.
func (cl *Cluster) runOnConn(ctx context.Context, conn *Conn, op func(c *Conn) error) error {
	defer conn.Close()
	stopCancel := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopCancel()
	if cl.cfg.authToken != "" {
		if err := Authenticate(conn, cl.cfg.authToken); err != nil {
			return preferContextErr(ctx, err)
		}
	}
	if cl.cfg.stableRef != "" {
		if err := SetStableRef(conn, cl.cfg.stableRef); err != nil {
			return preferContextErr(ctx, err)
		}
	}
	return preferContextErr(ctx, op(conn))
}

func preferContextErr(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return err
}

// firstAddr picks the cached leader when available, otherwise the first
// configured member.
func (cl *Cluster) firstAddr() string {
	if hint := cl.LeaderHint(); hint != "" {
		return hint
	}
	return cl.members[0]
}

// nextAddr returns the configured member after current, wrapping at
// the end. An unknown current address restarts at the first member.
func (cl *Cluster) nextAddr(current string) string {
	for i, member := range cl.members {
		if member == current {
			return cl.members[(i+1)%len(cl.members)]
		}
	}
	return cl.members[0]
}

// redirectTarget reports whether err is a *NotLeaderError and, if so,
// the Leader field on it.
func redirectTarget(err error) (bool, string) {
	var nle *NotLeaderError
	if errors.As(err, &nle) {
		return true, nle.Leader
	}
	return false, ""
}

// Acquire is the cluster-aware Acquire.
func (cl *Cluster) Acquire(ctx context.Context, key string, timeout time.Duration, opts ...Option) (string, int, error) {
	var token string
	var ttl int
	err := cl.dispatch(ctx, func(c *Conn) error {
		t, l, e := Acquire(c, key, timeout, opts...)
		token, ttl = t, l
		return e
	})
	return token, ttl, err
}

// Release is the cluster-aware Release.
func (cl *Cluster) Release(ctx context.Context, key, token string) error {
	return cl.dispatch(ctx, func(c *Conn) error { return Release(c, key, token) })
}

// Renew is the cluster-aware Renew.
func (cl *Cluster) Renew(ctx context.Context, key, token string, opts ...Option) (int, error) {
	var remaining int
	err := cl.dispatch(ctx, func(c *Conn) error {
		r, e := Renew(c, key, token, opts...)
		remaining = r
		return e
	})
	return remaining, err
}

// Enqueue is the cluster-aware Enqueue.
func (cl *Cluster) Enqueue(ctx context.Context, key string, opts ...Option) (string, string, int, error) {
	var status, token string
	var ttl int
	err := cl.dispatch(ctx, func(c *Conn) error {
		s, t, l, e := Enqueue(c, key, opts...)
		status, token, ttl = s, t, l
		return e
	})
	return status, token, ttl, err
}

// Wait is the cluster-aware Wait.
func (cl *Cluster) Wait(ctx context.Context, key string, timeout time.Duration) (string, int, error) {
	var token string
	var ttl int
	err := cl.dispatch(ctx, func(c *Conn) error {
		t, l, e := Wait(c, key, timeout)
		token, ttl = t, l
		return e
	})
	return token, ttl, err
}

// SemAcquire is the cluster-aware SemAcquire.
func (cl *Cluster) SemAcquire(ctx context.Context, key string, timeout time.Duration, limit int, opts ...Option) (string, int, error) {
	var token string
	var ttl int
	err := cl.dispatch(ctx, func(c *Conn) error {
		t, l, e := SemAcquire(c, key, timeout, limit, opts...)
		token, ttl = t, l
		return e
	})
	return token, ttl, err
}

// SemRelease is the cluster-aware SemRelease.
func (cl *Cluster) SemRelease(ctx context.Context, key, token string) error {
	return cl.dispatch(ctx, func(c *Conn) error { return SemRelease(c, key, token) })
}

// SemRenew is the cluster-aware SemRenew.
func (cl *Cluster) SemRenew(ctx context.Context, key, token string, opts ...Option) (int, error) {
	var remaining int
	err := cl.dispatch(ctx, func(c *Conn) error {
		r, e := SemRenew(c, key, token, opts...)
		remaining = r
		return e
	})
	return remaining, err
}

// SemEnqueue is the cluster-aware SemEnqueue.
func (cl *Cluster) SemEnqueue(ctx context.Context, key string, limit int, opts ...Option) (string, string, int, error) {
	var status, token string
	var ttl int
	err := cl.dispatch(ctx, func(c *Conn) error {
		s, t, l, e := SemEnqueue(c, key, limit, opts...)
		status, token, ttl = s, t, l
		return e
	})
	return status, token, ttl, err
}

// SemWait is the cluster-aware SemWait.
func (cl *Cluster) SemWait(ctx context.Context, key string, timeout time.Duration) (string, int, error) {
	var token string
	var ttl int
	err := cl.dispatch(ctx, func(c *Conn) error {
		t, l, e := SemWait(c, key, timeout)
		token, ttl = t, l
		return e
	})
	return token, ttl, err
}

// Barrier is the cluster-aware Barrier. In cluster mode the returned
// nil means the leader's commit index has caught up to the latest
// proposal at call time; a subsequent read on the leader will reflect
// every preceding committed write.
func (cl *Cluster) Barrier(ctx context.Context) error {
	return cl.dispatch(ctx, func(c *Conn) error { return Barrier(c) })
}
