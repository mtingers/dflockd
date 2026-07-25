package cluster

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/mtingers/dflockd/client"
	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/raft"
	"github.com/mtingers/dflockd/internal/server"
)

// Three-node end-to-end test: real TCP for both the Raft transport and
// the client transport, real dflockd Servers, real client.Dial calls.
// Verifies election, replication, follower→leader redirect, leader
// failover, mid-flight client continuity, and rejoin.

// e2eNode is one node's worth of state in the harness — all the
// resources we need to start/stop/restart it.
type e2eNode struct {
	id         raft.NodeID
	tcpAddr    string
	clientAddr string
	storage    *raft.MemStorage // state-preserving across restarts
	lm         *lock.LockManager
	transport  *raft.TCPTransport
	srv        *server.Server
	srvCancel  context.CancelFunc
	lis        net.Listener
	node       *Node
	cfg        *config.Config
}

type e2eCluster struct {
	t         *testing.T
	ids       []raft.NodeID
	members   map[raft.NodeID]Member
	nodes     map[raft.NodeID]*e2eNode
	orphanTTL time.Duration // OrphanTTL for every node's LockManager (0 = off)
	mu        sync.Mutex
}

func startE2ECluster(t *testing.T, ids ...raft.NodeID) *e2eCluster {
	return startE2EClusterOrphan(t, 0, ids...)
}

// startE2EClusterOrphan is startE2ECluster with stable-ref re-attach
// enabled (OrphanTTL > 0) on every node, for failover re-attach tests.
func startE2EClusterOrphan(t *testing.T, orphanTTL time.Duration, ids ...raft.NodeID) *e2eCluster {
	t.Helper()
	c := &e2eCluster{t: t, ids: ids, nodes: map[raft.NodeID]*e2eNode{}, orphanTTL: orphanTTL}
	c.allocMembers()
	for _, id := range ids {
		c.startOne(id)
	}
	return c
}

// allocMembers pre-reserves a Raft transport address and a client
// address for every node so each node's --cluster-peers value matches
// across nodes.
func (c *e2eCluster) allocMembers() {
	c.members = map[raft.NodeID]Member{}
	for _, id := range c.ids {
		c.members[id] = Member{RaftAddr: freeLoopback(c.t), ClientAddr: freeLoopback(c.t)}
	}
}

func (c *e2eCluster) startOne(id raft.NodeID) {
	c.t.Helper()
	n := &e2eNode{
		id:         id,
		tcpAddr:    c.members[id].RaftAddr,
		clientAddr: c.members[id].ClientAddr,
		storage:    raft.NewMemStorage(),
	}
	c.bringNodeUp(n)
	c.nodes[id] = n
}

// bringNodeUp constructs a fresh raft+server stack for n, reusing its
// MemStorage (so a stop-then-start exercises crash recovery).
func (c *e2eCluster) bringNodeUp(n *e2eNode) {
	c.t.Helper()
	n.cfg = &config.Config{
		Host: host(n.clientAddr), Port: port(n.clientAddr),
		ReadTimeout:     2 * time.Second,
		WriteTimeout:    1 * time.Second,
		DefaultLeaseTTL: 30 * time.Second,
		GCMaxIdleTime:   60 * time.Second,
		MaxLocks:        128,
		OrphanTTL:       c.orphanTTL,
	}
	lm, err := lock.NewLockManager(n.cfg, slog.Default())
	if err != nil {
		c.t.Fatalf("NewLockManager(%s): %v", n.id, err)
	}
	n.lm = lm
	c.buildRaftAndServer(n)
	c.bindClientPort(n)
}

func (c *e2eCluster) buildRaftAndServer(n *e2eNode) {
	c.t.Helper()
	rcfg := raft.DefaultConfig()
	rcfg.ID = n.id
	rcfg.HeartbeatInterval = 10 * time.Millisecond
	rcfg.ElectionTimeoutMin = 60 * time.Millisecond
	rcfg.ElectionTimeoutMax = 120 * time.Millisecond
	tr, err := raft.NewTCPTransport(
		n.id, n.tcpAddr, slog.Default(),
		raft.WithClusterSecret("0123456789abcdef0123456789abcdef"),
	)
	if err != nil {
		c.t.Fatalf("NewTCPTransport(%s): %v", n.id, err)
	}
	for id, m := range c.members {
		if id != n.id {
			tr.AddPeer(id, m.RaftAddr)
		}
	}
	n.transport = tr
	ccfg := Config{Raft: rcfg, Members: c.members, AdvertiseAddr: n.clientAddr}
	node, err := NewNode(ccfg, n.lm, n.storage, tr, slog.Default())
	if err != nil {
		c.t.Fatalf("cluster NewNode(%s): %v", n.id, err)
	}
	node.Start()
	n.node = node
	n.srv = server.New(n.lm, n.cfg, slog.Default())
	n.srv.SetCluster(node)
}

func (c *e2eCluster) bindClientPort(n *e2eNode) {
	c.t.Helper()
	lis, err := net.Listen("tcp", n.clientAddr)
	if err != nil {
		c.t.Fatalf("client listen(%s): %v", n.id, err)
	}
	n.lis = lis
	ctx, cancel := context.WithCancel(context.Background())
	n.srvCancel = cancel
	go func() { _ = n.srv.RunOnListener(ctx, lis) }()
}

// stopOne simulates a process kill: SetCluster(nil) FIRST so no
// in-flight teardownConn can propose CleanupConn — that models a hard
// crash, where the leader's holders persist on survivors until their
// leases expire (no graceful disconnect to replicate).
func (c *e2eCluster) stopOne(id raft.NodeID) {
	c.t.Helper()
	n := c.nodes[id]
	if n == nil || n.srv == nil {
		return // never started, or already stopped (idempotent)
	}
	n.srv.SetCluster(nil)
	n.srvCancel()
	_ = n.lis.Close()
	_ = n.node.Close()
	_ = n.transport.Close()
	_ = n.lm.Close()
	n.node, n.transport, n.lm, n.srv, n.srvCancel, n.lis = nil, nil, nil, nil, nil, nil
}

// restartOne brings node id back up against the same MemStorage —
// simulates a process crash + restart.
func (c *e2eCluster) restartOne(id raft.NodeID) {
	c.t.Helper()
	c.bringNodeUp(c.nodes[id])
}

func (c *e2eCluster) stopAll() {
	for _, id := range c.ids {
		c.stopOne(id)
	}
}

// waitLeader waits until exactly one node (among `ids`) is leader and
// every reachable node agrees on its identity.
func (c *e2eCluster) waitLeader(timeout time.Duration, ids ...raft.NodeID) raft.NodeID {
	c.t.Helper()
	if len(ids) == 0 {
		ids = c.ids
	}
	var leader raft.NodeID
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if id, ok := c.findStableLeader(ids); ok {
			leader = id
			return leader
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("no stable leader among %v in %v", ids, timeout)
	return ""
}

func (c *e2eCluster) findStableLeader(ids []raft.NodeID) (raft.NodeID, bool) {
	var leader raft.NodeID
	count := 0
	for _, id := range ids {
		n := c.nodes[id]
		if n == nil || n.node == nil {
			continue
		}
		if n.node.IsLeader() {
			leader, count = id, count+1
		}
	}
	if count != 1 {
		return "", false
	}
	for _, id := range ids {
		n := c.nodes[id]
		if n != nil && n.node != nil && n.node.LeaderID() != leader {
			return "", false
		}
	}
	return leader, true
}

func (c *e2eCluster) holdersOnNode(id raft.NodeID, key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.nodes[id]
	if n == nil || n.lm == nil {
		return -1
	}
	st := n.lm.Stats(0)
	count := 0
	for _, li := range st.Locks {
		if lock.StripKeyPrefix(li.Key) == key {
			count++
		}
	}
	return count
}

func (c *e2eCluster) clientAddrOf(id raft.NodeID) string { return c.members[id].ClientAddr }

func otherIDs(all []raft.NodeID, excl raft.NodeID) []raft.NodeID {
	out := make([]raft.NodeID, 0, len(all)-1)
	for _, id := range all {
		if id != excl {
			out = append(out, id)
		}
	}
	return out
}

// --- the actual test ---

func TestE2EThreeNodeReplicationAndFailover(t *testing.T) {
	c := startE2ECluster(t, "alpha", "beta", "gamma")
	defer c.stopAll()

	// === step 1: elect a leader ===
	leader := c.waitLeader(3 * time.Second)
	t.Logf("step 1: cluster elected %s as leader", leader)
	leaderAddr := c.clientAddrOf(leader)

	// === step 2: acquire on leader, verify replication ===
	// Keep this connection alive across the test — closing it would
	// (correctly) trigger CleanupConn and auto-release the lock.
	leaderConn, err := client.Dial(leaderAddr)
	if err != nil {
		t.Fatalf("step 2: Dial leader %s: %v", leader, err)
	}
	defer leaderConn.Close()
	token, lease, err := client.Acquire(leaderConn, "kA", 2*time.Second)
	if err != nil {
		t.Fatalf("step 2: Acquire on leader: %v", err)
	}
	if token == "" || lease <= 0 {
		t.Fatalf("step 2: bad Acquire result tok=%q lease=%d", token, lease)
	}
	t.Logf("step 2: acquired kA on leader %s; token=%s lease=%ds", leader, token, lease)
	for _, id := range c.ids {
		if !waitForN(t, 2*time.Second, func() bool { return c.holdersOnNode(id, "kA") == 1 }) {
			t.Fatalf("step 2: node %s never observed the kA holder", id)
		}
	}
	for _, id := range c.ids {
		tokens := c.nodes[id].lm.DebugHolderTokens("lock:kA")
		if len(tokens) != 1 || tokens[0] != token {
			t.Fatalf("step 2: node %s kA holders = %v, want [%s]", id, tokens, token)
		}
	}
	t.Logf("step 2: all 3 LockManagers hold kA at exactly the same token")

	// === step 3: a follower redirects mutating ops ===
	follower := otherIDs(c.ids, leader)[0]
	{
		conn, err := client.Dial(c.clientAddrOf(follower))
		if err != nil {
			t.Fatalf("step 3: Dial follower %s: %v", follower, err)
		}
		_, _, err = client.Acquire(conn, "kB", 500*time.Millisecond)
		_ = conn.Close()
		var nle *client.NotLeaderError
		if !client.IsNotLeader(err, &nle) {
			t.Fatalf("step 3: want *NotLeaderError, got %v", err)
		}
		if nle.Leader != leaderAddr {
			t.Fatalf("step 3: redirect target = %q, want %q", nle.Leader, leaderAddr)
		}
		t.Logf("step 3: follower %s redirected client to %s", follower, nle.Leader)
	}

	// === step 4: hard-crash the leader, verify cluster keeps serving ===
	// A "hard crash" model: SetCluster(nil) before the server tears down
	// so no CleanupConn is proposed. The crashed leader's holders persist
	// on the survivors until lease expiry — that's the correct
	// safety-first behaviour (the operator can't tell a hung leader from
	// a partitioned one, so locks must NOT auto-release on crash).
	t.Logf("step 4: hard-crashing leader %s...", leader)
	c.stopOne(leader)
	rest := otherIDs(c.ids, leader)
	// kA must still be held on the survivors (we didn't release it).
	for _, id := range rest {
		toks := c.nodes[id].lm.DebugHolderTokens("lock:kA")
		if len(toks) != 1 || toks[0] != token {
			t.Fatalf("step 4: survivor %s kA = %v, want [%s] (crash must preserve holders)", id, toks, token)
		}
	}
	t.Logf("step 4: kA holders persist on survivors (correct crash-safety semantic)")

	newLeader := c.waitLeader(5*time.Second, rest...)
	if newLeader == leader {
		t.Fatalf("step 4: new leader same as old crashed one?")
	}
	t.Logf("step 4: new leader is %s", newLeader)

	newLeaderConn, err := client.Dial(c.clientAddrOf(newLeader))
	if err != nil {
		t.Fatalf("step 4: Dial new leader: %v", err)
	}
	defer newLeaderConn.Close()
	newTok, newLease, err := client.Acquire(newLeaderConn, "kPost", 2*time.Second)
	if err != nil {
		t.Fatalf("step 4: Acquire on new leader: %v", err)
	}
	t.Logf("step 4: acquired kPost on new leader; token=%s lease=%ds", newTok, newLease)
	for _, id := range rest {
		if !waitForN(t, 2*time.Second, func() bool { return c.holdersOnNode(id, "kPost") == 1 }) {
			t.Fatalf("step 4: survivor %s never observed kPost", id)
		}
		toks := c.nodes[id].lm.DebugHolderTokens("lock:kPost")
		if len(toks) != 1 || toks[0] != newTok {
			t.Fatalf("step 4: survivor %s kPost = %v, want [%s]", id, toks, newTok)
		}
	}
	t.Logf("step 4: kPost replicated identically to both survivors")

	// === step 5: rejoin the crashed leader ===
	t.Logf("step 5: restarting node %s...", leader)
	c.restartOne(leader)
	if !waitForN(t, 3*time.Second, func() bool {
		n := c.nodes[leader]
		if n == nil || n.node == nil {
			return false
		}
		s := n.node.Status()
		return s.Role == "follower" && s.LeaderID == newLeader
	}) {
		s := c.nodes[leader].node.Status()
		t.Fatalf("step 5: rejoined %s status = %+v, want follower of %s", leader, s, newLeader)
	}
	t.Logf("step 5: %s rejoined as follower of %s", leader, newLeader)
	// And it catches up — its FSM sees kPost.
	if !waitForN(t, 3*time.Second, func() bool { return c.holdersOnNode(leader, "kPost") == 1 }) {
		t.Fatalf("step 5: %s did not catch up to kPost", leader)
	}
	toks := c.nodes[leader].lm.DebugHolderTokens("lock:kPost")
	if len(toks) != 1 || toks[0] != newTok {
		t.Fatalf("step 5: rejoined %s kPost = %v, want [%s]", leader, toks, newTok)
	}
	t.Logf("step 5: %s caught up to kPost (token matches)", leader)

	// Releasing kPost via the new leader works.
	if err := client.Release(newLeaderConn, "kPost", newTok); err != nil {
		t.Fatalf("step 5: Release kPost: %v", err)
	}
	t.Logf("step 5: released kPost cleanly")
}

func waitForN(t *testing.T, d time.Duration, ok func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if ok() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// freeLoopback picks an ephemeral loopback address and closes the
// listener, returning the "host:port" string.
func freeLoopback(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for free port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

// TestE2EStableRefReAttachAcrossFailover proves the headline failover
// promise end-to-end over real TCP: a client holding a lock under a
// stable ref, whose leader is HARD-crashed (no graceful CleanupConn),
// reclaims the SAME lock token on reconnect to the freshly-elected
// leader. This needs OrphanTTL > 0. Before the PR-5 re-adopt fix the
// reconnect's single-phase Acquire would queue behind its own orphaned
// holder and time out — so this test is the regression guard for the
// gap PR-4 shipped but did not close.
func TestE2EStableRefReAttachAcrossFailover(t *testing.T) {
	c := startE2EClusterOrphan(t, 30*time.Second, "alpha", "beta", "gamma")
	defer c.stopAll()

	leader := c.waitLeader(3 * time.Second)

	// Acquire kA under a stable ref and KEEP the connection open. Closing
	// it would gracefully orphan the holder (abandonedAtNanos set) — the
	// case even the old finders matched. Holding it open until the crash
	// is what makes this a true hard crash: stopOne tears the node down
	// with cluster=nil, so no CleanupConn is ever replicated and the
	// holder reaches the new leader with abandonedAtNanos == 0.
	conn, err := client.Dial(c.clientAddrOf(leader))
	if err != nil {
		t.Fatalf("dial leader: %v", err)
	}
	if err := client.SetStableRef(conn, "worker-1"); err != nil {
		t.Fatalf("SetStableRef: %v", err)
	}
	token, _, err := client.Acquire(conn, "kA", 2*time.Second)
	if err != nil {
		t.Fatalf("Acquire kA: %v", err)
	}
	for _, id := range c.ids {
		if !waitForN(t, 2*time.Second, func() bool { return c.holdersOnNode(id, "kA") == 1 }) {
			t.Fatalf("node %s never observed the kA holder", id)
		}
	}

	// HARD crash the leader, then close the now-dead socket.
	c.stopOne(leader)
	_ = conn.Close()
	rest := otherIDs(c.ids, leader)
	newLeader := c.waitLeader(5*time.Second, rest...)
	if newLeader == leader {
		t.Fatalf("new leader == crashed leader %s", leader)
	}
	// The holder survived the crash un-orphaned (abandonedAtNanos == 0) —
	// the exact state PR-4's finders skipped.
	for _, id := range rest {
		toks := c.nodes[id].lm.DebugHolderTokens("lock:kA")
		if len(toks) != 1 || toks[0] != token {
			t.Fatalf("pre-reconnect survivor %s kA = %v, want [%s]", id, toks, token)
		}
	}

	// Reconnect to the new leader with the SAME stable ref and re-acquire.
	// Without re-adopt this single-phase Acquire would queue behind the
	// orphaned holder and time out; with it, the original token returns.
	conn2, err := client.Dial(c.clientAddrOf(newLeader))
	if err != nil {
		t.Fatalf("dial new leader: %v", err)
	}
	defer conn2.Close()
	if err := client.SetStableRef(conn2, "worker-1"); err != nil {
		t.Fatalf("SetStableRef on reconnect: %v", err)
	}
	token2, _, err := client.Acquire(conn2, "kA", 2*time.Second)
	if err != nil {
		t.Fatalf("re-acquire kA on new leader: %v", err)
	}
	if token2 != token {
		t.Fatalf("re-attach token = %q, want original %q (reconnect must reclaim its holder, not re-queue)", token2, token)
	}
	// Exactly one holder on every survivor — re-adopted, not duplicated.
	for _, id := range rest {
		toks := c.nodes[id].lm.DebugHolderTokens("lock:kA")
		if len(toks) != 1 || toks[0] != token {
			t.Fatalf("survivor %s kA holders = %v, want [%s] (one re-adopted holder)", id, toks, token)
		}
	}
}
