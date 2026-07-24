package cluster

import (
	"context"
	"log/slog"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/mtingers/dflockd/client"
	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/raft"
	"github.com/mtingers/dflockd/internal/server"
)

// End-to-end smoke test: spin up a real single-node Raft cluster
// running a full dflockd Server over a real TCP socket, point the Go
// client at it, and verify Acquire / Release work — proving the whole
// stack (raft → cluster.Node → server cluster handlers → wire protocol
// → client) is wired correctly.

func TestE2ESingleNodeAcquireRelease(t *testing.T) {
	tcpAddr, clientAddr := freeAddrs(t, 2)
	id := raft.NodeID("e2e-1")

	cfg := &config.Config{
		Host: host(clientAddr), Port: port(clientAddr),
		ReadTimeout:     2 * time.Second,
		WriteTimeout:    1 * time.Second,
		DefaultLeaseTTL: 30 * time.Second,
		GCMaxIdleTime:   60 * time.Second,
		MaxLocks:        128,
	}
	lm, err := lock.NewLockManager(cfg, slog.Default())
	if err != nil {
		t.Fatalf("NewLockManager: %v", err)
	}
	defer lm.Close()

	rcfg := raft.DefaultConfig()
	rcfg.ID = id
	rcfg.HeartbeatInterval = 5 * time.Millisecond
	rcfg.ElectionTimeoutMin = 30 * time.Millisecond
	rcfg.ElectionTimeoutMax = 60 * time.Millisecond

	transport, err := raft.NewTCPTransport(
		id, tcpAddr, slog.Default(),
		raft.WithClusterSecret("0123456789abcdef0123456789abcdef"),
	)
	if err != nil {
		t.Fatalf("NewTCPTransport: %v", err)
	}
	defer transport.Close()
	storage := raft.NewMemStorage()
	ccfg := Config{
		Raft:          rcfg,
		Members:       map[raft.NodeID]Member{id: {RaftAddr: tcpAddr, ClientAddr: clientAddr}},
		AdvertiseAddr: clientAddr,
	}
	node, err := NewNode(ccfg, lm, storage, transport, slog.Default())
	if err != nil {
		t.Fatalf("cluster NewNode: %v", err)
	}
	node.Start()
	defer node.Close()

	srv := server.New(lm, cfg, slog.Default())
	srv.SetCluster(node)

	lis, err := net.Listen("tcp", clientAddr)
	if err != nil {
		t.Fatalf("client listen: %v", err)
	}
	defer lis.Close()
	srvCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = srv.RunOnListener(srvCtx, lis) }()

	// Wait for the cluster to elect itself.
	if !waitFor(t, 2*time.Second, node.IsLeader) {
		t.Fatalf("single-node cluster failed to self-elect")
	}

	// Dial the server over the real wire and run an Acquire/Release.
	c, err := client.Dial(clientAddr)
	if err != nil {
		t.Fatalf("client.Dial: %v", err)
	}
	defer c.Close()
	token, leaseSec, err := client.Acquire(c, "e2e-key", 2*time.Second)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if token == "" || leaseSec <= 0 {
		t.Fatalf("Acquire result: token=%q lease=%d", token, leaseSec)
	}
	if err := client.Release(c, "e2e-key", token); err != nil {
		t.Fatalf("Release: %v", err)
	}
}

// freeAddrs returns n free loopback addresses (host:port) by listening
// on port 0 and closing.
func freeAddrs(t *testing.T, n int) (string, string) {
	t.Helper()
	if n != 2 {
		t.Fatalf("freeAddrs supports exactly 2")
	}
	a := freePort(t)
	b := freePort(t)
	return a, b
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for free port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func host(addr string) string {
	h, _, _ := net.SplitHostPort(addr)
	return h
}

func port(addr string) int {
	_, p, _ := net.SplitHostPort(addr)
	n, _ := strconv.Atoi(p)
	return n
}

func waitFor(t *testing.T, d time.Duration, ok func() bool) bool {
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
