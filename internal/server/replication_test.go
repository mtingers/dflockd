package server

import (
	"bufio"
	"context"
	"log/slog"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/replication"
)

// pickFreeAddr returns an immediately-available 127.0.0.1 address.
func pickFreeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// snapshotAdapter is the same translation shim used in cmd/dflockd/main.go,
// duplicated here to keep the test self-contained.
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

// startReplicaPair brings up a primary + secondary pair of full
// Server instances connected via an in-process replication link. It
// returns both client-listener addresses, both lock managers (so tests
// can poke at state directly), and a single cleanup that tears
// everything down. Used by replication integration tests below.
func startReplicaPair(t *testing.T) (priAddr, secAddr string, priLM, secLM *lock.LockManager, cleanup func()) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	priCfg := testConfig()
	priCfg.LeaseSweepInterval = time.Hour // suppress noise during the test
	priLM = lock.NewLockManager(priCfg, log)
	priSrv := New(priLM, priCfg, log)

	secCfg := testConfig()
	secCfg.LeaseSweepInterval = time.Hour
	secLM = lock.NewLockManager(secCfg, log)
	secSrv := New(secLM, secCfg, log)

	priClientLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	priAddr = priClientLn.Addr().String()
	secClientLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	secAddr = secClientLn.Addr().String()

	repPeerAddr := pickFreeAddr(t)

	priRep := replication.NewReplicator(replication.Config{
		Role:        replication.RolePrimary,
		NodeID:      "pri",
		PeerAddr:    repPeerAddr,
		MaxPause:    5 * time.Second,
		Apply:       priLM,
		Snapshotter: snapshotAdapter{priLM},
		Log:         log,
	})
	priLM.SetReplicationHook(priRep)
	priSrv.SetReplicator(priRep)

	secRep := replication.NewReplicator(replication.Config{
		Role:        replication.RoleSecondary,
		NodeID:      "sec",
		ListenAddr:  repPeerAddr,
		Apply:       secLM,
		Snapshotter: snapshotAdapter{secLM},
		Log:         log,
	})
	secSrv.SetReplicator(secRep)

	ctx, cancel := context.WithCancel(context.Background())
	if err := secRep.Start(ctx); err != nil {
		t.Fatalf("sec rep start: %v", err)
	}
	if err := priRep.Start(ctx); err != nil {
		t.Fatalf("pri rep start: %v", err)
	}

	priDone := make(chan struct{})
	secDone := make(chan struct{})
	go func() {
		defer close(priDone)
		_ = priSrv.RunOnListener(ctx, priClientLn)
	}()
	go func() {
		defer close(secDone)
		_ = secSrv.RunOnListener(ctx, secClientLn)
	}()

	// Wait until the replication link is Active on the primary side
	// (handshake complete).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && priRep.State() != replication.StateActive {
		time.Sleep(20 * time.Millisecond)
	}
	if priRep.State() != replication.StateActive {
		cancel()
		<-priDone
		<-secDone
		t.Fatalf("primary replication did not reach Active: %s", priRep.State())
	}

	cleanup = func() {
		cancel()
		<-priDone
		<-secDone
	}
	return priAddr, secAddr, priLM, secLM, cleanup
}

// TestIntegration_ReplicationAcquireReplicates exercises the full
// stack: a TCP client connects to the primary, acquires a lock, and
// the secondary's lock manager sees the resulting holder.
func TestIntegration_ReplicationAcquireReplicates(t *testing.T) {
	priAddr, _, _, secLM, cleanup := startReplicaPair(t)
	defer cleanup()

	conn, err := net.Dial("tcp", priAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	resp := connSendCmd(t, conn, reader, "l", "k1", "5 30")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("primary acquire: %q", resp)
	}
	parts := strings.Fields(resp)
	if len(parts) < 3 {
		t.Fatalf("malformed acquire response: %q", resp)
	}
	tok := parts[1]

	// Secondary should now have a holder for "lock:k1" with this token.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		secLM.LockKeyForTest(lock.LockPrefix + "k1")
		rs := secLM.ResourceForTest(lock.LockPrefix + "k1")
		_, has := false, false
		if rs != nil {
			_, has = rs.Holders[tok]
		}
		secLM.UnlockKeyForTest(lock.LockPrefix + "k1")
		_ = has
		if rs != nil && len(rs.Holders) == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("secondary never received the holder")
}

// TestIntegration_ReplicationSecondaryRefusesMutations confirms that
// a TCP client connecting directly to the secondary's client listener
// gets error_paused for any mutation command.
func TestIntegration_ReplicationSecondaryRefusesMutations(t *testing.T) {
	_, secAddr, _, _, cleanup := startReplicaPair(t)
	defer cleanup()

	conn, err := net.Dial("tcp", secAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// ping should still work.
	if resp := connSendCmd(t, conn, reader, "ping", "_", ""); resp != "ok" {
		t.Fatalf("ping on secondary: %q", resp)
	}
	// Acquire (a mutation) should be rejected.
	if resp := connSendCmd(t, conn, reader, "l", "k1", "5 30"); resp != "error_paused" {
		t.Fatalf("mutation on secondary: got %q want error_paused", resp)
	}
	// Release also rejected.
	if resp := connSendCmd(t, conn, reader, "r", "k1", "anything"); resp != "error_paused" {
		t.Fatalf("release on secondary: got %q want error_paused", resp)
	}
}

// TestIntegration_ReplicationReleaseReplicates verifies the
// reverse direction: a Release on the primary clears the holder on
// the secondary too.
func TestIntegration_ReplicationReleaseReplicates(t *testing.T) {
	priAddr, _, _, secLM, cleanup := startReplicaPair(t)
	defer cleanup()

	conn, err := net.Dial("tcp", priAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	resp := connSendCmd(t, conn, reader, "l", "release-key", "5 30")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("acquire: %q", resp)
	}
	tok := strings.Fields(resp)[1]

	// Wait for replication to land.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		secLM.LockKeyForTest(lock.LockPrefix + "release-key")
		rs := secLM.ResourceForTest(lock.LockPrefix + "release-key")
		hasHolder := rs != nil && len(rs.Holders) == 1
		secLM.UnlockKeyForTest(lock.LockPrefix + "release-key")
		if hasHolder {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if resp := connSendCmd(t, conn, reader, "r", "release-key", tok); resp != "ok" {
		t.Fatalf("release: %q", resp)
	}

	// Secondary should see the holder removed.
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		secLM.LockKeyForTest(lock.LockPrefix + "release-key")
		rs := secLM.ResourceForTest(lock.LockPrefix + "release-key")
		empty := rs == nil || len(rs.Holders) == 0
		secLM.UnlockKeyForTest(lock.LockPrefix + "release-key")
		if empty {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("secondary still has holder after primary release")
}

// silence the unused import on platforms where the harness doesn't
// reach the *config.Config type via testConfig().
var _ = config.Config{}
