package client

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/protocol"
	"github.com/mtingers/dflockd/internal/server"
)

// pipeFakeServer is a mini in-process dflockd server. Each instance has a
// canned response routine; a connection's worth of input is read line
// by line and one response line is written back per request.
type pipeFakeServer struct {
	respond func(cmd, key, arg string) string
	count   atomic.Int64
}

// newFakeConn returns a *Conn backed by a net.Pipe whose other end is
// driven by srv.respond. The handler goroutine runs until the test's
// cleanup closes the connection.
func newFakeConn(t *testing.T, srv *pipeFakeServer) *Conn {
	t.Helper()
	c1, c2 := net.Pipe()
	go runFakeServer(c2, srv)
	t.Cleanup(func() { _ = c1.Close(); _ = c2.Close() })
	return &Conn{conn: c1, reader: bufio.NewReader(c1)}
}

func newConnClosingOnFirstOperation(t *testing.T) *Conn {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	go func() {
		defer serverConn.Close()
		reader := bufio.NewReader(serverConn)
		for i := 0; i < 3; i++ {
			if _, err := readLineFake(reader); err != nil {
				return
			}
		}
		if _, err := serverConn.Write([]byte("ok\n")); err != nil {
			return
		}
		for i := 0; i < 3; i++ {
			if _, err := readLineFake(reader); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() { _ = clientConn.Close(); _ = serverConn.Close() })
	return &Conn{conn: clientConn, reader: bufio.NewReader(clientConn)}
}

// runFakeServer reads three-line dflockd requests (cmd, key, arg) and
// writes one response line per request, until conn is closed.
func runFakeServer(conn net.Conn, srv *pipeFakeServer) {
	defer conn.Close()
	rd := bufio.NewReader(conn)
	for {
		cmd, err := readLineFake(rd)
		if err != nil {
			return
		}
		key, err := readLineFake(rd)
		if err != nil {
			return
		}
		arg, err := readLineFake(rd)
		if err != nil {
			return
		}
		resp := "ok"
		if cmd != "stable-ref" {
			srv.count.Add(1)
			resp = srv.respond(cmd, key, arg)
		}
		if _, err := conn.Write([]byte(resp + "\n")); err != nil {
			return
		}
	}
}

func readLineFake(rd *bufio.Reader) (string, error) {
	line, err := rd.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\n"), nil
}

// dialerFor returns a dialFunc that maps an address string to its
// matching *Conn, using net.Pipe under the hood. Unknown addresses
// return net.ErrClosed.
func dialerFor(t *testing.T, mp map[string]*pipeFakeServer) dialFunc {
	return func(_ context.Context, addr string) (*Conn, error) {
		srv, ok := mp[addr]
		if !ok {
			return nil, errors.New("dialerFor: no fake server registered for " + addr)
		}
		return newFakeConn(t, srv), nil
	}
}

// always returns the same response regardless of the request.
func always(resp string) func(string, string, string) string {
	return func(_, _, _ string) string { return resp }
}

// validTokenLine is a real-looking 32-char hex token + lease line, so
// the client parsers accept it as a successful grant.
const validTokenLine = "ok 0123456789abcdef0123456789abcdef 60"

// TestNewClusterRejectsEmptyMembers verifies the constructor refuses
// an empty members slice. This is a low-cost guard against an obvious
// caller bug.
func TestNewClusterRejectsEmptyMembers(t *testing.T) {
	_, err := NewCluster(nil)
	if !errors.Is(err, ErrNoMembers) {
		t.Fatalf("err = %v, want ErrNoMembers", err)
	}
}

func TestNewClusterRejectsInvalidStableRef(t *testing.T) {
	_, err := NewCluster([]string{"127.0.0.1:9001"}, WithClusterStableRef(""))
	if !errors.Is(err, ErrInvalidStableRef) {
		t.Fatalf("err = %v, want ErrInvalidStableRef", err)
	}
}

func TestNewClusterAcceptsTLSWithInjectedDialer(t *testing.T) {
	cl, err := NewCluster(
		[]string{"127.0.0.1:9001"},
		WithClusterTLS(&tls.Config{MinVersion: tls.VersionTLS13}),
		withClusterDial(func(context.Context, string) (*Conn, error) {
			return nil, errors.New("unused")
		}),
	)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	if cl.cfg.tlsConfig == nil || cl.cfg.tlsConfig.MinVersion != tls.VersionTLS13 {
		t.Fatalf("TLS config was not retained: %+v", cl.cfg.tlsConfig)
	}
}

func TestNewClusterGeneratesStableIdentityByDefault(t *testing.T) {
	a, err := NewCluster([]string{"127.0.0.1:9001"}, withClusterDial(func(context.Context, string) (*Conn, error) {
		return nil, errors.New("unused")
	}))
	if err != nil {
		t.Fatalf("NewCluster A: %v", err)
	}
	b, err := NewCluster([]string{"127.0.0.1:9001"}, withClusterDial(func(context.Context, string) (*Conn, error) {
		return nil, errors.New("unused")
	}))
	if err != nil {
		t.Fatalf("NewCluster B: %v", err)
	}
	if a.cfg.stableRef == "" || a.cfg.stableRef == b.cfg.stableRef {
		t.Fatalf("generated refs = %q, %q", a.cfg.stableRef, b.cfg.stableRef)
	}
	if err := protocol.ValidateStableRef(a.cfg.stableRef); err != nil {
		t.Fatalf("generated ref invalid: %v", err)
	}
}

func TestClusterReusesPersistentLanesForAcquireAndRelease(t *testing.T) {
	srv := &pipeFakeServer{respond: func(cmd, _, _ string) string {
		if cmd == "l" {
			return validTokenLine
		}
		return "ok"
	}}
	var dials atomic.Int64
	cl, err := NewCluster([]string{"127.0.0.1:9001"}, withClusterDial(func(_ context.Context, _ string) (*Conn, error) {
		dials.Add(1)
		return newFakeConn(t, srv), nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	// Acquire uses the session lane and Release the control lane, so the
	// first pair dials both. Everything after that reuses them.
	for i := 0; i < 3; i++ {
		token, _, err := cl.Acquire(context.Background(), "k", 0)
		if err != nil {
			t.Fatalf("Acquire %d: %v", i, err)
		}
		if err := cl.Release(context.Background(), "k", token); err != nil {
			t.Fatalf("Release %d: %v", i, err)
		}
	}
	if got := dials.Load(); got != 2 {
		t.Fatalf("dial count = %d, want 2 persistent lanes (session + control)", got)
	}
	if err := cl.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, _, err := cl.Acquire(context.Background(), "k", 0); !errors.Is(err, ErrClusterClosed) {
		t.Fatalf("Acquire after Close = %v, want ErrClusterClosed", err)
	}
}

func TestClusterEnqueueAndWaitShareConnection(t *testing.T) {
	srv := &pipeFakeServer{respond: func(cmd, _, _ string) string {
		if cmd == "e" {
			return "queued"
		}
		return validTokenLine
	}}
	var dials atomic.Int64
	cl, err := NewCluster([]string{"127.0.0.1:9001"}, withClusterDial(func(_ context.Context, _ string) (*Conn, error) {
		dials.Add(1)
		return newFakeConn(t, srv), nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	status, _, _, err := cl.Enqueue(context.Background(), "k")
	if err != nil || status != "queued" {
		t.Fatalf("Enqueue = %q, %v", status, err)
	}
	token, _, err := cl.Wait(context.Background(), "k", time.Second)
	if err != nil || token == "" {
		t.Fatalf("Wait = %q, %v", token, err)
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("dial count = %d, want one shared connection", got)
	}
}

func TestClusterPersistentSessionAgainstRealServer(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	cfg := &config.Config{
		MaxLocks: 128, DefaultLeaseTTL: time.Minute,
		LeaseSweepInterval: 10 * time.Millisecond,
		GCInterval:         time.Second, GCMaxIdleTime: time.Minute,
		ReadTimeout: 25 * time.Millisecond, WriteTimeout: time.Second,
		ShutdownTimeout: time.Second, AutoReleaseOnDisconnect: true,
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	lm, err := lock.NewLockManager(cfg, log)
	if err != nil {
		listener.Close()
		t.Fatalf("NewLockManager: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	srv := server.New(lm, cfg, log)
	go func() { done <- srv.RunOnListener(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("server did not stop")
		}
		_ = lm.Close()
	})

	owner, err := NewCluster([]string{listener.Addr().String()})
	if err != nil {
		t.Fatalf("NewCluster owner: %v", err)
	}
	defer owner.Close()
	competitor, err := NewCluster([]string{listener.Addr().String()})
	if err != nil {
		t.Fatalf("NewCluster competitor: %v", err)
	}
	defer competitor.Close()

	token, _, err := owner.Acquire(context.Background(), "persistent", 0)
	if err != nil || token == "" {
		t.Fatalf("owner Acquire = %q, %v", token, err)
	}
	if got := lm.DebugHolderTokens("lock:persistent"); len(got) != 1 || got[0] != token {
		t.Fatalf("holder after owner acquire = %v, want %q", got, token)
	}
	time.Sleep(75 * time.Millisecond)
	if _, _, err := competitor.Acquire(context.Background(), "persistent", 0); !errors.Is(err, ErrTimeout) {
		t.Fatalf("competitor acquired before release: %v", err)
	}
	if got := lm.DebugHolderTokens("lock:persistent"); len(got) != 1 || got[0] != token {
		t.Fatalf("holder after competitor timeout = %v, want %q", got, token)
	}
	if err := owner.Release(context.Background(), "persistent", token); err != nil {
		t.Fatalf("owner Release: %v", err)
	}
	token2, _, err := competitor.Acquire(context.Background(), "persistent", time.Second)
	if err != nil || token2 == "" {
		t.Fatalf("competitor Acquire after release = %q, %v", token2, err)
	}
}

// TestClusterAcquireFollowsLeaderRedirect: the first member returns
// not_leader pointing at the second, the second succeeds. The client
// should transparently follow the redirect and return the token.
func TestClusterAcquireFollowsLeaderRedirect(t *testing.T) {
	follower := &pipeFakeServer{respond: always("error_not_leader 127.0.0.1:9002")}
	leader := &pipeFakeServer{respond: always(validTokenLine)}
	cl, err := NewCluster([]string{"127.0.0.1:9001", "127.0.0.1:9002"},
		withClusterDial(dialerFor(t, map[string]*pipeFakeServer{
			"127.0.0.1:9001": follower,
			"127.0.0.1:9002": leader,
		})))
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	tok, ttl, err := cl.Acquire(context.Background(), "k", 0)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if tok == "" || ttl == 0 {
		t.Fatalf("Acquire: empty response %q %d", tok, ttl)
	}
	if got := cl.LeaderHint(); got != "127.0.0.1:9002" {
		t.Fatalf("LeaderHint = %q, want 127.0.0.1:9002", got)
	}
}

// TestClusterDialsCachedLeaderFirst: once the leader cache is set, a
// subsequent call should hit the leader on the first try.
func TestClusterDialsCachedLeaderFirst(t *testing.T) {
	follower := &pipeFakeServer{respond: always("error_not_leader 127.0.0.1:9002")}
	leader := &pipeFakeServer{respond: always(validTokenLine)}
	cl, _ := NewCluster([]string{"127.0.0.1:9001", "127.0.0.1:9002"},
		withClusterDial(dialerFor(t, map[string]*pipeFakeServer{
			"127.0.0.1:9001": follower,
			"127.0.0.1:9002": leader,
		})))
	// Warm up the cache.
	if _, _, err := cl.Acquire(context.Background(), "k", 0); err != nil {
		t.Fatalf("warmup: %v", err)
	}
	// Reset counters; the second call should not hit the follower.
	follower.count.Store(0)
	leader.count.Store(0)
	if _, _, err := cl.Acquire(context.Background(), "k", 0); err != nil {
		t.Fatalf("second call: %v", err)
	}
	if follower.count.Load() != 0 {
		t.Fatalf("cached call hit follower %d times", follower.count.Load())
	}
	if leader.count.Load() != 1 {
		t.Fatalf("leader hit %d times, want 1", leader.count.Load())
	}
}

// TestClusterExhaustsBudget: every member redirects in a cycle; the
// client should surface ErrTooManyRedirects within the configured attempts,
// preserve the final redirect, and not loop forever.
func TestClusterExhaustsBudget(t *testing.T) {
	a := &pipeFakeServer{respond: always("error_not_leader 127.0.0.1:9002")}
	b := &pipeFakeServer{respond: always("error_not_leader 127.0.0.1:9001")}
	cl, _ := NewCluster([]string{"127.0.0.1:9001", "127.0.0.1:9002"},
		WithClusterRedirectBudget(3),
		withClusterDial(dialerFor(t, map[string]*pipeFakeServer{
			"127.0.0.1:9001": a,
			"127.0.0.1:9002": b,
		})))
	_, _, err := cl.Acquire(context.Background(), "k", 0)
	if !errors.Is(err, ErrTooManyRedirects) {
		t.Fatalf("err = %v, want ErrTooManyRedirects", err)
	}
	var nle *NotLeaderError
	if !errors.As(err, &nle) || nle.Leader != "127.0.0.1:9002" {
		t.Fatalf("err = %v, want wrapped final NotLeaderError for 127.0.0.1:9002", err)
	}
	if got := a.count.Load() + b.count.Load(); got != 3 {
		t.Fatalf("attempts = %d, want budget 3", got)
	}
}

func TestClusterFollowsRedirectTargetImmediately(t *testing.T) {
	a := &pipeFakeServer{respond: always("error_not_leader 127.0.0.1:9003")}
	b := &pipeFakeServer{respond: always(validTokenLine)}
	c := &pipeFakeServer{respond: always(validTokenLine)}
	cl, _ := NewCluster([]string{"127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"},
		withClusterDial(dialerFor(t, map[string]*pipeFakeServer{
			"127.0.0.1:9001": a,
			"127.0.0.1:9002": b,
			"127.0.0.1:9003": c,
		})))

	if _, _, err := cl.Acquire(context.Background(), "k", 0); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if b.count.Load() != 0 || c.count.Load() != 1 {
		t.Fatalf("redirect hit intermediate=%d target=%d, want 0,1", b.count.Load(), c.count.Load())
	}
}

func TestClusterRetriesTransportFailureOnAnotherMember(t *testing.T) {
	leader := &pipeFakeServer{respond: always(validTokenLine)}
	var first atomic.Bool
	cl, err := NewCluster(
		[]string{"127.0.0.1:9001", "127.0.0.1:9002"},
		withClusterDial(func(_ context.Context, addr string) (*Conn, error) {
			if addr == "127.0.0.1:9001" && !first.Swap(true) {
				return newConnClosingOnFirstOperation(t), nil
			}
			if addr == "127.0.0.1:9002" {
				return newFakeConn(t, leader), nil
			}
			return nil, errors.New("unexpected dial " + addr)
		}),
	)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	token, _, err := cl.Acquire(context.Background(), "k", 0)
	if err != nil || token == "" {
		t.Fatalf("Acquire after transport failure = %q, %v", token, err)
	}
	if got := cl.LeaderHint(); got != "127.0.0.1:9002" {
		t.Fatalf("LeaderHint = %q, want second member", got)
	}
}

func TestClusterDialFailuresRotateFromCachedLeaderAndWrapCause(t *testing.T) {
	dialErr := errors.New("connection refused")
	var attempts []string
	cl, _ := NewCluster(
		[]string{"127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"},
		WithClusterRedirectBudget(3),
		withClusterDial(func(_ context.Context, addr string) (*Conn, error) {
			attempts = append(attempts, addr)
			return nil, dialErr
		}),
	)
	cl.updateLeaderHint("127.0.0.1:9002")

	_, _, err := cl.Acquire(context.Background(), "k", 0)
	if !errors.Is(err, ErrTooManyRedirects) || !errors.Is(err, dialErr) {
		t.Fatalf("err = %v, want ErrTooManyRedirects wrapping dial error", err)
	}
	got := strings.Join(attempts, ",")
	want := "127.0.0.1:9002,127.0.0.1:9003,127.0.0.1:9001"
	if got != want {
		t.Fatalf("dial order = %q, want %q", got, want)
	}
	if !strings.Contains(err.Error(), "127.0.0.1:9001") {
		t.Fatalf("err = %v, want final attempted address", err)
	}
}

// TestClusterRespectsContextCancellation: a cancelled context should
// short-circuit the retry loop before another dial attempt.
func TestClusterRespectsContextCancellation(t *testing.T) {
	srv := &pipeFakeServer{respond: always("error_not_leader 127.0.0.1:9001")}
	cl, _ := NewCluster([]string{"127.0.0.1:9001"},
		WithClusterRedirectBudget(100),
		withClusterDial(dialerFor(t, map[string]*pipeFakeServer{"127.0.0.1:9001": srv})))
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already done
	_, _, err := cl.Acquire(ctx, "k", 0)
	if err == nil {
		t.Fatalf("Acquire with cancelled ctx: want error, got nil")
	}
	if !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "canceled") {
		t.Fatalf("err = %v, want context.Canceled or similar", err)
	}
}

func TestClusterCancellationClosesBlockedOperation(t *testing.T) {
	entered := make(chan struct{})
	unblock := make(chan struct{})
	srv := &pipeFakeServer{respond: func(_, _, _ string) string {
		close(entered)
		<-unblock
		return validTokenLine
	}}
	cl, _ := NewCluster([]string{"127.0.0.1:9001"},
		withClusterDial(dialerFor(t, map[string]*pipeFakeServer{"127.0.0.1:9001": srv})))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := cl.Acquire(ctx, "k", time.Second)
		done <- err
	}()
	<-entered
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Acquire error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked operation did not stop after context cancellation")
	}
	close(unblock)
}

func TestClusterCancellationStopsBlockedDial(t *testing.T) {
	entered := make(chan struct{})
	cl, _ := NewCluster([]string{"127.0.0.1:9001"},
		withClusterDial(func(ctx context.Context, _ string) (*Conn, error) {
			close(entered)
			<-ctx.Done()
			return nil, ctx.Err()
		}))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := cl.Acquire(ctx, "k", time.Second)
		done <- err
	}()
	<-entered
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Acquire error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked dial did not stop after context cancellation")
	}
}

// TestClusterReleasePassesThrough: a successful Release on the leader
// returns nil; the response line is "ok".
func TestClusterReleasePassesThrough(t *testing.T) {
	srv := &pipeFakeServer{respond: always("ok")}
	cl, _ := NewCluster([]string{"127.0.0.1:9001"},
		withClusterDial(dialerFor(t, map[string]*pipeFakeServer{"127.0.0.1:9001": srv})))
	if err := cl.Release(context.Background(), "k", "0123456789abcdef0123456789abcdef"); err != nil {
		t.Fatalf("Release: %v", err)
	}
}

// TestClusterBarrierFollowsRedirect: Barrier is a mutating-ish call
// (it proposes through Raft), so it must follow not_leader redirects.
func TestClusterBarrierFollowsRedirect(t *testing.T) {
	follower := &pipeFakeServer{respond: always("error_not_leader 127.0.0.1:9002")}
	leader := &pipeFakeServer{respond: always("ok")}
	cl, _ := NewCluster([]string{"127.0.0.1:9001", "127.0.0.1:9002"},
		withClusterDial(dialerFor(t, map[string]*pipeFakeServer{
			"127.0.0.1:9001": follower,
			"127.0.0.1:9002": leader,
		})))
	if err := cl.Barrier(context.Background()); err != nil {
		t.Fatalf("Barrier: %v", err)
	}
}

// TestClusterIgnoresUnknownLeaderHint: a server returning
// `error_not_leader <evil.example.com:6388>` (an address not in the
// operator-supplied members list) must NOT cause the client to dial
// that address. The client clears its cache and keeps rotating
// through known members.
func TestClusterIgnoresUnknownLeaderHint(t *testing.T) {
	known := &pipeFakeServer{respond: always("error_not_leader 8.8.8.8:9999")}
	evil := &pipeFakeServer{respond: always(validTokenLine)}
	mp := map[string]*pipeFakeServer{
		"127.0.0.1:9001": known,
		"127.0.0.1:9002": known,
		"8.8.8.8:9999":   evil, // attacker-named address; must not be dialed
	}
	cl, _ := NewCluster([]string{"127.0.0.1:9001", "127.0.0.1:9002"},
		WithClusterRedirectBudget(5),
		withClusterDial(dialerFor(t, mp)))
	_, _, err := cl.Acquire(context.Background(), "k", 0)
	if !errors.Is(err, ErrTooManyRedirects) {
		t.Fatalf("err = %v, want ErrTooManyRedirects (client followed an unknown leader hint)", err)
	}
	if got := cl.LeaderHint(); got == "8.8.8.8:9999" {
		t.Fatalf("client cached untrusted leader hint: %q", got)
	}
	if evil.count.Load() != 0 {
		t.Fatalf("client dialed unknown leader hint %d times", evil.count.Load())
	}
}

// TestClusterIsConcurrencySafe: two goroutines hitting the same
// Cluster object should both succeed. Smoke test, not exhaustive.
func TestClusterIsConcurrencySafe(t *testing.T) {
	srv := &pipeFakeServer{respond: always(validTokenLine)}
	cl, _ := NewCluster([]string{"127.0.0.1:9001"},
		withClusterDial(dialerFor(t, map[string]*pipeFakeServer{"127.0.0.1:9001": srv})))
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := cl.Acquire(context.Background(), "k", 0)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Acquire: %v", err)
		}
	}
}
