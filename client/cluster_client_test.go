package client

import (
	"bufio"
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
		srv.count.Add(1)
		resp := srv.respond(cmd, key, arg)
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
