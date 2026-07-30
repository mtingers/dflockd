package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/raft"
	"github.com/mtingers/dflockd/internal/server"
)

// runSingleNodeChildEnv makes the test binary re-exec itself as a real dflockd
// process (single-node, on the given port) so startup wiring is exercised
// end-to-end rather than through in-process helpers.
const runSingleNodeChildEnv = "DFLOCKD_TEST_RUN_SINGLE_NODE_PORT"

func TestMain(m *testing.M) {
	if port := os.Getenv(runSingleNodeChildEnv); port != "" {
		cfg, err := config.Load([]string{"--host", "127.0.0.1", "--port", port, "--http-port", "0"})
		if err != nil {
			fmt.Fprintf(os.Stderr, "child config: %v\n", err)
			os.Exit(2)
		}
		os.Exit(run(cfg))
	}
	os.Exit(m.Run())
}

type fakeClusterRuntime struct {
	done chan struct{}
	err  error
}

func (f *fakeClusterRuntime) Done() <-chan struct{} { return f.done }
func (f *fakeClusterRuntime) Err() error            { return f.err }

func TestClusterClockFromEnv(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	t.Setenv(unsafeTestClockOffsetEnv, "2h")
	now, err := clusterClockFromEnv(log)
	if err != nil {
		t.Fatalf("clusterClockFromEnv: %v", err)
	}
	offset := time.Until(now())
	if offset < 2*time.Hour-time.Second || offset > 2*time.Hour+time.Second {
		t.Fatalf("clock offset = %s, want about 2h", offset)
	}
}

func TestClusterClockFromEnvRejectsInvalidOrExtremeOffset(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, value := range []string{"later", "25h", "-25h"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv(unsafeTestClockOffsetEnv, value)
			if _, err := clusterClockFromEnv(log); err == nil {
				t.Fatalf("offset %q accepted", value)
			}
		})
	}
}

func TestSuperviseClusterNodeReportsFatalAndUnexpectedStop(t *testing.T) {
	fatal := errors.New("injected raft failure")
	node := &fakeClusterRuntime{done: make(chan struct{}), err: fatal}
	close(node.done)
	if err := superviseClusterNode(context.Background(), node); !errors.Is(err, fatal) {
		t.Fatalf("fatal supervision error = %v, want %v", err, fatal)
	}

	node = &fakeClusterRuntime{done: make(chan struct{})}
	close(node.done)
	if err := superviseClusterNode(context.Background(), node); err == nil {
		t.Fatal("unexpected clean node stop was accepted")
	}
}

func TestSuperviseClusterNodeTreatsProcessCancellationAsClean(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	node := &fakeClusterRuntime{done: make(chan struct{})}
	if err := superviseClusterNode(ctx, node); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled supervision error = %v", err)
	}
}

func TestMembersFromConfigHonorsSingleNodeBootstrap(t *testing.T) {
	cfg := &config.Config{
		NodeID:           "n1",
		ClusterBootstrap: true,
		ClusterPeers: []config.ClusterPeer{
			{NodeID: "n1", RaftAddr: "r1", ClientAddr: "c1"},
			{NodeID: "n2", RaftAddr: "r2", ClientAddr: "c2"},
		},
	}
	got := membersFromConfig(cfg)
	if len(got) != 1 || got[raft.NodeID("n1")].ClientAddr != "c1" {
		t.Fatalf("bootstrap members = %+v, want only n1", got)
	}

	cfg.ClusterBootstrap = false
	got = membersFromConfig(cfg)
	if len(got) != 2 || got[raft.NodeID("n2")].RaftAddr != "r2" {
		t.Fatalf("static members = %+v, want n1+n2", got)
	}
}

// A single-node process has no cluster.Node. Boxing that nil pointer directly
// into a clusterRuntime would make runAll supervise an absent node and panic
// on the first Done() call, crashing every non-cluster process at startup.
func TestClusterRuntimeOfNilNodeIsNilInterface(t *testing.T) {
	if got := clusterRuntimeOf(nil); got != nil {
		t.Fatalf("clusterRuntimeOf(nil) = %#v, want a nil interface", got)
	}
}

// End-to-end guard for the single-node startup path: run() must wire the
// supervisor from a real *cluster.Node or not at all. A typed-nil boxed into
// clusterRuntime panics here and nowhere else in the test suite.
func TestRunSingleNodeStartsAndShutsDownCleanly(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("executable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, "-test.run=TestRunSingleNodeStartsAndShutsDownCleanly")
	cmd.Env = append(os.Environ(), runSingleNodeChildEnv+"="+strconv.Itoa(port))
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}

	deadline := time.Now().Add(15 * time.Second)
	var dialErr error
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			dialErr = nil
			break
		}
		dialErr = err
		time.Sleep(100 * time.Millisecond)
	}
	if dialErr != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("single-node server never accepted connections: %v\nchild output:\n%s", dialErr, out.String())
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal child: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("child exited %v\nchild output:\n%s", err, out.String())
	}
	if strings.Contains(out.String(), "panic:") {
		t.Fatalf("child panicked:\n%s", out.String())
	}
}

func TestRunAllWithoutClusterNodeDoesNotSupervise(t *testing.T) {
	cfg := &config.Config{
		Host: "127.0.0.1", Port: 0, HTTPPort: 0,
		MaxLocks: 8, DefaultLeaseTTL: time.Minute,
		LeaseSweepInterval: time.Second, GCInterval: time.Second,
		GCMaxIdleTime: time.Minute,
		ReadTimeout:   time.Second, WriteTimeout: time.Second,
		ShutdownTimeout: time.Second,
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	lm, err := lock.NewLockManager(cfg, log)
	if err != nil {
		t.Fatalf("NewLockManager: %v", err)
	}
	defer lm.Close()
	srv := server.New(lm, cfg, log)

	ctx, cancel := context.WithCancel(context.Background())
	rc := make(chan int, 1)
	go func() { rc <- runAll(ctx, srv, cfg, log, cancel, clusterRuntimeOf(nil)) }()
	time.Sleep(150 * time.Millisecond)
	cancel()
	select {
	case code := <-rc:
		if code != 0 {
			t.Fatalf("runAll exit code = %d, want 0", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runAll did not return after cancellation")
	}
}
