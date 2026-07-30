// Command dflockd is the distributed FIFO lock server.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/mtingers/dflockd/internal/cluster"
	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/httpapi"
	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/raft"
	"github.com/mtingers/dflockd/internal/server"
)

// version is set by the release tooling via -ldflags="-X main.version=...".
var version = "dev"

const (
	unsafeTestClockOffsetEnv = "DFLOCKD_UNSAFE_TEST_CLOCK_OFFSET"
	maxTestClockOffset       = 24 * time.Hour
)

func main() {
	cfg := mustLoadConfig()
	if cfg.Version {
		fmt.Println(version)
		return
	}
	os.Exit(run(cfg))
}

// mustLoadConfig parses argv or exits with a friendly error.
func mustLoadConfig() *config.Config {
	cfg, err := config.Load(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		os.Exit(1)
	}
	return cfg
}

// run wires up the lock manager + servers, blocks until shutdown,
// and returns the process exit code.
func run(cfg *config.Config) int {
	log := newLogger(cfg.Debug)
	if unbounded := cfg.UnboundedLimitWarnings(); len(unbounded) > 0 {
		log.Warn("running without one or more resource limits set; not recommended when reachable by untrusted clients",
			"unset", strings.Join(unbounded, " "))
	}
	lm, err := lock.NewLockManager(cfg, log)
	if err != nil {
		fmt.Fprintf(os.Stderr, "lock manager init failed: %v\n", err)
		return 1
	}
	defer lm.Close()
	srv := server.New(lm, cfg, log)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	var clusterNode *cluster.Node
	if cfg.IsCluster() {
		closer, node, err := startCluster(cfg, lm, srv, log)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cluster init failed: %v\n", err)
			return 1
		}
		defer closer()
		clusterNode = node
	}
	return runAll(ctx, srv, cfg, log, cancel, clusterRuntimeOf(clusterNode))
}

// startCluster opens persistent Raft state, the inter-node transport,
// and a cluster.Node bound to this lock manager and server. Returns a
// closer that tears everything down in reverse.
func startCluster(cfg *config.Config, lm *lock.LockManager, srv *server.Server, log *slog.Logger) (func(), *cluster.Node, error) {
	storage, err := raft.OpenFileStorage(cfg.RaftDir)
	if err != nil {
		return nil, nil, fmt.Errorf("open raft storage at %s: %w", cfg.RaftDir, err)
	}
	tlsCfg, err := raft.NewMutualTLSConfig(cfg.RaftTLSCert, cfg.RaftTLSKey, cfg.RaftTLSCA)
	if err != nil {
		_ = storage.Close()
		return nil, nil, err
	}
	if tlsCfg != nil {
		log.Info("raft transport: shared-secret encryption + mutual TLS enabled")
	} else {
		log.Info("raft transport: shared-secret authentication and encryption enabled")
	}
	transport, err := raft.NewTCPTransport(
		raft.NodeID(cfg.NodeID), cfg.RaftAddr, log,
		raft.WithClusterSecret(cfg.RaftAuthToken), raft.WithTLS(tlsCfg),
	)
	if err != nil {
		_ = storage.Close()
		return nil, nil, fmt.Errorf("open raft transport on %s: %w", cfg.RaftAddr, err)
	}
	registerPeers(transport, cfg)
	node, err := buildClusterNode(cfg, lm, storage, transport, log)
	if err != nil {
		_ = transport.Close()
		_ = storage.Close()
		return nil, nil, err
	}
	if err := node.Start(); err != nil {
		_ = transport.Close()
		_ = storage.Close()
		return nil, nil, fmt.Errorf("start cluster node: %w", err)
	}
	srv.SetCluster(node)
	return clusterCloser(srv, node, transport, storage, log), node, nil
}

func registerPeers(transport *raft.TCPTransport, cfg *config.Config) {
	for _, p := range cfg.ClusterPeers {
		if p.NodeID == cfg.NodeID {
			continue
		}
		transport.AddPeer(raft.NodeID(p.NodeID), p.RaftAddr)
	}
}

func buildClusterNode(cfg *config.Config, lm *lock.LockManager, storage raft.Storage, transport raft.Transport, log *slog.Logger) (*cluster.Node, error) {
	members := membersFromConfig(cfg)
	rcfg := raft.DefaultConfig()
	rcfg.ID = raft.NodeID(cfg.NodeID)
	now, err := clusterClockFromEnv(log)
	if err != nil {
		return nil, err
	}
	ccfg := cluster.Config{
		Raft: rcfg, Members: members,
		AdvertiseAddr: cfg.EffectiveAdvertiseAddr(),
		SweepInterval: cfg.LeaseSweepInterval,
		Now:           now,
	}
	return cluster.NewNode(ccfg, lm, storage, transport, log)
}

func clusterClockFromEnv(log *slog.Logger) (func() time.Time, error) {
	raw := os.Getenv(unsafeTestClockOffsetEnv)
	if raw == "" {
		return time.Now, nil
	}
	offset, err := time.ParseDuration(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", unsafeTestClockOffsetEnv, err)
	}
	if offset < -maxTestClockOffset || offset > maxTestClockOffset {
		return nil, fmt.Errorf("%s must be within +/- %s", unsafeTestClockOffsetEnv, maxTestClockOffset)
	}
	log.Warn("UNSAFE fault-injection clock offset enabled",
		"env", unsafeTestClockOffsetEnv, "offset", offset)
	return func() time.Time { return time.Now().Add(offset) }, nil
}

func membersFromConfig(cfg *config.Config) map[raft.NodeID]cluster.Member {
	if cfg.ClusterBootstrap {
		for _, p := range cfg.ClusterPeers {
			if p.NodeID == cfg.NodeID {
				return map[raft.NodeID]cluster.Member{
					raft.NodeID(p.NodeID): {RaftAddr: p.RaftAddr, ClientAddr: p.ClientAddr},
				}
			}
		}
	}
	out := make(map[raft.NodeID]cluster.Member, len(cfg.ClusterPeers))
	for _, p := range cfg.ClusterPeers {
		out[raft.NodeID(p.NodeID)] = cluster.Member{RaftAddr: p.RaftAddr, ClientAddr: p.ClientAddr}
	}
	return out
}

// clusterCloser tears down the cluster in reverse order: unwire the
// server first (so no new proposes flow), close the node (joins its
// goroutines), the transport, then the storage.
func clusterCloser(srv *server.Server, node *cluster.Node, transport *raft.TCPTransport, storage raft.Storage, log *slog.Logger) func() {
	return func() {
		srv.SetCluster(nil)
		if err := node.Close(); err != nil {
			log.Warn("cluster node close", "err", err)
		}
		_ = transport.Close()
		_ = storage.Close()
	}
}

func newLogger(debug bool) *slog.Logger {
	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
}

// runAll spawns the TCP server and (optionally) the HTTP server,
// waits for both to exit, and returns 0 on clean shutdown / 1 on
// any runner error.
func runAll(ctx context.Context, srv *server.Server, cfg *config.Config, log *slog.Logger, cancel context.CancelFunc, clusterNode clusterRuntime) int {
	var wg sync.WaitGroup
	var failed atomic.Bool
	runOne(&wg, "tcp server", &failed, cancel, log, func() error { return srv.Run(ctx) })
	if cfg.HTTPPort > 0 {
		runOne(&wg, "http server", &failed, cancel, log, func() error { return httpapi.Run(ctx, srv, cfg, log) })
	}
	if clusterNode != nil {
		runOne(&wg, "raft node", &failed, cancel, log, func() error {
			return superviseClusterNode(ctx, clusterNode)
		})
	}
	wg.Wait()
	if failed.Load() {
		return 1
	}
	return 0
}

type clusterRuntime interface {
	Done() <-chan struct{}
	Err() error
}

// clusterRuntimeOf boxes node only when one exists. Assigning a nil
// *cluster.Node straight into a clusterRuntime yields a NON-nil interface
// holding a nil pointer, so runAll would supervise an absent node and
// superviseClusterNode would nil-dereference on the first Done() call —
// crashing every single-node process at startup.
func clusterRuntimeOf(node *cluster.Node) clusterRuntime {
	if node == nil {
		return nil
	}
	return node
}

func superviseClusterNode(ctx context.Context, node clusterRuntime) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-node.Done():
		if err := node.Err(); err != nil {
			return err
		}
		return fmt.Errorf("raft node stopped unexpectedly")
	}
}

// runOne launches one server goroutine. Real errors are logged and
// cancel the parent ctx so the sibling exits promptly.
func runOne(wg *sync.WaitGroup, name string, failed *atomic.Bool, cancel context.CancelFunc, log *slog.Logger, fn func() error) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		recordRunnerErr(fn(), name, failed, cancel, log)
	}()
}

// recordRunnerErr classifies a runner's return value. Context
// cancellation is treated as clean shutdown.
func recordRunnerErr(err error, name string, failed *atomic.Bool, cancel context.CancelFunc, log *slog.Logger) {
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	log.Error("server error", "err", fmt.Errorf("%s: %w", name, err))
	failed.Store(true)
	cancel()
}
