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

	"github.com/mtingers/dflockd/internal/cluster"
	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/httpapi"
	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/raft"
	"github.com/mtingers/dflockd/internal/server"
)

// version is set by the release tooling via -ldflags="-X main.version=...".
var version = "dev"

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
	if cfg.IsCluster() {
		closer, err := startCluster(cfg, lm, srv, log)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cluster init failed: %v\n", err)
			return 1
		}
		defer closer()
	}
	return runAll(ctx, srv, cfg, log, cancel)
}

// startCluster opens persistent Raft state, the inter-node transport,
// and a cluster.Node bound to this lock manager and server. Returns a
// closer that tears everything down in reverse.
func startCluster(cfg *config.Config, lm *lock.LockManager, srv *server.Server, log *slog.Logger) (func(), error) {
	storage, err := raft.OpenFileStorage(cfg.RaftDir)
	if err != nil {
		return nil, fmt.Errorf("open raft storage at %s: %w", cfg.RaftDir, err)
	}
	transport, err := raft.NewTCPTransport(raft.NodeID(cfg.NodeID), cfg.RaftAddr, log)
	if err != nil {
		_ = storage.Close()
		return nil, fmt.Errorf("open raft transport on %s: %w", cfg.RaftAddr, err)
	}
	registerPeers(transport, cfg)
	node, err := buildClusterNode(cfg, lm, storage, transport, log)
	if err != nil {
		_ = transport.Close()
		_ = storage.Close()
		return nil, err
	}
	node.Start()
	srv.SetCluster(node)
	return clusterCloser(srv, node, transport, storage, log), nil
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
	ccfg := cluster.Config{Raft: rcfg, Members: members, AdvertiseAddr: cfg.EffectiveAdvertiseAddr()}
	return cluster.NewNode(ccfg, lm, storage, transport, log)
}

func membersFromConfig(cfg *config.Config) map[raft.NodeID]cluster.Member {
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
func runAll(ctx context.Context, srv *server.Server, cfg *config.Config, log *slog.Logger, cancel context.CancelFunc) int {
	var wg sync.WaitGroup
	var failed atomic.Bool
	runOne(&wg, "tcp server", &failed, cancel, log, func() error { return srv.Run(ctx) })
	if cfg.HTTPPort > 0 {
		runOne(&wg, "http server", &failed, cancel, log, func() error { return httpapi.Run(ctx, srv, cfg, log) })
	}
	wg.Wait()
	if failed.Load() {
		return 1
	}
	return 0
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
