package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/httpapi"
	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/replication"
	"github.com/mtingers/dflockd/internal/server"
)

var version = "dev"

// snapshotAdapter bridges *lock.LockManager.Snapshot() (returns
// []lock.SnapshotEntry) to replication.Snapshotter (expects
// []replication.SnapshotEntry). The two types carry the same
// information; the adapter just translates field-by-field so the
// replication package doesn't need to import lock.
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
					Token:              h.Token,
					ConnID:             h.ConnID,
					LeaseExpiresUnixNS: h.LeaseExpiresUnixNS,
				}
			}
		}
		if len(e.Enqueued) > 0 {
			out[i].Enqueued = make([]replication.SnapshotEnqueued, len(e.Enqueued))
			for j, q := range e.Enqueued {
				out[i].Enqueued[j] = replication.SnapshotEnqueued{
					ConnID:     q.ConnID,
					Token:      q.Token,
					LeaseTTLNS: q.LeaseTTLNS,
				}
			}
		}
	}
	return out
}

func main() {
	cfg, err := config.Load(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		os.Exit(1)
	}

	if cfg.Version {
		fmt.Println(version)
		os.Exit(0)
	}

	logLevel := slog.LevelInfo
	if cfg.Debug {
		logLevel = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLevel,
	}))

	lm := lock.NewLockManager(cfg, log)
	srv := server.New(lm, cfg, log)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// SIGUSR1 promotes a replication secondary to primary in-place.
	// One-way (no demote). For containerised deployments use the
	// admin HTTP endpoint instead — same semantics.
	promoteCh := make(chan os.Signal, 1)
	signal.Notify(promoteCh, syscall.SIGUSR1)
	go func() {
		for range promoteCh {
			if err := srv.Promote(); err != nil {
				log.Error("promote signal: failed", "err", err)
				continue
			}
			log.Warn("promote signal: secondary promoted to primary")
		}
	}()

	// Witness role: a tiny daemon that participates in auto-failover.
	// Holds no lock state, accepts no client traffic. Just listens
	// for primary/secondary connections, tracks heartbeats, and
	// answers liveness queries.
	if cfg.ReplicationRole == "witness" {
		ws := replication.NewWitnessServer(log.With("component", "witness"))
		if err := ws.Start(ctx, cfg.ReplicationListenAddr, nil); err != nil {
			log.Error("witness: failed to start", "err", err)
			os.Exit(1)
		}
		defer ws.Stop()
		log.Info("witness: ready", "addr", ws.Addr())
		<-ctx.Done()
		return
	}

	// Replication is opt-in. When enabled, the replicator owns the
	// peer link and is installed as the lock manager's mutation hook.
	// On the secondary, the server is also told to refuse client
	// mutations.
	var rep *replication.Replicator
	if cfg.ReplicationRole != "" {
		nodeID := cfg.ReplicationNodeID
		if nodeID == "" {
			nodeID = net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
		}
		rep = replication.NewReplicator(replication.Config{
			Role:        replication.Role(cfg.ReplicationRole),
			NodeID:      nodeID,
			PeerAddr:    cfg.ReplicationPeerAddr,
			ListenAddr:  cfg.ReplicationListenAddr,
			MaxPause:    cfg.ReplicationMaxPause,
			Apply:       lm, // used by secondary; harmless on primary
			Snapshotter: snapshotAdapter{lm},
			WitnessAddr: cfg.ReplicationWitnessAddr,
			Log:         log.With("component", "replication"),
		})
		if err := rep.Start(ctx); err != nil {
			log.Error("replication: failed to start", "err", err)
			os.Exit(1)
		}
		defer rep.Stop()
		// The primary publishes all state mutations to the replicator
		// so they reach the secondary; the secondary doesn't (its
		// mutations come from the wire).
		if cfg.ReplicationRole == "primary" {
			lm.SetReplicationHook(rep)
		}
		srv.SetReplicator(rep)
	}

	errCh := make(chan error, 2)
	runners := 1
	go func() {
		if err := srv.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("tcp server: %w", err)
			cancel() // cascade shutdown to the HTTP server
			return
		}
		errCh <- nil
	}()

	if cfg.HTTPPort > 0 {
		runners++
		go func() {
			if err := httpapi.Run(ctx, srv, cfg, log); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- fmt.Errorf("http server: %w", err)
				cancel() // cascade shutdown to the TCP server
				return
			}
			errCh <- nil
		}()
	}

	var failed bool
	for i := 0; i < runners; i++ {
		if err := <-errCh; err != nil {
			log.Error("server error", "err", err)
			failed = true
		}
	}
	if failed {
		os.Exit(1)
	}
}
