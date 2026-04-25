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
			Apply:       lm,
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
