// Package server implements the line-based TCP server. It owns the
// accept loop, per-connection IP/total caps, auth handshake, and
// dispatch into the LockManager. Background lock-manager loops are
// spawned here so the server's lifecycle owns them too.
package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/lock"
)

// Server accepts TCP connections and dispatches requests to the
// LockManager. One Server per process.
type Server struct {
	lm        *lock.LockManager
	cfg       *config.Config
	log       *slog.Logger
	connSeq   atomic.Uint64
	connCount atomic.Int64
	conns     sync.Map // net.Conn → struct{}
}

// New constructs a Server wrapping lm. The Server does not start any
// background work until Run (or RunOnListener) is called.
func New(lm *lock.LockManager, cfg *config.Config, log *slog.Logger) *Server {
	return &Server{lm: lm, cfg: cfg, log: log}
}

// LockManager exposes the underlying LockManager (used by the HTTP API).
func (s *Server) LockManager() *lock.LockManager { return s.lm }

// Config exposes the server config (used by the HTTP API).
func (s *Server) Config() *config.Config { return s.cfg }

// NextConnID allocates a fresh connection ID. The HTTP API uses this to
// give every session a connID that's unique across both transports so
// CleanupConnection doesn't collide.
func (s *Server) NextConnID() uint64 { return s.connSeq.Add(1) }

// ConnCount returns the number of currently-active TCP connections.
func (s *Server) ConnCount() int64 { return s.connCount.Load() }

// Run starts the listener on the configured host:port and blocks until
// ctx is cancelled. Returns nil on clean shutdown.
func (s *Server) Run(ctx context.Context) error {
	if (s.cfg.TLSCert != "") != (s.cfg.TLSKey != "") {
		return fmt.Errorf("both --tls-cert and --tls-key must be provided together")
	}
	addr := net.JoinHostPort(s.cfg.Host, strconv.Itoa(s.cfg.Port))
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	if s.cfg.TLSCert != "" {
		cert, err := tls.LoadX509KeyPair(s.cfg.TLSCert, s.cfg.TLSKey)
		if err != nil {
			listener.Close()
			return fmt.Errorf("tls: %w", err)
		}
		listener = tls.NewListener(listener, &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		})
		s.log.Info("TLS enabled")
	}
	s.log.Info("listening", "addr", addr)
	return s.serve(ctx, listener)
}

// RunOnListener serves on a pre-existing listener. Used by tests so
// they can pick a free port and bypass the TLS/host config.
func (s *Server) RunOnListener(ctx context.Context, listener net.Listener) error {
	s.log.Info("listening", "addr", listener.Addr())
	return s.serve(ctx, listener)
}

func (s *Server) serve(ctx context.Context, listener net.Listener) error {
	serveCtx, stop := context.WithCancel(ctx)
	defer stop()

	var wg sync.WaitGroup
	ipTracker := newIPTracker(s.cfg.MaxConnectionsPerIP)

	// Background lock-manager loops — owned by this Run.
	wg.Add(2)
	go func() { defer wg.Done(); s.lm.LeaseExpiryLoop(serveCtx) }()
	go func() { defer wg.Done(); s.lm.GCLoop(serveCtx) }()

	// Cancellation closes the listener so Accept returns.
	go func() {
		<-serveCtx.Done()
		listener.Close()
	}()

	var backoff time.Duration
	const maxBackoff = 1 * time.Second

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-serveCtx.Done():
				s.drain(&wg)
				return nil
			default:
			}
			if !isTemporaryNetErr(err) {
				stop()
				s.drain(&wg)
				return fmt.Errorf("accept: %w", err)
			}
			// Exponential backoff on transient accept errors so a
			// persistent failure (FD exhaustion, etc.) doesn't busy-spin.
			if backoff == 0 {
				backoff = 5 * time.Millisecond
			} else {
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
			s.log.Error("accept error, backing off", "err", err, "backoff", backoff)
			select {
			case <-time.After(backoff):
			case <-serveCtx.Done():
				s.drain(&wg)
				return nil
			}
			continue
		}
		backoff = 0
		if max := s.cfg.MaxConnections; max > 0 && s.connCount.Load() >= int64(max) {
			s.log.Warn("max connections reached, rejecting", "max", max)
			conn.Close()
			continue
		}
		ip, ok := ipTracker.acquire(conn)
		if !ok {
			s.log.Warn("max connections per IP, rejecting", "ip", ip, "max", s.cfg.MaxConnectionsPerIP)
			conn.Close()
			continue
		}
		s.connCount.Add(1)
		s.conns.Store(conn, struct{}{})
		connID := s.NextConnID()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer s.connCount.Add(-1)
			defer s.conns.Delete(conn)
			defer ipTracker.release(ip)
			s.ServeConn(serveCtx, conn, connID)
		}()
	}
}

// drain blocks until all connection goroutines exit, force-closing
// connections after ShutdownTimeout if needed.
func (s *Server) drain(wg *sync.WaitGroup) {
	s.log.Info("shutting down, draining connections")
	if s.cfg.ShutdownTimeout <= 0 {
		wg.Wait()
		return
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		return
	case <-time.After(s.cfg.ShutdownTimeout):
		s.log.Warn("shutdown timeout reached, force-closing connections")
		s.conns.Range(func(key, _ any) bool {
			if c, ok := key.(net.Conn); ok {
				c.Close()
			}
			return true
		})
		wg.Wait()
	}
}

func isTemporaryNetErr(err error) bool {
	var ne net.Error
	if !errors.As(err, &ne) {
		return false
	}
	type temporary interface{ Temporary() bool }
	if te, ok := ne.(temporary); ok {
		return te.Temporary()
	}
	return false
}

// ipTracker enforces per-remote-IP connection caps. nil-safe and
// cap=0 means unlimited.
type ipTracker struct {
	max    int
	mu     sync.Mutex
	counts map[string]int
}

func newIPTracker(max int) *ipTracker {
	return &ipTracker{max: max, counts: make(map[string]int)}
}

func (t *ipTracker) acquire(conn net.Conn) (string, bool) {
	if t.max <= 0 {
		return "", true
	}
	ip := remoteIP(conn.RemoteAddr().String())
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.counts[ip] >= t.max {
		return ip, false
	}
	t.counts[ip]++
	return ip, true
}

func (t *ipTracker) release(ip string) {
	if ip == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.counts[ip]--
	if t.counts[ip] <= 0 {
		delete(t.counts, ip)
	}
}

func remoteIP(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err == nil {
		return host
	}
	return addr
}
