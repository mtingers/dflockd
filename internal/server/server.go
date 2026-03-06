package server

import (
	"bufio"
	"context"
	"crypto/subtle"
	"crypto/tls"
	"encoding/json"
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
	"github.com/mtingers/dflockd/internal/protocol"
	"github.com/mtingers/dflockd/internal/signal"
)

type connState struct {
	id            uint64
	writeCh       chan []byte
	cancelConn    func()
	subscriptions int
}

type Server struct {
	lm        *lock.LockManager
	cfg       *config.Config
	log       *slog.Logger
	sig       *signal.Manager
	connSeq   atomic.Uint64
	connCount atomic.Int64
	conns     sync.Map // net.Conn → struct{}
}

func New(lm *lock.LockManager, cfg *config.Config, log *slog.Logger) *Server {
	return &Server{lm: lm, cfg: cfg, log: log, sig: signal.NewManager()}
}

func (s *Server) Run(ctx context.Context) error {
	hasCert := s.cfg.TLSCert != ""
	hasKey := s.cfg.TLSKey != ""
	if hasCert != hasKey {
		return fmt.Errorf("both --tls-cert and --tls-key must be provided together")
	}

	addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	if hasCert {
		cert, err := tls.LoadX509KeyPair(s.cfg.TLSCert, s.cfg.TLSKey)
		if err != nil {
			listener.Close()
			return fmt.Errorf("tls: %w", err)
		}
		tlsCfg := &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
		listener = tls.NewListener(listener, tlsCfg)
		s.log.Info("TLS enabled")
	}

	s.log.Info("listening", "addr", addr)
	return s.serve(ctx, listener)
}

// RunOnListener starts the server on a pre-existing listener (for testing).
func (s *Server) RunOnListener(ctx context.Context, listener net.Listener) error {
	s.log.Info("listening", "addr", listener.Addr())
	return s.serve(ctx, listener)
}

func (s *Server) serve(ctx context.Context, listener net.Listener) error {
	var wg sync.WaitGroup

	// Background loops
	wg.Add(2)
	go func() {
		defer wg.Done()
		s.lm.LeaseExpiryLoop(ctx)
	}()
	go func() {
		defer wg.Done()
		s.lm.GCLoop(ctx)
	}()

	// Close listener on context cancellation
	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				s.drain(&wg)
				return nil
			default:
				s.log.Error("accept error", "err", err)
				continue
			}
		}
		if max := s.cfg.MaxConnections; max > 0 && s.connCount.Load() >= int64(max) {
			s.log.Warn("max connections reached, rejecting", "max", max)
			conn.Close()
			continue
		}
		s.connCount.Add(1)
		s.conns.Store(conn, struct{}{})
		connID := s.connSeq.Add(1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.handleConn(ctx, conn, connID)
		}()
	}
}

// drain waits for all goroutines to finish, force-closing connections if the
// shutdown timeout expires.
func (s *Server) drain(wg *sync.WaitGroup) {
	s.log.Info("shutting down, draining connections")

	if s.cfg.ShutdownTimeout <= 0 {
		wg.Wait()
		return
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

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

func (s *Server) writeResponse(conn net.Conn, data []byte) error {
	if s.cfg.WriteTimeout > 0 {
		conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
	}
	_, err := conn.Write(data)
	if s.cfg.WriteTimeout > 0 && err == nil {
		conn.SetWriteDeadline(time.Time{})
	}
	return err
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn, connID uint64) {
	peer := conn.RemoteAddr().String()
	s.log.Debug("client connected", "peer", peer, "conn_id", connID)

	// Create a per-connection context that is cancelled when the server
	// shuts down, allowing in-progress lock waits to be interrupted.
	connCtx, connCancel := context.WithCancel(ctx)

	// writeCh is used by the signal push writer goroutine.
	writeCh := make(chan []byte, 64)
	var writeMu sync.Mutex

	cs := &connState{
		id:         connID,
		writeCh:    writeCh,
		cancelConn: connCancel,
	}

	// Push writer goroutine: drains writeCh and sends async messages
	// (signal notifications) to the client.
	var pushWg sync.WaitGroup
	pushWg.Add(1)
	go func() {
		defer pushWg.Done()
		for msg := range writeCh {
			writeMu.Lock()
			err := s.writeResponse(conn, msg)
			writeMu.Unlock()
			if err != nil {
				connCancel()
				// Drain remaining messages to unblock senders.
				for range writeCh {
				}
				return
			}
		}
	}()

	defer func() {
		connCancel()
		s.sig.UnlistenAll(connID)
		s.conns.Delete(conn)
		s.connCount.Add(-1)
		s.lm.CleanupConnection(connID)
		close(writeCh)
		pushWg.Wait()
		conn.Close()
		s.log.Debug("client closed", "peer", peer, "conn_id", connID)
	}()

	reader := bufio.NewReader(conn)
	defaultLeaseTTL := s.cfg.DefaultLeaseTTL
	defaultLeaseTTLSec := int(defaultLeaseTTL.Seconds())

	writeResp := func(ack *protocol.Ack) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return s.writeResponse(conn, protocol.FormatResponse(ack, defaultLeaseTTLSec))
	}

	if s.cfg.AuthToken != "" {
		req, err := protocol.ReadRequest(reader, s.cfg.ReadTimeout, conn, defaultLeaseTTL)
		if err != nil || req.Cmd != "auth" ||
			subtle.ConstantTimeCompare([]byte(req.Token), []byte(s.cfg.AuthToken)) != 1 {
			s.log.Warn("auth failed", "peer", peer, "conn_id", connID)
			writeResp(&protocol.Ack{Status: "error_auth"})
			// Small delay to slow down brute-force attempts.
			time.Sleep(100 * time.Millisecond)
			return
		}
		if err := writeResp(&protocol.Ack{Status: "ok"}); err != nil {
			s.log.Debug("write error during auth, disconnecting", "peer", peer, "err", err)
			return
		}
	}

	for {
		req, err := protocol.ReadRequest(reader, s.cfg.ReadTimeout, conn, defaultLeaseTTL)
		if err != nil {
			var pe *protocol.ProtocolError
			if errors.As(err, &pe) {
				if pe.Code == 11 {
					// Client disconnected
					break
				}
				s.log.Warn("protocol error", "peer", peer, "code", pe.Code, "msg", pe.Message)
				if err := writeResp(&protocol.Ack{Status: "error"}); err != nil {
					s.log.Debug("write error, disconnecting", "peer", peer, "err", err)
					break
				}
				// Read-level errors (timeout, line too long) may have
				// desynchronized the protocol stream — disconnect.
				// Parse-level errors are safe to continue from because
				// all three request lines were consumed.
				if pe.Code == 10 || pe.Code == 12 {
					break
				}
				continue
			}
			s.log.Error("read error", "peer", peer, "err", err)
			break
		}

		ack := s.handleRequest(connCtx, req, cs)
		if ack == nil {
			break
		}
		if err := writeResp(ack); err != nil {
			s.log.Debug("write error, disconnecting", "peer", peer, "err", err)
			break
		}
	}
}

func (s *Server) handleRequest(ctx context.Context, req *protocol.Request, cs *connState) *protocol.Ack {
	connID := cs.id
	s.log.Debug("request", "conn", connID, "cmd", req.Cmd, "key", req.Key)

	switch req.Cmd {
	case "stats":
		st := s.lm.Stats(s.connCount.Load())
		sigStats := s.sig.Stats()
		for _, si := range sigStats {
			st.SignalChannels = append(st.SignalChannels, lock.SignalChannelInfo{
				Pattern:   si.Pattern,
				Group:     si.Group,
				Listeners: si.Listeners,
			})
		}
		data, err := json.Marshal(st)
		if err != nil {
			return &protocol.Ack{Status: "error"}
		}
		return &protocol.Ack{Status: "ok", Extra: string(data)}

	case "l", "sl":
		limit := 1
		if req.Cmd == "sl" {
			limit = req.Limit
		}
		tok, err := s.lm.Acquire(ctx, req.Key, req.AcquireTimeout, req.LeaseTTL, connID, limit)
		if err != nil {
			if errors.Is(err, lock.ErrMaxLocks) {
				return &protocol.Ack{Status: "error_max_locks"}
			}
			if errors.Is(err, lock.ErrLimitMismatch) {
				return &protocol.Ack{Status: "error_limit_mismatch"}
			}
			if errors.Is(err, lock.ErrMaxWaiters) {
				return &protocol.Ack{Status: "error_max_waiters"}
			}
			if errors.Is(err, lock.ErrWaiterClosed) {
				s.log.Debug("waiter closed during acquire", "key", req.Key, "conn", connID)
				return &protocol.Ack{Status: "error"}
			}
			return &protocol.Ack{Status: "error"}
		}
		if tok == "" {
			return &protocol.Ack{Status: "timeout"}
		}
		return &protocol.Ack{Status: "ok", Token: tok, LeaseTTL: int(req.LeaseTTL.Seconds())}

	case "r", "sr":
		if s.lm.Release(req.Key, req.Token) {
			return &protocol.Ack{Status: "ok"}
		}
		return &protocol.Ack{Status: "error"}

	case "n", "sn":
		remaining, ok := s.lm.Renew(req.Key, req.Token, req.LeaseTTL)
		if !ok {
			return &protocol.Ack{Status: "error"}
		}
		return &protocol.Ack{Status: "ok", Extra: fmt.Sprintf("%d", remaining)}

	case "e", "se":
		limit := 1
		if req.Cmd == "se" {
			limit = req.Limit
		}
		status, tok, lease, err := s.lm.Enqueue(req.Key, req.LeaseTTL, connID, limit)
		if err != nil {
			if errors.Is(err, lock.ErrMaxLocks) {
				return &protocol.Ack{Status: "error_max_locks"}
			}
			if errors.Is(err, lock.ErrLimitMismatch) {
				return &protocol.Ack{Status: "error_limit_mismatch"}
			}
			if errors.Is(err, lock.ErrMaxWaiters) {
				return &protocol.Ack{Status: "error_max_waiters"}
			}
			if errors.Is(err, lock.ErrAlreadyEnqueued) {
				return &protocol.Ack{Status: "error_already_enqueued"}
			}
			return &protocol.Ack{Status: "error"}
		}
		return &protocol.Ack{Status: status, Token: tok, LeaseTTL: lease}

	case "w", "sw":
		tok, lease, err := s.lm.Wait(ctx, req.Key, req.AcquireTimeout, connID)
		if err != nil {
			if errors.Is(err, lock.ErrNotEnqueued) {
				return &protocol.Ack{Status: "error_not_enqueued"}
			}
			if errors.Is(err, lock.ErrLeaseExpired) {
				return &protocol.Ack{Status: "error_lease_expired"}
			}
			if errors.Is(err, lock.ErrWaiterClosed) {
				s.log.Debug("waiter closed during wait", "key", req.Key, "conn", connID)
				return &protocol.Ack{Status: "error"}
			}
			return &protocol.Ack{Status: "error"}
		}
		if tok == "" {
			return &protocol.Ack{Status: "timeout"}
		}
		return &protocol.Ack{Status: "ok", Token: tok, LeaseTTL: lease}

	case "listen":
		if max := s.cfg.MaxSubscriptions; max > 0 && cs.subscriptions >= max {
			return &protocol.Ack{Status: "error"}
		}
		listener := &signal.Listener{
			ConnID:     connID,
			Pattern:    req.Key,
			Group:      req.Group,
			WriteCh:    cs.writeCh,
			CancelConn: cs.cancelConn,
		}
		added, err := s.sig.Listen(listener)
		if err != nil {
			return &protocol.Ack{Status: "error"}
		}
		if added {
			cs.subscriptions++
		}
		return &protocol.Ack{Status: "ok"}

	case "unlisten":
		if s.sig.Unlisten(req.Key, connID, req.Group) {
			if cs.subscriptions > 0 {
				cs.subscriptions--
			}
		}
		return &protocol.Ack{Status: "ok"}

	case "signal":
		n := s.sig.Signal(req.Key, req.Value)
		return &protocol.Ack{Status: "ok", Extra: strconv.Itoa(n)}
	}

	s.log.Warn("unknown command in handleRequest", "cmd", req.Cmd, "conn", connID)
	return &protocol.Ack{Status: "error"}
}
