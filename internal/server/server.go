package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"

	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/protocol"
)

type Server struct {
	lm      *lock.LockManager
	cfg     *config.Config
	log     *slog.Logger
	connSeq atomic.Uint64
}

func New(lm *lock.LockManager, cfg *config.Config, log *slog.Logger) *Server {
	return &Server{lm: lm, cfg: cfg, log: log}
}

func (s *Server) Run(ctx context.Context) error {
	addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	s.log.Info("listening", "addr", addr)

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
				wg.Wait()
				return nil
			default:
				s.log.Error("accept error", "err", err)
				continue
			}
		}
		connID := s.connSeq.Add(1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.handleConn(conn, connID)
		}()
	}
}

// RunOnListener starts the server on a pre-existing listener (for testing).
func (s *Server) RunOnListener(ctx context.Context, listener net.Listener) error {
	s.log.Info("listening", "addr", listener.Addr())

	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		s.lm.LeaseExpiryLoop(ctx)
	}()
	go func() {
		defer wg.Done()
		s.lm.GCLoop(ctx)
	}()

	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				wg.Wait()
				return nil
			default:
				s.log.Error("accept error", "err", err)
				continue
			}
		}
		connID := s.connSeq.Add(1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.handleConn(conn, connID)
		}()
	}
}

func (s *Server) handleConn(conn net.Conn, connID uint64) {
	peer := conn.RemoteAddr().String()
	s.log.Debug("client connected", "peer", peer, "conn_id", connID)

	defer func() {
		s.lm.CleanupConnection(connID)
		conn.Close()
		s.log.Debug("client closed", "peer", peer, "conn_id", connID)
	}()

	reader := bufio.NewReader(conn)
	defaultLeaseTTL := s.cfg.DefaultLeaseTTL
	defaultLeaseTTLSec := int(defaultLeaseTTL.Seconds())

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
				if _, err := conn.Write(protocol.FormatResponse(&protocol.Ack{Status: "error"}, defaultLeaseTTLSec)); err != nil {
				s.log.Debug("write error, disconnecting", "peer", peer, "err", err)
			}
				break
			}
			s.log.Error("read error", "peer", peer, "err", err)
			break
		}

		ack := s.handleRequest(req, connID)
		if _, err := conn.Write(protocol.FormatResponse(ack, defaultLeaseTTLSec)); err != nil {
			s.log.Debug("write error, disconnecting", "peer", peer, "err", err)
			break
		}
	}
}

func (s *Server) handleRequest(req *protocol.Request, connID uint64) *protocol.Ack {
	s.log.Debug("request", "conn", connID, "cmd", req.Cmd, "key", req.Key)

	switch req.Cmd {
	case "l":
		tok, err := s.lm.FIFOAcquire(req.Key, req.AcquireTimeout, req.LeaseTTL, connID)
		if err != nil {
			if errors.Is(err, lock.ErrMaxLocks) {
				return &protocol.Ack{Status: "error_max_locks"}
			}
			return &protocol.Ack{Status: "error"}
		}
		if tok == "" {
			return &protocol.Ack{Status: "timeout"}
		}
		return &protocol.Ack{Status: "ok", Token: tok, LeaseTTL: int(req.LeaseTTL.Seconds())}

	case "r":
		if s.lm.FIFORelease(req.Key, req.Token) {
			return &protocol.Ack{Status: "ok"}
		}
		return &protocol.Ack{Status: "error"}

	case "n":
		remaining, ok := s.lm.FIFORenew(req.Key, req.Token, req.LeaseTTL)
		if !ok {
			return &protocol.Ack{Status: "error"}
		}
		return &protocol.Ack{Status: "ok", Extra: fmt.Sprintf("%d", remaining)}

	case "e":
		status, tok, lease, err := s.lm.FIFOEnqueue(req.Key, req.LeaseTTL, connID)
		if err != nil {
			if errors.Is(err, lock.ErrMaxLocks) {
				return &protocol.Ack{Status: "error_max_locks"}
			}
			return &protocol.Ack{Status: "error"}
		}
		return &protocol.Ack{Status: status, Token: tok, LeaseTTL: lease}

	case "w":
		tok, lease, err := s.lm.FIFOWait(req.Key, req.AcquireTimeout, connID)
		if err != nil {
			if errors.Is(err, lock.ErrNotEnqueued) {
				return &protocol.Ack{Status: "error"}
			}
			return &protocol.Ack{Status: "error"}
		}
		if tok == "" {
			return &protocol.Ack{Status: "timeout"}
		}
		return &protocol.Ack{Status: "ok", Token: tok, LeaseTTL: lease}

	case "sl":
		tok, err := s.lm.SemAcquire(req.Key, req.AcquireTimeout, req.LeaseTTL, connID, req.Limit)
		if err != nil {
			if errors.Is(err, lock.ErrMaxLocks) {
				return &protocol.Ack{Status: "error_max_locks"}
			}
			if errors.Is(err, lock.ErrLimitMismatch) {
				return &protocol.Ack{Status: "error_limit_mismatch"}
			}
			return &protocol.Ack{Status: "error"}
		}
		if tok == "" {
			return &protocol.Ack{Status: "timeout"}
		}
		return &protocol.Ack{Status: "ok", Token: tok, LeaseTTL: int(req.LeaseTTL.Seconds())}

	case "sr":
		if s.lm.SemRelease(req.Key, req.Token) {
			return &protocol.Ack{Status: "ok"}
		}
		return &protocol.Ack{Status: "error"}

	case "sn":
		remaining, ok := s.lm.SemRenew(req.Key, req.Token, req.LeaseTTL)
		if !ok {
			return &protocol.Ack{Status: "error"}
		}
		return &protocol.Ack{Status: "ok", Extra: fmt.Sprintf("%d", remaining)}

	case "se":
		status, tok, lease, err := s.lm.SemEnqueue(req.Key, req.LeaseTTL, connID, req.Limit)
		if err != nil {
			if errors.Is(err, lock.ErrMaxLocks) {
				return &protocol.Ack{Status: "error_max_locks"}
			}
			if errors.Is(err, lock.ErrLimitMismatch) {
				return &protocol.Ack{Status: "error_limit_mismatch"}
			}
			return &protocol.Ack{Status: "error"}
		}
		return &protocol.Ack{Status: status, Token: tok, LeaseTTL: lease}

	case "sw":
		tok, lease, err := s.lm.SemWait(req.Key, req.AcquireTimeout, connID)
		if err != nil {
			if errors.Is(err, lock.ErrNotEnqueued) {
				return &protocol.Ack{Status: "error"}
			}
			return &protocol.Ack{Status: "error"}
		}
		if tok == "" {
			return &protocol.Ack{Status: "timeout"}
		}
		return &protocol.Ack{Status: "ok", Token: tok, LeaseTTL: lease}
	}

	return &protocol.Ack{Status: "error"}
}
