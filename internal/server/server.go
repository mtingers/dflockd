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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/protocol"
	"github.com/mtingers/dflockd/internal/signal"
)

type connState struct {
	id      uint64
	writeCh chan []byte
}

type Server struct {
	lm        *lock.LockManager
	sig       *signal.Manager
	cfg       *config.Config
	log       *slog.Logger
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
	if s.cfg.WriteTimeout > 0 {
		conn.SetWriteDeadline(time.Time{})
	}
	return err
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn, connID uint64) {
	peer := conn.RemoteAddr().String()
	s.log.Debug("client connected", "peer", peer, "conn_id", connID)
	s.connCount.Add(1)
	s.conns.Store(conn, struct{}{})

	// Create a per-connection context that is cancelled when the server
	// shuts down, allowing in-progress lock waits to be interrupted.
	connCtx, connCancel := context.WithCancel(ctx)

	// Write multiplexer: writeMu serializes all writes to the connection.
	// writeCh is used by the signal push writer goroutine.
	var writeMu sync.Mutex
	writeCh := make(chan []byte, 64)

	cs := &connState{id: connID, writeCh: writeCh}

	writeResp := func(data []byte) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return s.writeResponse(conn, data)
	}

	// Push writer goroutine: drains writeCh and writes to conn.
	go func() {
		for data := range writeCh {
			writeMu.Lock()
			s.writeResponse(conn, data)
			writeMu.Unlock()
		}
	}()

	defer func() {
		connCancel()
		s.sig.UnlistenAll(connID)
		close(writeCh)
		s.conns.Delete(conn)
		s.connCount.Add(-1)
		s.lm.CleanupConnection(connID)
		conn.Close()
		s.log.Debug("client closed", "peer", peer, "conn_id", connID)
	}()

	reader := bufio.NewReader(conn)
	defaultLeaseTTL := s.cfg.DefaultLeaseTTL
	defaultLeaseTTLSec := int(defaultLeaseTTL.Seconds())

	if s.cfg.AuthToken != "" {
		req, err := protocol.ReadRequest(reader, s.cfg.ReadTimeout, conn, defaultLeaseTTL)
		if err != nil || req.Cmd != "auth" ||
			subtle.ConstantTimeCompare([]byte(req.Token), []byte(s.cfg.AuthToken)) != 1 {
			s.log.Warn("auth failed", "peer", peer, "conn_id", connID)
			writeResp(protocol.FormatResponse(&protocol.Ack{Status: "error_auth"}, defaultLeaseTTLSec))
			// Small delay to slow down brute-force attempts.
			time.Sleep(100 * time.Millisecond)
			return
		}
		writeResp(protocol.FormatResponse(&protocol.Ack{Status: "ok"}, defaultLeaseTTLSec))
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
				if err := writeResp(protocol.FormatResponse(&protocol.Ack{Status: "error"}, defaultLeaseTTLSec)); err != nil {
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
		if err := writeResp(protocol.FormatResponse(ack, defaultLeaseTTLSec)); err != nil {
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

	// --- Phase 1: Atomic Counters ---

	case "incr":
		val, err := s.lm.Incr(req.Key, req.Delta)
		if err != nil {
			if errors.Is(err, lock.ErrMaxKeys) {
				return &protocol.Ack{Status: "error_max_keys"}
			}
			return &protocol.Ack{Status: "error"}
		}
		return &protocol.Ack{Status: "ok", Extra: strconv.FormatInt(val, 10)}

	case "decr":
		val, err := s.lm.Decr(req.Key, req.Delta)
		if err != nil {
			if errors.Is(err, lock.ErrMaxKeys) {
				return &protocol.Ack{Status: "error_max_keys"}
			}
			return &protocol.Ack{Status: "error"}
		}
		return &protocol.Ack{Status: "ok", Extra: strconv.FormatInt(val, 10)}

	case "get":
		val := s.lm.GetCounter(req.Key)
		return &protocol.Ack{Status: "ok", Extra: strconv.FormatInt(val, 10)}

	case "cset":
		if err := s.lm.SetCounter(req.Key, req.Delta); err != nil {
			if errors.Is(err, lock.ErrMaxKeys) {
				return &protocol.Ack{Status: "error_max_keys"}
			}
			return &protocol.Ack{Status: "error"}
		}
		return &protocol.Ack{Status: "ok"}

	// --- Phase 2: KV with TTL ---

	case "kset":
		value := req.Value
		ttl := 0
		// Parse optional TTL: last space-separated token is TTL if it parses
		// as int >= 0 AND stripping it does not leave the value empty.
		if idx := strings.LastIndex(value, " "); idx >= 0 {
			candidate := value[idx+1:]
			if n, err := strconv.Atoi(candidate); err == nil && n >= 0 && idx > 0 {
				ttl = n
				value = value[:idx]
			}
		}
		if err := s.lm.KVSet(req.Key, value, ttl); err != nil {
			if errors.Is(err, lock.ErrMaxKeys) {
				return &protocol.Ack{Status: "error_max_keys"}
			}
			return &protocol.Ack{Status: "error"}
		}
		return &protocol.Ack{Status: "ok"}

	case "kget":
		val, ok := s.lm.KVGet(req.Key)
		if !ok {
			return &protocol.Ack{Status: "nil"}
		}
		return &protocol.Ack{Status: "ok", Extra: val}

	case "kdel":
		s.lm.KVDel(req.Key)
		return &protocol.Ack{Status: "ok"}

	// --- Phase 3: Signaling ---

	case "listen":
		listener := &signal.Listener{
			ConnID:  connID,
			Pattern: req.Key,
			WriteCh: cs.writeCh,
		}
		s.sig.Listen(req.Key, listener)
		return &protocol.Ack{Status: "ok"}

	case "unlisten":
		s.sig.Unlisten(req.Key, connID)
		return &protocol.Ack{Status: "ok"}

	case "signal":
		n := s.sig.Signal(req.Key, req.Value)
		return &protocol.Ack{Status: "ok", Extra: strconv.Itoa(n)}

	// --- Phase 4: Lists/Queues ---

	case "lpush":
		n, err := s.lm.LPush(req.Key, req.Value)
		if err != nil {
			if errors.Is(err, lock.ErrMaxKeys) {
				return &protocol.Ack{Status: "error_max_keys"}
			}
			if errors.Is(err, lock.ErrListFull) {
				return &protocol.Ack{Status: "error_list_full"}
			}
			return &protocol.Ack{Status: "error"}
		}
		return &protocol.Ack{Status: "ok", Extra: strconv.Itoa(n)}

	case "rpush":
		n, err := s.lm.RPush(req.Key, req.Value)
		if err != nil {
			if errors.Is(err, lock.ErrMaxKeys) {
				return &protocol.Ack{Status: "error_max_keys"}
			}
			if errors.Is(err, lock.ErrListFull) {
				return &protocol.Ack{Status: "error_list_full"}
			}
			return &protocol.Ack{Status: "error"}
		}
		return &protocol.Ack{Status: "ok", Extra: strconv.Itoa(n)}

	case "lpop":
		val, ok := s.lm.LPop(req.Key)
		if !ok {
			return &protocol.Ack{Status: "nil"}
		}
		return &protocol.Ack{Status: "ok", Extra: val}

	case "rpop":
		val, ok := s.lm.RPop(req.Key)
		if !ok {
			return &protocol.Ack{Status: "nil"}
		}
		return &protocol.Ack{Status: "ok", Extra: val}

	case "llen":
		n := s.lm.LLen(req.Key)
		return &protocol.Ack{Status: "ok", Extra: strconv.Itoa(n)}

	case "lrange":
		items := s.lm.LRange(req.Key, req.Start, req.Stop)
		data, err := json.Marshal(items)
		if err != nil {
			return &protocol.Ack{Status: "error"}
		}
		return &protocol.Ack{Status: "ok", Extra: string(data)}
	}

	s.log.Warn("unknown command in handleRequest", "cmd", req.Cmd, "conn", connID)
	return &protocol.Ack{Status: "error"}
}
