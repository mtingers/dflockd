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
	"github.com/mtingers/dflockd/internal/watch"
)

type connState struct {
	id         uint64
	writeCh    chan []byte
	cancelConn func() // cancels the connection context (disconnect slow consumer)
}

type Server struct {
	lm        *lock.LockManager
	sig       *signal.Manager
	wm        *watch.Manager
	cfg       *config.Config
	log       *slog.Logger
	connSeq   atomic.Uint64
	connCount atomic.Int64
	conns     sync.Map // net.Conn → struct{}
}

func New(lm *lock.LockManager, cfg *config.Config, log *slog.Logger) *Server {
	s := &Server{lm: lm, cfg: cfg, log: log, sig: signal.NewManager(), wm: watch.NewManager()}
	lm.OnLockRelease = func(key string) {
		s.wm.Notify("release", key)
	}
	return s
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
	var pushWg sync.WaitGroup

	cs := &connState{id: connID, writeCh: writeCh, cancelConn: connCancel}

	writeResp := func(data []byte) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return s.writeResponse(conn, data)
	}

	// Push writer goroutine: drains writeCh and writes to conn.
	// On write error (dead connection), cancels conn context to unblock
	// any in-flight blocking commands, then drains without writing.
	pushWg.Add(1)
	go func() {
		defer pushWg.Done()
		var failed bool
		for data := range writeCh {
			if failed {
				continue // drain without writing
			}
			writeMu.Lock()
			if err := s.writeResponse(conn, data); err != nil {
				failed = true
				cs.cancelConn()
			}
			writeMu.Unlock()
		}
	}()

	defer func() {
		connCancel()
		s.sig.UnlistenAll(connID)
		s.wm.UnwatchAll(connID)
		s.lm.UnregisterAllLeaderWatchers(connID)
		close(writeCh)
		pushWg.Wait() // wait for push writer to drain before closing conn
		s.lm.CleanupConnection(connID)
		s.conns.Delete(conn)
		s.connCount.Add(-1)
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
		if ack == nil {
			if connCtx.Err() != nil {
				break // connection shutting down
			}
			continue // no response needed (e.g. connection closing)
		}
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
				Group:     si.Group,
				Listeners: si.Listeners,
			})
		}
		watchStats := s.wm.Stats()
		for _, wi := range watchStats {
			st.WatchChannels = append(st.WatchChannels, lock.WatchChannelInfo{
				Pattern:  wi.Pattern,
				Watchers: wi.Watchers,
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
		tok, fence, err := s.lm.AcquireWithFence(ctx, req.Key, req.AcquireTimeout, req.LeaseTTL, connID, limit)
		if err != nil {
			if errors.Is(err, lock.ErrMaxLocks) {
				return &protocol.Ack{Status: "error_max_locks"}
			}
			if errors.Is(err, lock.ErrLimitMismatch) {
				return &protocol.Ack{Status: "error_limit_mismatch"}
			}
			if errors.Is(err, lock.ErrTypeMismatch) {
				return &protocol.Ack{Status: "error_type_mismatch"}
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
		s.wm.Notify("acquire", req.Key)
		return &protocol.Ack{Status: "ok", Token: tok, LeaseTTL: int(req.LeaseTTL.Seconds()), Fence: fence}

	case "r", "sr":
		if s.lm.Release(req.Key, req.Token) {
			s.wm.Notify("release", req.Key)
			return &protocol.Ack{Status: "ok"}
		}
		return &protocol.Ack{Status: "error"}

	case "n", "sn":
		remaining, fence, ok := s.lm.RenewWithFence(req.Key, req.Token, req.LeaseTTL)
		if !ok {
			return &protocol.Ack{Status: "error"}
		}
		return &protocol.Ack{Status: "ok", Extra: fmt.Sprintf("%d %d", remaining, fence)}

	case "e", "se":
		limit := 1
		if req.Cmd == "se" {
			limit = req.Limit
		}
		status, tok, lease, fence, err := s.lm.EnqueueWithFence(req.Key, req.LeaseTTL, connID, limit)
		if err != nil {
			if errors.Is(err, lock.ErrMaxLocks) {
				return &protocol.Ack{Status: "error_max_locks"}
			}
			if errors.Is(err, lock.ErrLimitMismatch) {
				return &protocol.Ack{Status: "error_limit_mismatch"}
			}
			if errors.Is(err, lock.ErrTypeMismatch) {
				return &protocol.Ack{Status: "error_type_mismatch"}
			}
			if errors.Is(err, lock.ErrMaxWaiters) {
				return &protocol.Ack{Status: "error_max_waiters"}
			}
			if errors.Is(err, lock.ErrAlreadyEnqueued) {
				return &protocol.Ack{Status: "error_already_enqueued"}
			}
			return &protocol.Ack{Status: "error"}
		}
		if status == "acquired" {
			s.wm.Notify(req.Cmd, req.Key)
		}
		return &protocol.Ack{Status: status, Token: tok, LeaseTTL: lease, Fence: fence}

	case "w", "sw":
		tok, lease, fence, err := s.lm.WaitWithFence(ctx, req.Key, req.AcquireTimeout, connID)
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
		s.wm.Notify(req.Cmd, req.Key)
		return &protocol.Ack{Status: "ok", Token: tok, LeaseTTL: lease, Fence: fence}

	// --- Phase 1: Atomic Counters ---

	case "incr":
		val, err := s.lm.Incr(req.Key, req.Delta)
		if err != nil {
			if errors.Is(err, lock.ErrMaxKeys) {
				return &protocol.Ack{Status: "error_max_keys"}
			}
			return &protocol.Ack{Status: "error"}
		}
		s.wm.Notify("incr", req.Key)
		return &protocol.Ack{Status: "ok", Extra: strconv.FormatInt(val, 10)}

	case "decr":
		val, err := s.lm.Decr(req.Key, req.Delta)
		if err != nil {
			if errors.Is(err, lock.ErrMaxKeys) {
				return &protocol.Ack{Status: "error_max_keys"}
			}
			return &protocol.Ack{Status: "error"}
		}
		s.wm.Notify("decr", req.Key)
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
		s.wm.Notify("cset", req.Key)
		return &protocol.Ack{Status: "ok"}

	// --- Phase 2: KV with TTL ---

	case "kset":
		if err := s.lm.KVSet(req.Key, req.Value, req.TTLSeconds); err != nil {
			if errors.Is(err, lock.ErrMaxKeys) {
				return &protocol.Ack{Status: "error_max_keys"}
			}
			return &protocol.Ack{Status: "error"}
		}
		s.wm.Notify("kset", req.Key)
		return &protocol.Ack{Status: "ok"}

	case "kget":
		val, ok := s.lm.KVGet(req.Key)
		if !ok {
			return &protocol.Ack{Status: "nil"}
		}
		return &protocol.Ack{Status: "ok", Extra: val}

	case "kdel":
		s.lm.KVDel(req.Key)
		s.wm.Notify("kdel", req.Key)
		return &protocol.Ack{Status: "ok"}

	case "kcas":
		ok, err := s.lm.KVCAS(req.Key, req.OldValue, req.Value, req.TTLSeconds)
		if err != nil {
			if errors.Is(err, lock.ErrMaxKeys) {
				return &protocol.Ack{Status: "error_max_keys"}
			}
			return &protocol.Ack{Status: "error"}
		}
		if !ok {
			return &protocol.Ack{Status: "cas_conflict"}
		}
		s.wm.Notify("kcas", req.Key)
		return &protocol.Ack{Status: "ok"}

	// --- Phase 3: Signaling ---

	case "listen":
		listener := &signal.Listener{
			ConnID:     connID,
			Pattern:    req.Key,
			Group:      req.Group,
			WriteCh:    cs.writeCh,
			CancelConn: cs.cancelConn,
		}
		if err := s.sig.Listen(listener); err != nil {
			return &protocol.Ack{Status: "error"}
		}
		return &protocol.Ack{Status: "ok"}

	case "unlisten":
		s.sig.Unlisten(req.Key, connID, req.Group)
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
		s.wm.Notify("lpush", req.Key)
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
		s.wm.Notify("rpush", req.Key)
		return &protocol.Ack{Status: "ok", Extra: strconv.Itoa(n)}

	case "lpop":
		val, ok := s.lm.LPop(req.Key)
		if !ok {
			return &protocol.Ack{Status: "nil"}
		}
		s.wm.Notify("lpop", req.Key)
		return &protocol.Ack{Status: "ok", Extra: val}

	case "rpop":
		val, ok := s.lm.RPop(req.Key)
		if !ok {
			return &protocol.Ack{Status: "nil"}
		}
		s.wm.Notify("rpop", req.Key)
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

	// --- Blocking List Pop ---

	case "blpop":
		val, err := s.lm.BLPop(ctx, req.Key, req.AcquireTimeout, connID)
		if err != nil {
			if errors.Is(err, lock.ErrWaiterClosed) {
				return nil // connection closing, skip response
			}
			if ctx.Err() != nil {
				return nil // connection/server shutting down, skip response
			}
			if errors.Is(err, lock.ErrMaxKeys) {
				return &protocol.Ack{Status: "error_max_keys"}
			}
			return &protocol.Ack{Status: "error"}
		}
		if val == "" {
			return &protocol.Ack{Status: "nil"}
		}
		s.wm.Notify("blpop", req.Key)
		return &protocol.Ack{Status: "ok", Extra: val}

	case "brpop":
		val, err := s.lm.BRPop(ctx, req.Key, req.AcquireTimeout, connID)
		if err != nil {
			if errors.Is(err, lock.ErrWaiterClosed) {
				return nil // connection closing, skip response
			}
			if ctx.Err() != nil {
				return nil // connection/server shutting down, skip response
			}
			if errors.Is(err, lock.ErrMaxKeys) {
				return &protocol.Ack{Status: "error_max_keys"}
			}
			return &protocol.Ack{Status: "error"}
		}
		if val == "" {
			return &protocol.Ack{Status: "nil"}
		}
		s.wm.Notify("brpop", req.Key)
		return &protocol.Ack{Status: "ok", Extra: val}

	// --- Read-Write Locks ---

	case "rl":
		tok, fence, err := s.lm.RWAcquire(ctx, req.Key, 'r', req.AcquireTimeout, req.LeaseTTL, connID)
		if err != nil {
			return rwAcquireError(err)
		}
		if tok == "" {
			return &protocol.Ack{Status: "timeout"}
		}
		s.wm.Notify("rl", req.Key)
		return &protocol.Ack{Status: "ok", Token: tok, LeaseTTL: int(req.LeaseTTL.Seconds()), Fence: fence}

	case "wl":
		tok, fence, err := s.lm.RWAcquire(ctx, req.Key, 'w', req.AcquireTimeout, req.LeaseTTL, connID)
		if err != nil {
			return rwAcquireError(err)
		}
		if tok == "" {
			return &protocol.Ack{Status: "timeout"}
		}
		s.wm.Notify("wl", req.Key)
		return &protocol.Ack{Status: "ok", Token: tok, LeaseTTL: int(req.LeaseTTL.Seconds()), Fence: fence}

	case "rr", "wr":
		if s.lm.RWRelease(req.Key, req.Token) {
			s.wm.Notify(req.Cmd, req.Key)
			return &protocol.Ack{Status: "ok"}
		}
		return &protocol.Ack{Status: "error"}

	case "rn", "wn":
		remaining, fence, ok := s.lm.RWRenew(req.Key, req.Token, req.LeaseTTL)
		if !ok {
			return &protocol.Ack{Status: "error"}
		}
		return &protocol.Ack{Status: "ok", Extra: fmt.Sprintf("%d %d", remaining, fence)}

	case "re":
		status, tok, lease, fence, err := s.lm.RWEnqueue(req.Key, 'r', req.LeaseTTL, connID)
		if err != nil {
			return rwAcquireError(err)
		}
		if status == "acquired" {
			s.wm.Notify("re", req.Key)
		}
		return &protocol.Ack{Status: status, Token: tok, LeaseTTL: lease, Fence: fence}

	case "we":
		status, tok, lease, fence, err := s.lm.RWEnqueue(req.Key, 'w', req.LeaseTTL, connID)
		if err != nil {
			return rwAcquireError(err)
		}
		if status == "acquired" {
			s.wm.Notify("we", req.Key)
		}
		return &protocol.Ack{Status: status, Token: tok, LeaseTTL: lease, Fence: fence}

	case "rw", "ww":
		tok, lease, fence, err := s.lm.RWWait(ctx, req.Key, req.AcquireTimeout, connID)
		if err != nil {
			if errors.Is(err, lock.ErrNotEnqueued) {
				return &protocol.Ack{Status: "error_not_enqueued"}
			}
			if errors.Is(err, lock.ErrLeaseExpired) {
				return &protocol.Ack{Status: "error_lease_expired"}
			}
			if errors.Is(err, lock.ErrWaiterClosed) {
				s.log.Debug("waiter closed during rw wait", "key", req.Key, "conn", connID)
				return &protocol.Ack{Status: "error"}
			}
			return &protocol.Ack{Status: "error"}
		}
		if tok == "" {
			return &protocol.Ack{Status: "timeout"}
		}
		s.wm.Notify(req.Cmd, req.Key)
		return &protocol.Ack{Status: "ok", Token: tok, LeaseTTL: lease, Fence: fence}

	// --- Watch/Notify ---

	case "watch":
		w := &watch.Watcher{
			ConnID:     connID,
			Pattern:    req.Key,
			WriteCh:    cs.writeCh,
			CancelConn: cs.cancelConn,
		}
		if err := s.wm.Watch(w); err != nil {
			return &protocol.Ack{Status: "error"}
		}
		return &protocol.Ack{Status: "ok"}

	case "unwatch":
		s.wm.Unwatch(req.Key, connID)
		return &protocol.Ack{Status: "ok"}

	// --- Barriers ---

	case "bwait":
		ok, err := s.lm.BarrierWait(ctx, req.Key, req.Limit, req.AcquireTimeout, connID)
		if err != nil {
			if errors.Is(err, lock.ErrBarrierCountMismatch) {
				return &protocol.Ack{Status: "error_barrier_count_mismatch"}
			}
			if errors.Is(err, lock.ErrMaxKeys) {
				return &protocol.Ack{Status: "error_max_keys"}
			}
			return &protocol.Ack{Status: "error"}
		}
		if !ok {
			return &protocol.Ack{Status: "timeout"}
		}
		return &protocol.Ack{Status: "ok"}

	// --- Leader Election ---

	case "elect":
		tok, fence, err := s.lm.AcquireWithFence(ctx, req.Key, req.AcquireTimeout, req.LeaseTTL, connID, 1)
		if err != nil {
			if errors.Is(err, lock.ErrMaxLocks) {
				return &protocol.Ack{Status: "error_max_locks"}
			}
			if errors.Is(err, lock.ErrLimitMismatch) {
				return &protocol.Ack{Status: "error_limit_mismatch"}
			}
			if errors.Is(err, lock.ErrTypeMismatch) {
				return &protocol.Ack{Status: "error_type_mismatch"}
			}
			if errors.Is(err, lock.ErrMaxWaiters) {
				return &protocol.Ack{Status: "error_max_waiters"}
			}
			if errors.Is(err, lock.ErrWaiterClosed) {
				s.log.Debug("waiter closed during elect", "key", req.Key, "conn", connID)
				return &protocol.Ack{Status: "error"}
			}
			return &protocol.Ack{Status: "error"}
		}
		if tok == "" {
			return &protocol.Ack{Status: "timeout"}
		}
		s.wm.Notify("elect", req.Key)
		s.lm.RegisterAndNotifyLeader(req.Key, "elected", connID, cs.writeCh, cs.cancelConn)
		return &protocol.Ack{Status: "ok", Token: tok, LeaseTTL: int(req.LeaseTTL.Seconds()), Fence: fence}

	case "resign":
		if s.lm.ResignLeader(req.Key, req.Token, connID) {
			s.wm.Notify("resign", req.Key)
			return &protocol.Ack{Status: "ok"}
		}
		return &protocol.Ack{Status: "error"}

	case "observe":
		s.lm.RegisterLeaderWatcherWithStatus(req.Key, connID, cs.writeCh, cs.cancelConn)
		return &protocol.Ack{Status: "ok"}

	case "unobserve":
		s.lm.UnregisterLeaderWatcher(req.Key, connID)
		return &protocol.Ack{Status: "ok"}
	}

	s.log.Warn("unknown command in handleRequest", "cmd", req.Cmd, "conn", connID)
	return &protocol.Ack{Status: "error"}
}

func rwAcquireError(err error) *protocol.Ack {
	if errors.Is(err, lock.ErrMaxLocks) {
		return &protocol.Ack{Status: "error_max_locks"}
	}
	if errors.Is(err, lock.ErrTypeMismatch) {
		return &protocol.Ack{Status: "error_type_mismatch"}
	}
	if errors.Is(err, lock.ErrMaxWaiters) {
		return &protocol.Ack{Status: "error_max_waiters"}
	}
	if errors.Is(err, lock.ErrAlreadyEnqueued) {
		return &protocol.Ack{Status: "error_already_enqueued"}
	}
	if errors.Is(err, lock.ErrWaiterClosed) {
		return &protocol.Ack{Status: "error"}
	}
	return &protocol.Ack{Status: "error"}
}
