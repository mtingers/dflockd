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

// writeChBuffer is the buffer size for a connection's push-writer writeCh.
// Matches client.signalChanBuffer and httpapi.sigChBuffer so no stage of
// the signal pipeline bottlenecks independently.
const writeChBuffer = 64

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

	addr := net.JoinHostPort(s.cfg.Host, strconv.Itoa(s.cfg.Port))
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

	// Exponential backoff on Accept errors. Without this, a persistent
	// error (e.g. FD exhaustion) busy-spins logging at full tilt. Reset
	// to zero on any successful accept.
	var backoff time.Duration
	const maxBackoff = 1 * time.Second

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				s.drain(&wg)
				return nil
			default:
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
				case <-ctx.Done():
					s.drain(&wg)
					return nil
				}
				continue
			}
		}
		backoff = 0
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
			defer s.connCount.Add(-1)
			defer s.conns.Delete(conn)
			s.ServeConn(ctx, conn, connID)
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

const (
	peerCloseWatchDelay = 10 * time.Millisecond
	peerCloseWatchPoll  = 50 * time.Millisecond
)

func requestMayBlock(req *protocol.Request) bool {
	switch req.Cmd {
	case "l", "sl", "w", "sw":
		return req.AcquireTimeout > 0
	default:
		return false
	}
}

func isTimeoutErr(err error) bool {
	// errors.As (rather than a direct type assertion) so that errors
	// wrapped by fmt.Errorf("...: %w", ...) or by transport layers
	// (e.g. crypto/tls) still classify correctly.
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// watchPeerClose watches for a peer close while a blocking lock operation is
// in flight. It peeks without consuming bytes, so a client that pipelines its
// next request leaves that byte buffered for the normal protocol reader. The
// delayed start keeps fast uncontended commands off this path.
//
// Concurrency invariant: the bufio.Reader is shared with the main handler
// goroutine, which is NOT safe for concurrent reads. This is only safe
// because the caller (ServeConn) guarantees:
//
//  1. The watcher goroutine is spawned only while the main goroutine is
//     inside handleRequest (i.e. not reading from `reader`).
//  2. stopPeerWatch() is called before the next ReadRequest and blocks
//     on `<-done`, so the watcher has fully exited before the main
//     goroutine resumes touching `reader`.
//
// Any future change that touches the reader from the main goroutine during
// a blocking handleRequest call, or that allows the watcher to outlive
// stopPeerWatch, will introduce a data race.
func watchPeerClose(reader *bufio.Reader, conn net.Conn, cancelConn func()) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)

		timer := time.NewTimer(peerCloseWatchDelay)
		defer timer.Stop()
		select {
		case <-stop:
			return
		case <-timer.C:
		}

		for {
			select {
			case <-stop:
				_ = conn.SetReadDeadline(time.Time{})
				return
			default:
			}

			peekN := reader.Buffered() + 1
			if peekN > reader.Size() {
				// The peer has filled the reader with pipelined bytes
				// behind a blocking command. We cannot observe EOF without
				// consuming the next request, and preserving a full-buffer
				// pipeline would leave disconnected waiters queued until
				// grant or timeout. Treat this as an abusive pipeline and
				// cancel the connection.
				cancelConn()
				return
			}

			_ = conn.SetReadDeadline(time.Now().Add(peerCloseWatchPoll))
			_, err := reader.Peek(peekN)
			_ = conn.SetReadDeadline(time.Time{})

			select {
			case <-stop:
				return
			default:
			}

			switch {
			case err == nil:
				// More pipelined data arrived. Keep peeking one byte
				// past the buffered data so a later EOF is still observed
				// without consuming the next request.
				continue
			case isTimeoutErr(err):
				continue
			default:
				cancelConn()
				return
			}
		}
	}()

	return func() {
		close(stop)
		// Force any in-flight reader.Peek to return immediately. Without
		// this, the watcher sits in Peek until its 50ms poll deadline
		// fires before it can observe `close(stop)` — up to 50ms of
		// added latency on every blocking command response. Under heavy
		// contention that overhead feeds back (longer responses →
		// longer queue waits → more watchers cross the 10ms initial
		// delay → more peek loops), collapsing throughput.
		_ = conn.SetReadDeadline(aLongTimeAgo)
		<-done
		_ = conn.SetReadDeadline(time.Time{})
	}
}

// aLongTimeAgo is a sentinel past time used to force an in-flight read to
// return with a timeout immediately. Same pattern used in net/http.
var aLongTimeAgo = time.Unix(1, 0)

// NextConnID allocates a new connection ID from the shared counter.
// Used by alternate transports (e.g. the HTTP bridge) that mint their
// own virtual connections and need a unique connID.
func (s *Server) NextConnID() uint64 {
	return s.connSeq.Add(1)
}

// LockManager returns the shared lock manager. Used by alternate transports.
func (s *Server) LockManager() *lock.LockManager {
	return s.lm
}

// Signals returns the shared signal manager. Used by alternate transports.
func (s *Server) Signals() *signal.Manager {
	return s.sig
}

// ConnCount returns the current number of active TCP connections.
func (s *Server) ConnCount() int64 {
	return s.connCount.Load()
}

// Config returns the server config. Used by alternate transports.
func (s *Server) Config() *config.Config {
	return s.cfg
}

// ServeConn runs the protocol handler loop on a single conn with the given
// connID until the conn is closed or the ctx is cancelled. The caller is
// responsible for conn-level accounting (e.g. s.conns tracking in the TCP
// accept loop) and for supplying a unique connID — typically from NextConnID().
//
// This is the entry point used by both the TCP accept loop (via handleConn
// below) and the HTTP bridge (via net.Pipe virtual connections).
func (s *Server) ServeConn(ctx context.Context, conn net.Conn, connID uint64) {
	peer := conn.RemoteAddr().String()
	s.log.Debug("client connected", "peer", peer, "conn_id", connID)

	// Create a per-connection context that is cancelled when the server
	// shuts down, allowing in-progress lock waits to be interrupted.
	connCtx, connCancel := context.WithCancel(ctx)

	// cancelConn cancels the per-connection context AND closes the TCP
	// connection. Closing the conn interrupts any blocking ReadRequest,
	// ensuring slow consumers (whose WriteCh is full) are promptly torn
	// down rather than lingering until ReadTimeout fires. All three
	// operations are idempotent and safe to call concurrently.
	cancelConn := func() {
		connCancel()
		conn.Close()
	}

	// writeCh is used by the signal push writer goroutine.
	writeCh := make(chan []byte, writeChBuffer)
	var writeMu sync.Mutex

	cs := &connState{
		id:         connID,
		writeCh:    writeCh,
		cancelConn: cancelConn,
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
				cancelConn()
				// Drain remaining messages to unblock senders.
				for range writeCh {
				}
				return
			}
		}
	}()

	defer func() {
		cancelConn()
		s.sig.UnlistenAll(connID)
		s.lm.CleanupConnection(connID)
		close(writeCh)
		pushWg.Wait()
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
		token := ""
		cmdOK := false
		if err == nil && req != nil {
			cmdOK = req.Cmd == "auth"
			if cmdOK {
				token = req.Token
			}
		}
		tokenOK := subtle.ConstantTimeCompare([]byte(token), []byte(s.cfg.AuthToken)) == 1
		if !cmdOK || !tokenOK {
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

		var stopPeerWatch func()
		if requestMayBlock(req) {
			stopPeerWatch = watchPeerClose(reader, conn, cancelConn)
		}
		ack := s.handleRequest(connCtx, req, cs)
		if stopPeerWatch != nil {
			stopPeerWatch()
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
	case "ping":
		return &protocol.Ack{Status: "ok"}

	case "stats":
		st := s.lm.Stats(s.connCount.Load())
		st.SignalChannels = append(st.SignalChannels, s.sig.Stats()...)
		// Strip internal key prefixes from stats output.
		for i := range st.Locks {
			st.Locks[i].Key = lock.StripKeyPrefix(st.Locks[i].Key)
		}
		for i := range st.Semaphores {
			st.Semaphores[i].Key = lock.StripKeyPrefix(st.Semaphores[i].Key)
		}
		for i := range st.IdleLocks {
			st.IdleLocks[i].Key = lock.StripKeyPrefix(st.IdleLocks[i].Key)
		}
		for i := range st.IdleSemaphores {
			st.IdleSemaphores[i].Key = lock.StripKeyPrefix(st.IdleSemaphores[i].Key)
		}
		data, err := json.Marshal(st)
		if err != nil {
			return &protocol.Ack{Status: "error"}
		}
		return &protocol.Ack{Status: "ok", Extra: string(data)}

	case "l", "sl":
		limit := 1
		key := lock.LockPrefix + req.Key
		if req.Cmd == "sl" {
			limit = req.Limit
			key = lock.SemPrefix + req.Key
		}
		tok, err := s.lm.Acquire(ctx, key, req.AcquireTimeout, req.LeaseTTL, connID, limit)
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
			if errors.Is(err, lock.ErrLeaseExpired) {
				// The slot was granted but its lease expired before we
				// could observe it (rare; happens when leaseTTL is very
				// short or the goroutine wake-up is delayed). Surfacing
				// this distinct status mirrors the two-phase Wait path
				// so single-phase callers can also distinguish it from
				// a generic error.
				return &protocol.Ack{Status: "error_lease_expired"}
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
		key := lock.LockPrefix + req.Key
		if req.Cmd == "sr" {
			key = lock.SemPrefix + req.Key
		}
		if s.lm.Release(key, req.Token) {
			return &protocol.Ack{Status: "ok"}
		}
		return &protocol.Ack{Status: "error"}

	case "n", "sn":
		key := lock.LockPrefix + req.Key
		if req.Cmd == "sn" {
			key = lock.SemPrefix + req.Key
		}
		remaining, ok := s.lm.Renew(key, req.Token, req.LeaseTTL)
		if !ok {
			return &protocol.Ack{Status: "error"}
		}
		return &protocol.Ack{Status: "ok", Extra: fmt.Sprintf("%d", remaining)}

	case "e", "se":
		limit := 1
		key := lock.LockPrefix + req.Key
		if req.Cmd == "se" {
			limit = req.Limit
			key = lock.SemPrefix + req.Key
		}
		status, tok, lease, err := s.lm.Enqueue(key, req.LeaseTTL, connID, limit)
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
		key := lock.LockPrefix + req.Key
		if req.Cmd == "sw" {
			key = lock.SemPrefix + req.Key
		}
		tok, lease, err := s.lm.Wait(ctx, key, req.AcquireTimeout, connID)
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
		removed, err := s.sig.Unlisten(req.Key, connID, req.Group)
		if err != nil {
			return &protocol.Ack{Status: "error"}
		}
		if removed {
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
