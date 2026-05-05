package server

import (
	"bufio"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"time"

	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/protocol"
)

// ServeConn runs the per-connection request loop until the connection
// closes or ctx is cancelled. Used directly by the TCP accept loop and
// (via NextConnID) is also the model the HTTP API uses for session
// lifecycle even though it doesn't replay through this function.
func (s *Server) ServeConn(ctx context.Context, conn net.Conn, connID uint64) {
	peer := conn.RemoteAddr().String()
	s.log.Debug("client connected", "peer", peer, "conn_id", connID)

	connCtx, connCancel := context.WithCancel(ctx)
	cancelConn := func() {
		connCancel()
		conn.Close()
	}

	defer func() {
		cancelConn()
		s.lm.CleanupConnection(connID)
		s.log.Debug("client closed", "peer", peer, "conn_id", connID)
	}()

	reader := bufio.NewReader(conn)
	defaultLeaseTTL := s.cfg.DefaultLeaseTTL
	defaultLeaseTTLSec := int(defaultLeaseTTL.Seconds())

	writeResp := func(ack *protocol.Ack) error {
		return s.writeResponse(conn, protocol.FormatResponse(ack, defaultLeaseTTLSec))
	}

	if s.cfg.AuthToken != "" {
		if !s.authHandshake(reader, conn, peer, connID, defaultLeaseTTL, writeResp) {
			return
		}
	}

	for {
		req, err := protocol.ReadRequest(reader, s.cfg.ReadTimeout, conn, defaultLeaseTTL)
		if err != nil {
			var pe *protocol.ProtocolError
			if errors.As(err, &pe) {
				if pe.Code == protocol.ErrCodeDisconnect {
					return
				}
				s.log.Warn("protocol error", "peer", peer, "code", pe.Code, "msg", pe.Message)
				if err := writeResp(&protocol.Ack{Status: protocol.StatusError}); err != nil {
					return
				}
				// Read-level errors (timeout, line too long) may have
				// desynced the framing; disconnect rather than guess.
				if pe.Code == protocol.ErrCodeReadTimeout || pe.Code == protocol.ErrCodeLineTooLong {
					return
				}
				continue
			}
			s.log.Error("read error", "peer", peer, "err", err)
			return
		}
		if ctx.Err() != nil {
			_ = writeResp(&protocol.Ack{Status: protocol.StatusErrorDraining})
			return
		}

		var stopWatch func()
		if requestMayBlock(req) {
			stopWatch = watchPeerClose(reader, conn, cancelConn)
		}
		ack := s.handleRequest(connCtx, req, connID)
		if stopWatch != nil {
			stopWatch()
		}
		if err := writeResp(ack); err != nil {
			return
		}
	}
}

// authHandshake performs the protocol-level auth exchange. Returns true
// to continue serving, false to disconnect. Brute-force attempts are
// slowed by a fixed sleep before close.
func (s *Server) authHandshake(reader *bufio.Reader, conn net.Conn, peer string, connID uint64, defaultLeaseTTL time.Duration, writeResp func(*protocol.Ack) error) bool {
	req, err := protocol.ReadRequest(reader, s.cfg.ReadTimeout, conn, defaultLeaseTTL)
	got := ""
	cmdOK := err == nil && req != nil && req.Cmd == protocol.CmdAuth
	if cmdOK {
		got = req.AuthToken
	}
	tokenOK := subtle.ConstantTimeCompare([]byte(got), []byte(s.cfg.AuthToken)) == 1
	if !cmdOK || !tokenOK {
		s.log.Warn("auth failed", "peer", peer, "conn_id", connID)
		_ = writeResp(&protocol.Ack{Status: protocol.StatusErrorAuth})
		time.Sleep(100 * time.Millisecond)
		return false
	}
	if err := writeResp(&protocol.Ack{Status: protocol.StatusOK}); err != nil {
		return false
	}
	return true
}

// writeResponse writes data to conn under the configured WriteTimeout.
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

// requestMayBlock reports whether req can keep the request goroutine
// blocked long enough to need watchPeerClose.
func requestMayBlock(req *protocol.Request) bool {
	switch req.Cmd {
	case protocol.CmdAcquire, protocol.CmdSemAcquire,
		protocol.CmdWait, protocol.CmdSemWait:
		return req.AcquireTimeout > 0
	}
	return false
}

// handleRequest dispatches a fully-parsed request and returns the Ack.
// The caller writes the Ack; this function never touches the wire.
func (s *Server) handleRequest(ctx context.Context, req *protocol.Request, connID uint64) *protocol.Ack {
	s.log.Debug("request", "conn", connID, "cmd", req.Cmd, "key", req.Key)

	switch req.Cmd {
	case protocol.CmdPing:
		return &protocol.Ack{Status: protocol.StatusOK}
	case protocol.CmdStats:
		return s.handleStats()
	case protocol.CmdAcquire, protocol.CmdSemAcquire:
		return s.handleAcquire(ctx, req, connID)
	case protocol.CmdRelease, protocol.CmdSemRelease:
		return s.handleRelease(req)
	case protocol.CmdRenew, protocol.CmdSemRenew:
		return s.handleRenew(req)
	case protocol.CmdEnqueue, protocol.CmdSemEnqueue:
		return s.handleEnqueue(req, connID)
	case protocol.CmdWait, protocol.CmdSemWait:
		return s.handleWait(ctx, req, connID)
	}
	s.log.Warn("unknown command", "cmd", req.Cmd, "conn", connID)
	return &protocol.Ack{Status: protocol.StatusError}
}

// handleStats returns the JSON-encoded LockManager snapshot.
func (s *Server) handleStats() *protocol.Ack {
	st := s.lm.Stats(s.connCount.Load())
	stripStatsKeys(st)
	data, err := json.Marshal(st)
	if err != nil {
		return &protocol.Ack{Status: protocol.StatusError}
	}
	return &protocol.Ack{Status: protocol.StatusOK, Extra: string(data)}
}

// stripStatsKeys removes the internal lock:/sem: prefix from keys
// before they're shipped to clients.
func stripStatsKeys(st *lock.Stats) {
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
}

func (s *Server) handleAcquire(ctx context.Context, req *protocol.Request, connID uint64) *protocol.Ack {
	limit := 1
	key := lock.LockPrefix + req.Key
	if req.Cmd == protocol.CmdSemAcquire {
		limit = req.Limit
		key = lock.SemPrefix + req.Key
	}
	tok, err := s.lm.Acquire(ctx, key, req.AcquireTimeout, req.LeaseTTL, connID, limit)
	if err != nil {
		return ackForLockErr(err)
	}
	if tok == "" {
		return &protocol.Ack{Status: protocol.StatusTimeout}
	}
	return &protocol.Ack{Status: protocol.StatusOK, Token: tok, LeaseTTL: int(req.LeaseTTL.Seconds())}
}

func (s *Server) handleRelease(req *protocol.Request) *protocol.Ack {
	key := lock.LockPrefix + req.Key
	if req.Cmd == protocol.CmdSemRelease {
		key = lock.SemPrefix + req.Key
	}
	if s.lm.Release(key, req.Token) {
		return &protocol.Ack{Status: protocol.StatusOK}
	}
	return &protocol.Ack{Status: protocol.StatusError}
}

func (s *Server) handleRenew(req *protocol.Request) *protocol.Ack {
	key := lock.LockPrefix + req.Key
	if req.Cmd == protocol.CmdSemRenew {
		key = lock.SemPrefix + req.Key
	}
	remaining, ok := s.lm.Renew(key, req.Token, req.LeaseTTL)
	if !ok {
		return &protocol.Ack{Status: protocol.StatusError}
	}
	return &protocol.Ack{Status: protocol.StatusOK, Extra: strconv.Itoa(remaining)}
}

func (s *Server) handleEnqueue(req *protocol.Request, connID uint64) *protocol.Ack {
	limit := 1
	key := lock.LockPrefix + req.Key
	if req.Cmd == protocol.CmdSemEnqueue {
		limit = req.Limit
		key = lock.SemPrefix + req.Key
	}
	status, tok, lease, err := s.lm.Enqueue(key, req.LeaseTTL, connID, limit)
	if err != nil {
		return ackForLockErr(err)
	}
	return &protocol.Ack{Status: status, Token: tok, LeaseTTL: lease}
}

func (s *Server) handleWait(ctx context.Context, req *protocol.Request, connID uint64) *protocol.Ack {
	key := lock.LockPrefix + req.Key
	if req.Cmd == protocol.CmdSemWait {
		key = lock.SemPrefix + req.Key
	}
	tok, lease, err := s.lm.Wait(ctx, key, req.AcquireTimeout, connID)
	if err != nil {
		return ackForLockErr(err)
	}
	if tok == "" {
		return &protocol.Ack{Status: protocol.StatusTimeout}
	}
	return &protocol.Ack{Status: protocol.StatusOK, Token: tok, LeaseTTL: lease}
}

// ackForLockErr maps a LockManager error to its protocol Ack. Returns
// a generic StatusError for unknown errors so the wire never leaks
// internal detail.
func ackForLockErr(err error) *protocol.Ack {
	switch {
	case errors.Is(err, lock.ErrMaxLocks):
		return &protocol.Ack{Status: protocol.StatusErrorMaxLocks}
	case errors.Is(err, lock.ErrMaxWaiters):
		return &protocol.Ack{Status: protocol.StatusErrorMaxWaiters}
	case errors.Is(err, lock.ErrLimitMismatch):
		return &protocol.Ack{Status: protocol.StatusErrorLimitMismatch}
	case errors.Is(err, lock.ErrAlreadyEnqueued):
		return &protocol.Ack{Status: protocol.StatusErrorAlreadyEnqueued}
	case errors.Is(err, lock.ErrNotEnqueued):
		return &protocol.Ack{Status: protocol.StatusErrorNotEnqueued}
	case errors.Is(err, lock.ErrLeaseExpired):
		return &protocol.Ack{Status: protocol.StatusErrorLeaseExpired}
	case errors.Is(err, lock.ErrWaiterClosed):
		return &protocol.Ack{Status: protocol.StatusError}
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return &protocol.Ack{Status: protocol.StatusError}
	}
	return &protocol.Ack{Status: protocol.StatusError}
}
