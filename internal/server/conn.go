package server

import (
	"bufio"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net"
	"runtime/debug"
	"slices"
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
	peer := s.startServeConn(conn, connID)
	connCtx, cancelConn := newConnCtx(ctx, conn)
	defer s.teardownConn(conn, peer, connID, cancelConn)
	s.serveConnSession(ctx, connCtx, newConnSession(s, conn), conn, peer, connID, cancelConn)
}

func (s *Server) startServeConn(conn net.Conn, connID uint64) string {
	peer := conn.RemoteAddr().String()
	s.log.Debug("client connected", "peer", peer, "conn_id", connID)
	return peer
}

func (s *Server) serveConnSession(ctx, connCtx context.Context, cs *connSession, conn net.Conn, peer string, connID uint64, cancelConn func()) {
	if !s.preflightAuth(cs.reader, conn, peer, connID, cs.writeResp) {
		return
	}
	s.requestLoop(ctx, connCtx, cs, conn, peer, connID, cancelConn)
}

// connSession bundles the per-connection state used by the request loop.
type connSession struct {
	reader          *bufio.Reader
	defaultLeaseTTL time.Duration
	writeResp       func(*protocol.Ack) error
}

func newConnSession(s *Server, conn net.Conn) *connSession {
	cs := baseConnSession(s, conn)
	cs.writeResp = newResponseWriter(s, conn, cs.defaultLeaseTTL)
	return cs
}

func baseConnSession(s *Server, conn net.Conn) *connSession {
	return &connSession{reader: bufio.NewReader(conn), defaultLeaseTTL: s.cfg.DefaultLeaseTTL}
}

func newResponseWriter(s *Server, conn net.Conn, leaseTTL time.Duration) func(*protocol.Ack) error {
	leaseSec := int(leaseTTL.Seconds())
	return func(ack *protocol.Ack) error {
		return s.writeResponse(conn, protocol.FormatResponse(ack, leaseSec))
	}
}

// newConnCtx makes the per-connection context + a cancel that also
// closes the underlying conn. Closing the conn unblocks any pending
// read so the handler exits promptly.
func newConnCtx(parent context.Context, conn net.Conn) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	return ctx, func() { cancel(); conn.Close() }
}

// teardownConn runs the deferred cleanup chain for ServeConn. In
// cluster mode it proposes a CleanupConn command so every replica
// drops this connection's holders/waiters; in single-node mode it
// goes through the lock manager directly. (If we're not the leader
// the propose returns an error — that's fine, the legacy path can't
// do anything either, and lease expiry will reclaim the holders.)
func (s *Server) teardownConn(conn net.Conn, peer string, connID uint64, cancelConn func()) {
	cancelConn()
	if c := s.clusterOrNil(); c != nil {
		s.teardownConnClustered(c, peer, connID)
	} else if err := s.lm.CleanupConnection(connID); err != nil {
		s.log.Error("connection cleanup failed", "peer", peer, "conn_id", connID, "err", err)
	}
	s.clearStableRef(connID)
	s.log.Debug("client closed", "peer", peer, "conn_id", connID)
}

func (s *Server) teardownConnClustered(c Cluster, peer string, connID uint64) {
	s.dropPendingGrant(connID) // a queued-but-never-waited Enqueue's listener
	if !c.IsLeader() {
		return // can't propose; lease expiry is the backstop
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.ReadTimeout)
	defer cancel()
	if _, err := c.ProposeCleanupConn(ctx, s.clusterRef(connID), s.clusterConnID(connID)); err != nil {
		s.log.Warn("cluster cleanup propose failed", "peer", peer, "conn_id", connID, "err", err)
	}
}

// preflightAuth runs the optional auth handshake. Returns false if
// the handshake failed (caller should exit ServeConn).
func (s *Server) preflightAuth(reader *bufio.Reader, conn net.Conn, peer string, connID uint64, writeResp func(*protocol.Ack) error) bool {
	if s.cfg.AuthToken == "" {
		return true
	}
	return s.authHandshake(reader, conn, peer, connID, s.cfg.DefaultLeaseTTL, writeResp)
}

// requestLoop is the main read/dispatch/write cycle.
func (s *Server) requestLoop(ctx, connCtx context.Context, cs *connSession, conn net.Conn, peer string, connID uint64, cancelConn func()) {
	for {
		if !s.requestStep(ctx, connCtx, cs, conn, peer, connID, cancelConn) {
			return
		}
	}
}

func (s *Server) requestStep(ctx, connCtx context.Context, cs *connSession, conn net.Conn, peer string, connID uint64, cancelConn func()) bool {
	req, keep := s.readRequest(cs, conn, peer)
	if req == nil {
		return keep
	}
	return s.dispatchReadRequest(ctx, connCtx, req, cs, conn, connID, cancelConn)
}

func (s *Server) dispatchReadRequest(ctx, connCtx context.Context, req *protocol.Request, cs *connSession, conn net.Conn, connID uint64, cancelConn func()) bool {
	if drainIfShuttingDown(ctx, cs.writeResp) {
		return false
	}
	return s.dispatchAndWrite(connCtx, req, cs, conn, connID, cancelConn)
}

func (s *Server) readRequest(cs *connSession, conn net.Conn, peer string) (*protocol.Request, bool) {
	req, err := protocol.ReadRequest(cs.reader, s.cfg.ReadTimeout, conn, cs.defaultLeaseTTL)
	if err != nil {
		return nil, s.handleReadErr(err, peer, cs.writeResp)
	}
	return req, true
}

// handleReadErr classifies a ReadRequest error. Returns true to
// continue the loop, false to exit ServeConn.
func (s *Server) handleReadErr(err error, peer string, writeResp func(*protocol.Ack) error) bool {
	pe, ok := asProtocolErr(err)
	if !ok {
		return s.handleNonProtocolReadErr(err, peer)
	}
	return s.handleProtocolReadErr(pe, peer, writeResp)
}

func asProtocolErr(err error) (*protocol.ProtocolError, bool) {
	var pe *protocol.ProtocolError
	ok := errors.As(err, &pe)
	return pe, ok
}

func (s *Server) handleNonProtocolReadErr(err error, peer string) bool {
	s.log.Error("read error", "peer", peer, "err", err)
	return false
}

func (s *Server) handleProtocolReadErr(pe *protocol.ProtocolError, peer string, writeResp func(*protocol.Ack) error) bool {
	if pe.Code == protocol.ErrCodeDisconnect {
		return false
	}
	return s.reportProtocolReadErr(pe, peer, writeResp)
}

func (s *Server) reportProtocolReadErr(pe *protocol.ProtocolError, peer string, writeResp func(*protocol.Ack) error) bool {
	s.log.Warn("protocol error", "peer", peer, "code", pe.Code, "msg", pe.Message)
	if !writeGenericError(writeResp) {
		return false
	}
	return canContinueAfterProtocolErr(pe)
}

func writeGenericError(writeResp func(*protocol.Ack) error) bool {
	return writeResp(&protocol.Ack{Status: protocol.StatusError}) == nil
}

func canContinueAfterProtocolErr(pe *protocol.ProtocolError) bool {
	return pe.Code != protocol.ErrCodeReadTimeout && pe.Code != protocol.ErrCodeLineTooLong
}

// drainIfShuttingDown writes error_draining and returns true when
// the parent context has been cancelled.
func drainIfShuttingDown(ctx context.Context, writeResp func(*protocol.Ack) error) bool {
	if ctx.Err() == nil {
		return false
	}
	_ = writeResp(&protocol.Ack{Status: protocol.StatusErrorDraining})
	return true
}

// dispatchAndWrite runs handleRequest under an optional peer-close
// watcher and writes the response. Returns false on write error.
func (s *Server) dispatchAndWrite(connCtx context.Context, req *protocol.Request, cs *connSession, conn net.Conn, connID uint64, cancelConn func()) bool {
	ack := s.dispatchWithPeerWatch(connCtx, req, cs, conn, connID, cancelConn)
	return cs.writeResp(ack) == nil
}

func (s *Server) dispatchWithPeerWatch(connCtx context.Context, req *protocol.Request, cs *connSession, conn net.Conn, connID uint64, cancelConn func()) *protocol.Ack {
	stopWatch := s.maybeWatchPeerClose(req, cs.reader, conn, cancelConn)
	defer stopIfWatching(stopWatch)
	return s.handleRequestRecovered(connCtx, req, connID)
}

// handleRequestRecovered runs handleRequest under a panic guard so one
// malformed request can't crash the process and drop every other
// connection (net/http does the same for the REST API). A recovered
// panic is logged with the offending command + stack and surfaces to
// the client as a generic error.
func (s *Server) handleRequestRecovered(ctx context.Context, req *protocol.Request, connID uint64) (ack *protocol.Ack) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("recovered panic in request handler",
				"conn_id", connID, "cmd", req.Cmd, "key", req.Key,
				"recovered", r, "stack", string(debug.Stack()))
			ack = &protocol.Ack{Status: protocol.StatusError}
		}
	}()
	return s.handleRequest(ctx, req, connID)
}

func stopIfWatching(stopWatch func()) {
	if stopWatch != nil {
		stopWatch()
	}
}

// maybeWatchPeerClose returns the stop function for the peer-close
// watcher when req can block; nil otherwise.
func (s *Server) maybeWatchPeerClose(req *protocol.Request, reader *bufio.Reader, conn net.Conn, cancelConn func()) func() {
	if !requestMayBlock(req) {
		return nil
	}
	return watchPeerClose(reader, conn, cancelConn)
}

// authHandshake performs the protocol-level auth exchange. Returns true
// to continue serving, false to disconnect. Brute-force attempts are
// slowed by a fixed sleep before close.
func (s *Server) authHandshake(reader *bufio.Reader, conn net.Conn, peer string, connID uint64, defaultLeaseTTL time.Duration, writeResp func(*protocol.Ack) error) bool {
	got, ok := s.readAuthToken(reader, conn, defaultLeaseTTL)
	if !ok || !s.authTokenMatches(got) {
		return s.rejectAuth(peer, connID, writeResp)
	}
	return s.acceptAuth(writeResp)
}

func (s *Server) readAuthToken(reader *bufio.Reader, conn net.Conn, defaultLeaseTTL time.Duration) (string, bool) {
	req, err := protocol.ReadRequest(reader, s.cfg.ReadTimeout, conn, defaultLeaseTTL)
	if err != nil || req == nil || req.Cmd != protocol.CmdAuth {
		return "", false
	}
	return req.AuthToken, true
}

func (s *Server) authTokenMatches(got string) bool {
	return subtle.ConstantTimeCompare([]byte(got), []byte(s.cfg.AuthToken)) == 1
}

func (s *Server) rejectAuth(peer string, connID uint64, writeResp func(*protocol.Ack) error) bool {
	s.log.Warn("auth failed", "peer", peer, "conn_id", connID)
	_ = writeResp(&protocol.Ack{Status: protocol.StatusErrorAuth})
	time.Sleep(100 * time.Millisecond)
	return false
}

func (s *Server) acceptAuth(writeResp func(*protocol.Ack) error) bool {
	return writeResp(&protocol.Ack{Status: protocol.StatusOK}) == nil
}

// writeResponse writes data to conn under the configured WriteTimeout.
func (s *Server) writeResponse(conn net.Conn, data []byte) error {
	s.setWriteDeadline(conn)
	_, err := conn.Write(data)
	s.clearWriteDeadline(conn, err)
	return err
}

func (s *Server) setWriteDeadline(conn net.Conn) {
	if s.cfg.WriteTimeout > 0 {
		conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
	}
}

func (s *Server) clearWriteDeadline(conn net.Conn, err error) {
	if s.cfg.WriteTimeout > 0 && err == nil {
		conn.SetWriteDeadline(time.Time{})
	}
}

// requestMayBlock reports whether req can keep the request goroutine
// blocked long enough to need watchPeerClose.
func requestMayBlock(req *protocol.Request) bool {
	return req.AcquireTimeout > 0 && requestCanBlock(req.Cmd)
}

func requestCanBlock(cmd string) bool {
	_, ok := blockingCommands[cmd]
	return ok
}

var blockingCommands = map[string]struct{}{
	protocol.CmdAcquire: {}, protocol.CmdSemAcquire: {},
	protocol.CmdWait: {}, protocol.CmdSemWait: {},
}

// commandHandler is a per-command dispatch entry.
type commandHandler func(s *Server, ctx context.Context, req *protocol.Request, connID uint64) *protocol.Ack

// commandTable maps every supported command to its handler. Looking
// up a missing command returns ok=false, which handleRequest maps to
// a generic error.
var commandTable = map[string]commandHandler{
	protocol.CmdPing: func(*Server, context.Context, *protocol.Request, uint64) *protocol.Ack {
		return &protocol.Ack{Status: protocol.StatusOK}
	},
	protocol.CmdStats: func(s *Server, _ context.Context, _ *protocol.Request, _ uint64) *protocol.Ack {
		return s.handleStats()
	},
	protocol.CmdAcquire:    (*Server).handleAcquire,
	protocol.CmdSemAcquire: (*Server).handleAcquire,
	protocol.CmdRelease: func(s *Server, _ context.Context, req *protocol.Request, _ uint64) *protocol.Ack {
		return s.handleRelease(req)
	},
	protocol.CmdSemRelease: func(s *Server, _ context.Context, req *protocol.Request, _ uint64) *protocol.Ack {
		return s.handleRelease(req)
	},
	protocol.CmdRenew: func(s *Server, _ context.Context, req *protocol.Request, _ uint64) *protocol.Ack {
		return s.handleRenew(req)
	},
	protocol.CmdSemRenew: func(s *Server, _ context.Context, req *protocol.Request, _ uint64) *protocol.Ack {
		return s.handleRenew(req)
	},
	protocol.CmdEnqueue: func(s *Server, _ context.Context, req *protocol.Request, connID uint64) *protocol.Ack {
		return s.handleEnqueue(req, connID)
	},
	protocol.CmdSemEnqueue: func(s *Server, _ context.Context, req *protocol.Request, connID uint64) *protocol.Ack {
		return s.handleEnqueue(req, connID)
	},
	protocol.CmdWait:    (*Server).handleWait,
	protocol.CmdSemWait: (*Server).handleWait,
	protocol.CmdBarrier: func(s *Server, ctx context.Context, _ *protocol.Request, _ uint64) *protocol.Ack {
		return s.handleBarrier(ctx)
	},
	protocol.CmdStableRef: func(s *Server, _ context.Context, req *protocol.Request, connID uint64) *protocol.Ack {
		return s.handleStableRef(req, connID)
	},
}

// handleStableRef records the caller-supplied stable ref on the
// connection. The ref is locked in on first use — a second
// stable-ref on the same connection is rejected.
func (s *Server) handleStableRef(req *protocol.Request, connID uint64) *protocol.Ack {
	if req.StableRef == "" {
		return &protocol.Ack{Status: protocol.StatusError, Extra: "stable_ref_empty"}
	}
	if !s.setStableRef(connID, req.StableRef) {
		return &protocol.Ack{Status: protocol.StatusError, Extra: "stable_ref_already_set"}
	}
	return &protocol.Ack{Status: protocol.StatusOK}
}

// handleRequest dispatches a fully-parsed request via commandTable.
func (s *Server) handleRequest(ctx context.Context, req *protocol.Request, connID uint64) *protocol.Ack {
	s.logRequest(req, connID)
	fn, ok := commandTable[req.Cmd]
	return s.dispatchKnownRequest(fn, ok, ctx, req, connID)
}

func (s *Server) dispatchKnownRequest(fn commandHandler, ok bool, ctx context.Context, req *protocol.Request, connID uint64) *protocol.Ack {
	if !ok {
		return s.unknownCommand(req, connID)
	}
	return fn(s, ctx, req, connID)
}

func (s *Server) logRequest(req *protocol.Request, connID uint64) {
	s.log.Debug("request", "conn", connID, "cmd", req.Cmd, "key", req.Key)
}

func (s *Server) unknownCommand(req *protocol.Request, connID uint64) *protocol.Ack {
	s.log.Warn("unknown command", "cmd", req.Cmd, "conn", connID)
	return &protocol.Ack{Status: protocol.StatusError}
}

// handleBarrier services the TCP `barrier` command. In cluster mode it
// proposes a no-op through Raft and waits for it to apply — a public
// linearizable-read primitive. In single-node mode it returns ok
// immediately (every preceding write is already visible). On a follower
// it returns error_not_leader with the leader's client address as Extra.
func (s *Server) handleBarrier(ctx context.Context) *protocol.Ack {
	c := s.clusterOrNil()
	if c == nil {
		return &protocol.Ack{Status: protocol.StatusOK}
	}
	if err := s.ClusterBarrier(ctx); err != nil {
		if errors.Is(err, ErrNotClusterLeader) {
			return &protocol.Ack{Status: protocol.StatusErrorNotLeader, Extra: s.ClusterLeaderAddr()}
		}
		return &protocol.Ack{Status: protocol.StatusError}
	}
	return &protocol.Ack{Status: protocol.StatusOK}
}

// handleStats returns the JSON-encoded LockManager snapshot.
func (s *Server) handleStats() *protocol.Ack {
	data, ok := s.statsJSON()
	if !ok {
		return &protocol.Ack{Status: protocol.StatusError}
	}
	return &protocol.Ack{Status: protocol.StatusOK, Extra: data}
}

func (s *Server) statsJSON() (string, bool) {
	st := s.lm.Stats(s.TotalConnCount())
	stripStatsKeys(st)
	var data []byte
	var err error
	if c := s.clusterOrNil(); c != nil {
		// Splice a "cluster" object alongside the lock stats (the lock
		// fields stay at the top level — single-node output is unchanged).
		data, err = json.Marshal(struct {
			*lock.Stats
			Cluster json.RawMessage `json:"cluster"`
		}{st, c.StatusJSON()})
	} else {
		data, err = json.Marshal(st)
	}
	return string(data), err == nil
}

// stripStatsKeys removes the internal lock:/sem: prefix from keys
// before they're shipped to clients.
func stripStatsKeys(st *lock.Stats) {
	stripLockInfoKeys(st.Locks)
	stripSemInfoKeys(st.Semaphores)
	stripIdleInfoKeys(st.IdleLocks)
	stripIdleInfoKeys(st.IdleSemaphores)
}

func stripLockInfoKeys(items []lock.LockInfo) {
	for i := range items {
		items[i].Key = lock.StripKeyPrefix(items[i].Key)
	}
}

func stripSemInfoKeys(items []lock.SemInfo) {
	for i := range items {
		items[i].Key = lock.StripKeyPrefix(items[i].Key)
	}
}

func stripIdleInfoKeys(items []lock.IdleInfo) {
	for i := range items {
		items[i].Key = lock.StripKeyPrefix(items[i].Key)
	}
}

func (s *Server) handleAcquire(ctx context.Context, req *protocol.Request, connID uint64) *protocol.Ack {
	if c := s.clusterOrNil(); c != nil {
		return s.clusterAcquire(ctx, c, req, connID)
	}
	tok, err := s.acquireToken(ctx, req, connID)
	if err != nil {
		return ackForLockErr(err)
	}
	return acquireAck(tok, req.LeaseTTL)
}

func (s *Server) acquireToken(ctx context.Context, req *protocol.Request, connID uint64) (string, error) {
	return s.lm.Acquire(ctx, requestKey(req), req.AcquireTimeout, req.LeaseTTL, connID, requestLimit(req))
}

func acquireAck(tok string, leaseTTL time.Duration) *protocol.Ack {
	if tok == "" {
		return &protocol.Ack{Status: protocol.StatusTimeout}
	}
	return &protocol.Ack{Status: protocol.StatusOK, Token: tok, LeaseTTL: int(leaseTTL.Seconds())}
}

func (s *Server) handleRelease(req *protocol.Request) *protocol.Ack {
	if c := s.clusterOrNil(); c != nil {
		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.ReadTimeout)
		defer cancel()
		return s.clusterRelease(ctx, c, req)
	}
	ok, err := s.lm.Release(requestKey(req), req.Token)
	if err != nil {
		return ackForLockErr(err)
	}
	if ok {
		return &protocol.Ack{Status: protocol.StatusOK}
	}
	return &protocol.Ack{Status: protocol.StatusError}
}

func (s *Server) handleRenew(req *protocol.Request) *protocol.Ack {
	if c := s.clusterOrNil(); c != nil {
		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.ReadTimeout)
		defer cancel()
		return s.clusterRenew(ctx, c, req)
	}
	remaining, ok, err := s.lm.Renew(requestKey(req), req.Token, req.LeaseTTL)
	if err != nil {
		return ackForLockErr(err)
	}
	if !ok {
		return &protocol.Ack{Status: protocol.StatusError}
	}
	return &protocol.Ack{Status: protocol.StatusOK, Extra: strconv.Itoa(remaining)}
}

func (s *Server) handleEnqueue(req *protocol.Request, connID uint64) *protocol.Ack {
	if c := s.clusterOrNil(); c != nil {
		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.ReadTimeout)
		defer cancel()
		return s.clusterEnqueue(ctx, c, req, connID)
	}
	status, tok, lease, err := s.enqueue(req, connID)
	if err != nil {
		return ackForLockErr(err)
	}
	return &protocol.Ack{Status: status, Token: tok, LeaseTTL: lease}
}

func (s *Server) enqueue(req *protocol.Request, connID uint64) (string, string, int, error) {
	return s.lm.Enqueue(requestKey(req), req.LeaseTTL, connID, requestLimit(req))
}

func (s *Server) handleWait(ctx context.Context, req *protocol.Request, connID uint64) *protocol.Ack {
	if c := s.clusterOrNil(); c != nil {
		return s.clusterWait(ctx, c, req, connID)
	}
	tok, lease, err := s.lm.Wait(ctx, requestKey(req), req.AcquireTimeout, connID)
	if err != nil {
		return ackForLockErr(err)
	}
	return waitAck(tok, lease)
}

func waitAck(tok string, lease int) *protocol.Ack {
	if tok == "" {
		return &protocol.Ack{Status: protocol.StatusTimeout}
	}
	return &protocol.Ack{Status: protocol.StatusOK, Token: tok, LeaseTTL: lease}
}

func requestKey(req *protocol.Request) string {
	return requestPrefix(req.Cmd) + req.Key
}

func requestPrefix(cmd string) string {
	if isSemaphoreCmd(cmd) {
		return lock.SemPrefix
	}
	return lock.LockPrefix
}

func requestLimit(req *protocol.Request) int {
	if isSemaphoreCmd(req.Cmd) {
		return req.Limit
	}
	return 1
}

func isSemaphoreCmd(cmd string) bool {
	_, ok := semaphoreCommands[cmd]
	return ok
}

var semaphoreCommands = map[string]struct{}{
	protocol.CmdSemAcquire: {}, protocol.CmdSemRelease: {},
	protocol.CmdSemRenew: {}, protocol.CmdSemEnqueue: {},
	protocol.CmdSemWait: {},
}

// ackForLockErr maps a LockManager error to its protocol Ack via a
// table lookup. Unknown errors fall through to StatusError so the
// wire never leaks internal detail.
func ackForLockErr(err error) *protocol.Ack {
	status, ok := lockErrStatus(err)
	if ok {
		return &protocol.Ack{Status: status}
	}
	return &protocol.Ack{Status: protocol.StatusError}
}

func lockErrStatus(err error) (string, bool) {
	idx := slices.IndexFunc(lockErrAcks, lockErrMatcher(err))
	if idx < 0 {
		return "", false
	}
	return lockErrAcks[idx].status, true
}

func lockErrMatcher(err error) func(lockErrAck) bool {
	return func(m lockErrAck) bool { return errors.Is(err, m.err) }
}

// lockErrAcks pairs each LockManager sentinel with its wire status.
// Order doesn't matter; errors.Is checks each in turn.
type lockErrAck struct {
	err    error
	status string
}

var lockErrAcks = []lockErrAck{
	{lock.ErrMaxLocks, protocol.StatusErrorMaxLocks},
	{lock.ErrMaxWaiters, protocol.StatusErrorMaxWaiters},
	{lock.ErrLimitMismatch, protocol.StatusErrorLimitMismatch},
	{lock.ErrAlreadyEnqueued, protocol.StatusErrorAlreadyEnqueued},
	{lock.ErrNotEnqueued, protocol.StatusErrorNotEnqueued},
	{lock.ErrLeaseExpired, protocol.StatusErrorLeaseExpired},
}
