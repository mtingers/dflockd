package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// TCPTransport carries Raft RPCs over real TCP. It is the production
// peer of MemTransport. Connection topology is symmetric: each pair of
// nodes ends up with two TCP connections, one initiated by each side;
// the dialer side carries requests + matched responses, and the
// accepter side serves incoming requests + writes their responses.
// This avoids the "which side wins on a simultaneous dial" tie-break
// and keeps the read/write halves of each connection unambiguous.
//
// TLS / cluster-secret auth on the handshake are intentionally NOT
// wired in v1 (see PLAN.md §3); they are additive over this layer.
type TCPTransport struct {
	id       NodeID
	listener net.Listener
	handler  func(from NodeID, req Message) Message
	addrs    sync.Map   // NodeID → string
	outbound sync.Map   // NodeID → *outboundConn
	accepted sync.Map   // net.Conn → struct{} (open accepted conns)
	dialMu   sync.Mutex // serialises dial-on-demand per peer
	log      *slog.Logger
	wg       sync.WaitGroup
	closed   atomic.Bool
}

var _ Transport = (*TCPTransport)(nil)

// NewTCPTransport listens on listenAddr ("host:port"; "0" gets a free
// port) and returns a Transport bound to id. Call SetHandler before
// expecting inbound RPCs to deliver.
func NewTCPTransport(id NodeID, listenAddr string, logger *slog.Logger) (*TCPTransport, error) {
	if logger == nil {
		logger = slog.Default()
	}
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("raft: tcp listen %s: %w", listenAddr, err)
	}
	t := &TCPTransport{id: id, listener: lis, log: logger.With("transport", id)}
	t.wg.Add(1)
	go t.acceptLoop()
	return t, nil
}

func (t *TCPTransport) LocalID() NodeID { return t.id }

// ListenAddr returns the resolved listen address (useful when the
// caller bound to ":0" and needs the assigned port).
func (t *TCPTransport) ListenAddr() string { return t.listener.Addr().String() }

func (t *TCPTransport) SetHandler(h func(from NodeID, req Message) Message) { t.handler = h }

func (t *TCPTransport) AddPeer(id NodeID, addr string) { t.addrs.Store(id, addr) }

func (t *TCPTransport) RemovePeer(id NodeID) {
	t.addrs.Delete(id)
	t.dropOutbound(id)
}

// Close stops the accept loop, closes the listener and every active
// outbound connection, and waits for all background goroutines to exit.
func (t *TCPTransport) Close() error {
	if !t.closed.CompareAndSwap(false, true) {
		return nil
	}
	_ = t.listener.Close()
	t.outbound.Range(func(k, v any) bool {
		v.(*outboundConn).close()
		return true
	})
	t.accepted.Range(func(k, _ any) bool {
		_ = k.(net.Conn).Close()
		return true
	})
	t.wg.Wait()
	return nil
}

// Send delivers req to `to` and blocks for the reply or ctx.Done.
func (t *TCPTransport) Send(ctx context.Context, to NodeID, req Message) (Message, error) {
	oc, err := t.getOrDialOutbound(to)
	if err != nil {
		return nil, err
	}
	return oc.send(ctx, req)
}

// ---------------------------------------------------------------------------
// Accept side
// ---------------------------------------------------------------------------

func (t *TCPTransport) acceptLoop() {
	defer t.wg.Done()
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			if t.closed.Load() {
				return
			}
			t.log.Debug("accept error", "err", err)
			continue
		}
		t.wg.Add(1)
		go t.serveAccepted(conn)
	}
}

func (t *TCPTransport) serveAccepted(conn net.Conn) {
	defer t.wg.Done()
	t.accepted.Store(conn, struct{}{})
	defer func() { t.accepted.Delete(conn); _ = conn.Close() }()
	from, err := t.serverHandshake(conn)
	if err != nil {
		t.log.Debug("inbound handshake failed", "err", err)
		return
	}
	t.serveRequests(conn, from)
}

func (t *TCPTransport) serverHandshake(conn net.Conn) (NodeID, error) {
	body, err := readFrame(conn, 5*time.Second)
	if err != nil {
		return "", fmt.Errorf("read hello: %w", err)
	}
	from, err := decodeHello(body)
	if err != nil {
		return "", err
	}
	if err := writeFrame(conn, encodeHello(t.id)); err != nil {
		return "", fmt.Errorf("write hello reply: %w", err)
	}
	return from, nil
}

// serveRequests reads request frames in a loop, dispatches to the
// handler, and writes responses back on the same connection.
func (t *TCPTransport) serveRequests(conn net.Conn, from NodeID) {
	for {
		body, err := readFrame(conn, 0)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) && !t.closed.Load() {
				t.log.Debug("inbound read error", "from", from, "err", err)
			}
			return
		}
		if err := t.handleOneInbound(conn, from, body); err != nil {
			t.log.Debug("inbound write error", "from", from, "err", err)
			return
		}
	}
}

func (t *TCPTransport) handleOneInbound(conn net.Conn, from NodeID, body []byte) error {
	kind, reqID, msg, err := decodeRPC(body)
	if err != nil || kind != frameRequest {
		return fmt.Errorf("inbound frame: %w (kind=%d)", err, kind)
	}
	resp := t.callHandler(from, msg)
	if resp == nil {
		// Handler declined; close the conn to surface the failure to the
		// sender so its Send ctx fires rather than blocking forever.
		return errors.New("handler returned nil")
	}
	return t.writeResponse(conn, reqID, resp)
}

func (t *TCPTransport) callHandler(from NodeID, msg Message) Message {
	if t.handler == nil {
		return nil
	}
	return t.handler(from, msg)
}

func (t *TCPTransport) writeResponse(conn net.Conn, reqID uint64, resp Message) error {
	body, err := encodeRPC(frameResponse, reqID, resp)
	if err != nil {
		return err
	}
	return writeFrame(conn, body)
}

// ---------------------------------------------------------------------------
// Dial side
// ---------------------------------------------------------------------------

// getOrDialOutbound returns the existing outboundConn for `to`, or dials
// a fresh one on first use / after a previous one failed.
func (t *TCPTransport) getOrDialOutbound(to NodeID) (*outboundConn, error) {
	if oc, ok := t.outbound.Load(to); ok {
		if conn := oc.(*outboundConn); !conn.isClosed() {
			return conn, nil
		}
	}
	t.dialMu.Lock()
	defer t.dialMu.Unlock()
	if oc, ok := t.outbound.Load(to); ok {
		if conn := oc.(*outboundConn); !conn.isClosed() {
			return conn, nil
		}
	}
	return t.dialFresh(to)
}

func (t *TCPTransport) dialFresh(to NodeID) (*outboundConn, error) {
	addrAny, ok := t.addrs.Load(to)
	if !ok {
		return nil, fmt.Errorf("raft: no address for peer %q", to)
	}
	conn, err := net.DialTimeout("tcp", addrAny.(string), 3*time.Second)
	if err != nil {
		return nil, fmt.Errorf("raft: dial %s: %w", to, err)
	}
	if err := t.clientHandshake(conn, to); err != nil {
		conn.Close()
		return nil, err
	}
	oc := newOutboundConn(conn, t)
	t.outbound.Store(to, oc)
	t.wg.Add(1)
	go func() { defer t.wg.Done(); oc.runReader() }()
	return oc, nil
}

func (t *TCPTransport) clientHandshake(conn net.Conn, want NodeID) error {
	if err := writeFrame(conn, encodeHello(t.id)); err != nil {
		return fmt.Errorf("write hello: %w", err)
	}
	body, err := readFrame(conn, 5*time.Second)
	if err != nil {
		return fmt.Errorf("read hello reply: %w", err)
	}
	from, err := decodeHello(body)
	if err != nil {
		return err
	}
	if from != want {
		return fmt.Errorf("raft: peer at %q identified as %q, want %q", conn.RemoteAddr(), from, want)
	}
	return nil
}

func (t *TCPTransport) dropOutbound(id NodeID) {
	if oc, ok := t.outbound.LoadAndDelete(id); ok {
		oc.(*outboundConn).close()
	}
}

// ---------------------------------------------------------------------------
// outboundConn — the dialer side of a connection.
// ---------------------------------------------------------------------------

type outboundConn struct {
	conn      net.Conn
	writeMu   sync.Mutex
	nextReqID atomic.Uint64
	pending   sync.Map // reqID → chan rpcReplyResult
	owner     *TCPTransport
	closed    atomic.Bool
}

type rpcReplyResult struct {
	msg Message
	err error
}

func newOutboundConn(conn net.Conn, owner *TCPTransport) *outboundConn {
	return &outboundConn{conn: conn, owner: owner}
}

func (oc *outboundConn) isClosed() bool { return oc.closed.Load() }

func (oc *outboundConn) close() {
	if !oc.closed.CompareAndSwap(false, true) {
		return
	}
	_ = oc.conn.Close()
	oc.failAllPending(errors.New("raft: connection closed"))
}

func (oc *outboundConn) failAllPending(err error) {
	oc.pending.Range(func(_, v any) bool {
		select {
		case v.(chan rpcReplyResult) <- rpcReplyResult{err: err}:
		default:
		}
		return true
	})
}

// send writes a request frame and blocks for the response or ctx.
func (oc *outboundConn) send(ctx context.Context, req Message) (Message, error) {
	reqID := oc.nextReqID.Add(1)
	body, err := encodeRPC(frameRequest, reqID, req)
	if err != nil {
		return nil, err
	}
	reply := make(chan rpcReplyResult, 1)
	oc.pending.Store(reqID, reply)
	defer oc.pending.Delete(reqID)
	if err := oc.writeFrameLocked(body); err != nil {
		oc.close()
		return nil, err
	}
	return oc.awaitReply(ctx, reply)
}

func (oc *outboundConn) writeFrameLocked(body []byte) error {
	oc.writeMu.Lock()
	defer oc.writeMu.Unlock()
	return writeFrame(oc.conn, body)
}

func (oc *outboundConn) awaitReply(ctx context.Context, reply chan rpcReplyResult) (Message, error) {
	select {
	case r := <-reply:
		return r.msg, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// runReader drains response frames, demuxing by reqID to the matching
// pending channel. Exits on connection error / close.
func (oc *outboundConn) runReader() {
	for {
		body, err := readFrame(oc.conn, 0)
		if err != nil {
			oc.close()
			return
		}
		oc.dispatchInbound(body)
	}
}

func (oc *outboundConn) dispatchInbound(body []byte) {
	kind, reqID, msg, err := decodeRPC(body)
	if err != nil || kind != frameResponse {
		oc.owner.log.Debug("outbound conn: bad inbound frame", "err", err)
		return
	}
	if v, ok := oc.pending.LoadAndDelete(reqID); ok {
		select {
		case v.(chan rpcReplyResult) <- rpcReplyResult{msg: msg}:
		default:
		}
	}
}
