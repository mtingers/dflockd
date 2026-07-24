package raft

import (
	"context"
	"crypto/tls"
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
// Every connection uses a shared-secret challenge-response handshake,
// then protects RPC frames with a derived AEAD session. Optional mutual
// TLS additionally binds each hello NodeID to the certificate Common Name.
type rpcHandler = func(from NodeID, req Message) Message

// TCPTransport is the production raft.Transport. It owns a listener
// for inbound RPCs, lazily-dialed outbound connections per peer, and
// background goroutines for accept + per-conn read loops. Construct
// via NewTCPTransport; install the dispatcher via SetHandler before
// Start; cleanup via Close (idempotent, joins all goroutines).
//
// Concurrency: safe for concurrent Send calls (one outbound conn per
// peer is reused, with serialised writes). The atomic handler pointer
// and the sync.Maps avoid locking on the hot path.
type TCPTransport struct {
	id            NodeID
	listener      net.Listener
	tlsCfg        *tls.Config                // non-nil → mutual TLS on every conn
	clusterSecret []byte                     // HMAC handshake + AEAD key source
	handler       atomic.Pointer[rpcHandler] // set via SetHandler; read off accept goroutines
	addrs         sync.Map                   // NodeID → string
	outbound      sync.Map                   // NodeID → *outboundConn
	accepted      sync.Map                   // net.Conn → struct{} (open accepted conns)
	lastDialFail  sync.Map                   // NodeID → time.Time (dial-failure cool-down)
	dialMu        sync.Mutex                 // serialises dial-on-demand
	log           *slog.Logger
	wg            sync.WaitGroup
	closed        atomic.Bool
}

var _ Transport = (*TCPTransport)(nil)

// errTransportClosed is returned by Send once Close has been called.
var errTransportClosed = errors.New("raft: transport closed")

// TCPOption configures a TCPTransport at construction time.
type TCPOption func(*tcpOptions)

type tcpOptions struct {
	tlsCfg        *tls.Config
	clusterSecret []byte
}

// WithTLS makes every inter-node connection use cfg (mutual TLS — see
// NewMutualTLSConfig). A nil cfg leaves shared-secret AEAD without TLS.
func WithTLS(cfg *tls.Config) TCPOption { return func(o *tcpOptions) { o.tlsCfg = cfg } }

// WithClusterSecret authenticates peers and encrypts every Raft RPC frame.
// The secret must contain at least 32 bytes of operator-supplied entropy.
func WithClusterSecret(secret string) TCPOption {
	return func(o *tcpOptions) { o.clusterSecret = append([]byte(nil), secret...) }
}

// NewTCPTransport listens on listenAddr ("host:port"; "0" gets a free
// port) and returns a Transport bound to id. Call SetHandler before
// expecting inbound RPCs to deliver. WithClusterSecret is required;
// pass WithTLS(cfg) to add mutual TLS and certificate identity binding.
func NewTCPTransport(id NodeID, listenAddr string, logger *slog.Logger, opts ...TCPOption) (*TCPTransport, error) {
	if logger == nil {
		logger = slog.Default()
	}
	var o tcpOptions
	for _, opt := range opts {
		opt(&o)
	}
	if err := validateTCPOptions(id, o); err != nil {
		return nil, err
	}
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("raft: tcp listen %s: %w", listenAddr, err)
	}
	if o.tlsCfg != nil {
		lis = tls.NewListener(lis, o.tlsCfg)
	}
	t := &TCPTransport{
		id: id, listener: lis, tlsCfg: o.tlsCfg,
		clusterSecret: o.clusterSecret, log: logger.With("transport", id),
	}
	t.wg.Add(1)
	go t.acceptLoop()
	return t, nil
}

func validateTCPOptions(id NodeID, o tcpOptions) error {
	if id == "" {
		return fmt.Errorf("raft: TCP transport node ID is required")
	}
	if len(id) >= maxString16 {
		return fmt.Errorf("raft: TCP transport node ID exceeds %d bytes", maxString16-1)
	}
	if len(o.clusterSecret) < minClusterSecretBytes {
		return fmt.Errorf("raft: cluster secret must be at least %d bytes", minClusterSecretBytes)
	}
	return nil
}

// LocalID implements raft.Transport.
func (t *TCPTransport) LocalID() NodeID { return t.id }

// ListenAddr returns the resolved listen address (useful when the
// caller bound to ":0" and needs the assigned port).
func (t *TCPTransport) ListenAddr() string { return t.listener.Addr().String() }

// SetHandler implements raft.Transport. Stored as an atomic.Pointer
// so the accept loop reads it lock-free per inbound RPC.
func (t *TCPTransport) SetHandler(h func(from NodeID, req Message) Message) {
	t.handler.Store(&h)
}

// AddPeer implements raft.Transport. Updates the peer-id → address
// map; the actual TCP connection is dialed lazily on first Send.
func (t *TCPTransport) AddPeer(id NodeID, addr string) { t.addrs.Store(id, addr) }

// RemovePeer implements raft.Transport. Drops the address, clears the
// dial-failure cool-down, and closes any open outbound connection to
// the peer.
func (t *TCPTransport) RemovePeer(id NodeID) {
	t.addrs.Delete(id)
	t.lastDialFail.Delete(id)
	t.dropOutbound(id)
}

// Close implements raft.Transport. Stops the accept loop, closes the
// listener and every active outbound connection, and waits for all
// background goroutines to exit. Idempotent — second and subsequent
// calls return nil immediately.
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
	if t.closed.Load() {
		return nil, errTransportClosed
	}
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
		if t.closed.Load() {
			_ = conn.Close() // accepted in the Close() race window
			return
		}
		t.wg.Add(1)
		go t.serveAccepted(conn)
	}
}

func (t *TCPTransport) serveAccepted(conn net.Conn) {
	defer t.wg.Done()
	tuneConn(conn)
	t.accepted.Store(conn, struct{}{})
	defer func() { t.accepted.Delete(conn); _ = conn.Close() }()
	if t.closed.Load() {
		return // Close() may have already finished its accepted.Range
	}
	from, session, err := t.serverHandshake(conn)
	if err != nil {
		t.log.Debug("inbound handshake failed", "err", err)
		return
	}
	t.serveRequests(conn, from, session)
}

func (t *TCPTransport) serverHandshake(conn net.Conn) (NodeID, *secureSession, error) {
	client, err := t.readClientHello(conn)
	if err != nil {
		return "", nil, err
	}
	server, err := t.exchangeServerProof(conn, client)
	if err != nil {
		return "", nil, err
	}
	session, err := newSecureSession(t.clusterSecret, client, server, false)
	if err != nil {
		return "", nil, err
	}
	return client.id, session, nil
}

func (t *TCPTransport) readClientHello(conn net.Conn) (handshakeHello, error) {
	body, err := readFrame(conn, handshakeTimeout)
	if err != nil {
		return handshakeHello{}, fmt.Errorf("read hello: %w", err)
	}
	client, err := decodeHello(body)
	if err != nil {
		return handshakeHello{}, err
	}
	if err := verifyClientHello(t.clusterSecret, client); err != nil {
		return handshakeHello{}, err
	}
	if err := verifyPeerTLSIdentity(conn, client.id); err != nil {
		return handshakeHello{}, err
	}
	return client, nil
}

func (t *TCPTransport) exchangeServerProof(conn net.Conn, client handshakeHello) (handshakeHello, error) {
	server, err := newServerHello(t.clusterSecret, client, t.id)
	if err != nil {
		return handshakeHello{}, err
	}
	if err := writeFrameTo(conn, encodeHello(server), handshakeTimeout); err != nil {
		return handshakeHello{}, fmt.Errorf("write hello reply: %w", err)
	}
	body, err := readFrame(conn, handshakeTimeout)
	if err != nil {
		return handshakeHello{}, fmt.Errorf("read client auth: %w", err)
	}
	proof, err := decodeAuth(body)
	if err != nil {
		return handshakeHello{}, err
	}
	if err := verifyClientFinal(t.clusterSecret, client, server, proof); err != nil {
		return handshakeHello{}, err
	}
	return server, nil
}

// serveRequests reads request frames in a loop, dispatches to the
// handler, and writes responses back on the same connection. A
// connIdleTimeout on each read recycles a peer that has gone silent.
func (t *TCPTransport) serveRequests(conn net.Conn, from NodeID, session *secureSession) {
	for {
		body, err := readSecureFrame(conn, session, connIdleTimeout)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) && !t.closed.Load() {
				t.log.Debug("inbound read error", "from", from, "err", err)
			}
			return
		}
		if err := t.handleOneInbound(conn, session, from, body); err != nil {
			t.log.Debug("inbound dispatch error", "from", from, "err", err)
			return
		}
	}
}

func (t *TCPTransport) handleOneInbound(conn net.Conn, session *secureSession, from NodeID, body []byte) error {
	kind, reqID, msg, err := decodeRPC(body)
	if err != nil {
		return fmt.Errorf("inbound frame decode: %w", err)
	}
	if kind != frameRequest {
		return fmt.Errorf("inbound frame: unexpected kind %d", kind)
	}
	resp := t.callHandler(from, msg)
	if resp == nil {
		// No handler installed yet, or the handler declined (e.g. node
		// shutting down). Close the conn so the sender's Send ctx fires
		// promptly rather than blocking for its full timeout; the peer
		// redials when it next needs us.
		return errors.New("handler returned nil")
	}
	return t.writeResponse(conn, session, reqID, resp)
}

func (t *TCPTransport) callHandler(from NodeID, msg Message) Message {
	hp := t.handler.Load()
	if hp == nil {
		return nil
	}
	return (*hp)(from, msg)
}

func (t *TCPTransport) writeResponse(conn net.Conn, session *secureSession, reqID uint64, resp Message) error {
	body, err := encodeRPC(frameResponse, reqID, resp)
	if err != nil {
		return err
	}
	return writeSecureFrameTo(conn, session, body, writeTimeout)
}

// tuneConn enables TCP keepalive so a peer whose host disappeared
// (no RST/FIN) is detected without waiting out connIdleTimeout. It
// unwraps a *tls.Conn to reach the underlying socket.
func tuneConn(c net.Conn) {
	switch v := c.(type) {
	case *net.TCPConn:
		_ = v.SetKeepAlive(true)
		_ = v.SetKeepAlivePeriod(tcpKeepAlivePeriod)
	case *tls.Conn:
		tuneConn(v.NetConn())
	}
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
	if t.closed.Load() {
		return nil, errTransportClosed
	}
	if err := t.dialCoolingDown(to); err != nil {
		return nil, err
	}
	return t.dialFresh(to)
}

// dialCoolingDown returns the cached dial error if `to` failed to dial
// within the last dialBackoff (heartbeats fire continuously, so without
// this every heartbeat would re-dial a downed peer).
func (t *TCPTransport) dialCoolingDown(to NodeID) error {
	if v, ok := t.lastDialFail.Load(to); ok {
		if since := time.Since(v.(time.Time)); since < dialBackoff {
			return fmt.Errorf("raft: peer %q dial cooling down (%s ago)", to, since.Round(time.Millisecond))
		}
	}
	return nil
}

func (t *TCPTransport) dialFresh(to NodeID) (*outboundConn, error) {
	addrAny, ok := t.addrs.Load(to)
	if !ok {
		return nil, fmt.Errorf("raft: no address for peer %q", to)
	}
	conn, err := t.dial(addrAny.(string))
	if err != nil {
		t.lastDialFail.Store(to, time.Now())
		return nil, fmt.Errorf("raft: dial %s: %w", to, err)
	}
	tuneConn(conn)
	session, err := t.clientHandshake(conn, to)
	if err != nil {
		conn.Close()
		t.lastDialFail.Store(to, time.Now())
		return nil, err
	}
	t.lastDialFail.Delete(to)
	oc := newOutboundConn(conn, session, t)
	t.outbound.Store(to, oc)
	t.wg.Add(1)
	go func() { defer t.wg.Done(); oc.runReader() }()
	return oc, nil
}

// dial opens a TCP connection (TLS-wrapped when configured). The 3 s
// budget covers connect + the TLS handshake as a whole.
func (t *TCPTransport) dial(addr string) (net.Conn, error) {
	d := &net.Dialer{Timeout: 3 * time.Second}
	if t.tlsCfg != nil {
		return tls.DialWithDialer(d, "tcp", addr, t.tlsCfg)
	}
	return d.Dial("tcp", addr)
}

func (t *TCPTransport) clientHandshake(conn net.Conn, want NodeID) (*secureSession, error) {
	client, err := newClientHello(t.clusterSecret, t.id)
	if err != nil {
		return nil, err
	}
	if err := writeFrameTo(conn, encodeHello(client), handshakeTimeout); err != nil {
		return nil, fmt.Errorf("write hello: %w", err)
	}
	body, err := readFrame(conn, handshakeTimeout)
	if err != nil {
		return nil, fmt.Errorf("read hello reply: %w", err)
	}
	server, err := decodeHello(body)
	if err != nil {
		return nil, err
	}
	if server.id != want {
		return nil, fmt.Errorf("raft: peer at %q identified as %q, want %q", conn.RemoteAddr(), server.id, want)
	}
	if err := verifyServerHello(t.clusterSecret, client, server); err != nil {
		return nil, err
	}
	if err := verifyPeerTLSIdentity(conn, server.id); err != nil {
		return nil, err
	}
	proof := clientFinalProof(t.clusterSecret, client, server)
	if err := writeFrameTo(conn, encodeAuth(proof), handshakeTimeout); err != nil {
		return nil, fmt.Errorf("write client auth: %w", err)
	}
	return newSecureSession(t.clusterSecret, client, server, true)
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
	session   *secureSession
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

func newOutboundConn(conn net.Conn, session *secureSession, owner *TCPTransport) *outboundConn {
	return &outboundConn{conn: conn, session: session, owner: owner}
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
	return writeSecureFrameTo(oc.conn, oc.session, body, writeTimeout)
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
// pending channel. The connIdleTimeout per read means a dead/partitioned
// peer's goroutine exits (and the conn is recycled) within it rather
// than blocking forever. Exits on connection error / close.
func (oc *outboundConn) runReader() {
	for {
		body, err := readSecureFrame(oc.conn, oc.session, connIdleTimeout)
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
