package replication

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

// WitnessLivenessThreshold is how long since the last heartbeat
// before the witness considers a peer "not alive." Sized to be
// comfortably larger than HeartbeatInterval (500ms) so transient
// blips don't trigger spurious failovers.
const WitnessLivenessThreshold = 3 * time.Second

// witnessPeerEntry tracks one connected peer's last-seen time and
// claimed epoch. The witness keys the table by NodeID so reconnects
// from the same node refresh the existing record rather than
// creating a duplicate.
type witnessPeerEntry struct {
	nodeID   string
	role     Role
	epoch    uint64
	lastSeen time.Time
}

// WitnessServer is the daemon-side state machine for witness mode.
// It tracks each connected peer's last heartbeat and endorses an
// authoritative epoch.
//
// In-memory only. On restart the witness loses its endorsement
// state — see the package docs for caveats and the
// WitnessLivenessThreshold-bounded recovery window.
type WitnessServer struct {
	log *slog.Logger

	mu       sync.Mutex
	peers    map[string]*witnessPeerEntry // nodeID → entry
	endorsed witnessEndorsement
	listener net.Listener

	stopOnce sync.Once
	stop     chan struct{}
	wg       sync.WaitGroup
}

type witnessEndorsement struct {
	nodeID string
	epoch  uint64
}

// NewWitnessServer constructs a witness. Call Start to begin
// listening; Stop to shut down.
func NewWitnessServer(log *slog.Logger) *WitnessServer {
	if log == nil {
		log = slog.Default()
	}
	return &WitnessServer{
		log:   log,
		peers: make(map[string]*witnessPeerEntry),
		stop:  make(chan struct{}),
	}
}

// Start binds to listenAddr and accepts incoming connections.
// Optional TLS via tlsCfg.
func (w *WitnessServer) Start(ctx context.Context, listenAddr string, tlsCfg *tls.Config) error {
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("witness listen: %w", err)
	}
	if tlsCfg != nil {
		lis = tls.NewListener(lis, tlsCfg)
	}
	w.mu.Lock()
	w.listener = lis
	w.mu.Unlock()
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		_ = runPeerListener(ctx, lis, func(pc *peerConn) {
			w.handleConn(ctx, pc)
		})
	}()
	w.log.Info("witness: listening", "addr", lis.Addr())
	return nil
}

// Stop terminates the witness cleanly. Idempotent.
func (w *WitnessServer) Stop() {
	w.stopOnce.Do(func() { close(w.stop) })
	w.mu.Lock()
	if w.listener != nil {
		_ = w.listener.Close()
		w.listener = nil
	}
	w.mu.Unlock()
	w.wg.Wait()
}

// Addr returns the address the witness is listening on, or "" if
// not yet started. Useful for tests that need the bound port.
func (w *WitnessServer) Addr() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.listener == nil {
		return ""
	}
	return w.listener.Addr().String()
}

func (w *WitnessServer) handleConn(ctx context.Context, pc *peerConn) {
	defer pc.Close(nil)

	// First frame must be a witness Hello.
	hello, err := pc.ReadFrame()
	if err != nil {
		return
	}
	if hello.Type != FrameWitnessHello || hello.WitnessHello == nil {
		w.log.Warn("witness: first frame was not wit_hello", "type", hello.Type)
		return
	}
	if hello.WitnessHello.ProtoVer != ProtoVersion {
		w.log.Warn("witness: proto mismatch",
			"theirs", hello.WitnessHello.ProtoVer, "ours", ProtoVersion)
		return
	}

	nodeID := hello.WitnessHello.NodeID
	w.recordHeartbeat(nodeID, hello.WitnessHello.Role, hello.WitnessHello.Epoch)
	w.log.Info("witness: peer connected",
		"node", nodeID, "role", hello.WitnessHello.Role, "epoch", hello.WitnessHello.Epoch)

	defer func() {
		w.log.Info("witness: peer disconnected", "node", nodeID)
	}()

	// Drain frames. A given peer may send heartbeats, queries, or
	// endorsements over its lifetime.
	drainFramesUntilClose(ctx, pc, func(f *Frame) error {
		switch f.Type {
		case FrameHeartbeat:
			if f.Heartbeat != nil {
				w.recordHeartbeat(nodeID, hello.WitnessHello.Role, f.Heartbeat.Epoch)
			}
			return nil
		case FrameWitnessQuery:
			status := w.statusForQuery()
			return pc.WriteFrame(&Frame{Type: FrameWitnessStatus, WitnessStatus: status})
		case FrameWitnessEndorse:
			if f.WitnessEndorse != nil {
				w.recordEndorsement(f.WitnessEndorse.NodeID, f.WitnessEndorse.Epoch)
			}
			// Echo the endorsed status back so the caller can confirm.
			status := w.statusForQuery()
			return pc.WriteFrame(&Frame{Type: FrameWitnessStatus, WitnessStatus: status})
		default:
			return nil
		}
	})
}

func (w *WitnessServer) recordHeartbeat(nodeID string, role Role, epoch uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := time.Now()
	entry, ok := w.peers[nodeID]
	if !ok {
		entry = &witnessPeerEntry{nodeID: nodeID, role: role}
		w.peers[nodeID] = entry
	}
	entry.role = role
	if epoch > entry.epoch {
		entry.epoch = epoch
	}
	entry.lastSeen = now
	// Auto-update endorsement: if a primary connects with higher
	// epoch than currently endorsed, accept it. This is how the
	// witness recovers after its own restart — the live primary
	// re-asserts itself on the next heartbeat.
	if role == RolePrimary && epoch > w.endorsed.epoch {
		w.endorsed = witnessEndorsement{nodeID: nodeID, epoch: epoch}
	}
}

func (w *WitnessServer) recordEndorsement(nodeID string, epoch uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if epoch <= w.endorsed.epoch {
		// Refuse to regress endorsement.
		return
	}
	w.endorsed = witnessEndorsement{nodeID: nodeID, epoch: epoch}
	// The endorsing peer is now the primary in our view.
	if entry, ok := w.peers[nodeID]; ok {
		entry.role = RolePrimary
		entry.epoch = epoch
		entry.lastSeen = time.Now()
	} else {
		w.peers[nodeID] = &witnessPeerEntry{
			nodeID: nodeID, role: RolePrimary, epoch: epoch, lastSeen: time.Now(),
		}
	}
	w.log.Warn("witness: endorsed new primary", "node", nodeID, "epoch", epoch)
}

func (w *WitnessServer) statusForQuery() *WitnessStatus {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := time.Now()
	st := &WitnessStatus{
		EndorsedNodeID: w.endorsed.nodeID,
		EndorsedEpoch:  w.endorsed.epoch,
	}
	if w.endorsed.nodeID != "" {
		if entry, ok := w.peers[w.endorsed.nodeID]; ok {
			st.PrimaryLastSeen = entry.lastSeen.UnixNano()
			st.PrimaryAlive = now.Sub(entry.lastSeen) < WitnessLivenessThreshold
		}
	}
	return st
}

// ---------------------------------------------------------------------------
// Witness client (used by primary and secondary to talk to witness)
// ---------------------------------------------------------------------------

// witnessClient is the per-peer-side connection to a witness. The
// primary uses it to advertise liveness; the secondary uses it to
// query liveness on failover.
type witnessClient struct {
	addr   string
	tlsCfg *tls.Config
	role   Role
	nodeID string

	mu       sync.Mutex
	conn     *peerConn
	epoch    atomic.Uint64
	stopOnce sync.Once
	stop     chan struct{}
	log      *slog.Logger
}

func newWitnessClient(addr string, tlsCfg *tls.Config, role Role, nodeID string, epoch uint64, log *slog.Logger) *witnessClient {
	wc := &witnessClient{
		addr:   addr,
		tlsCfg: tlsCfg,
		role:   role,
		nodeID: nodeID,
		stop:   make(chan struct{}),
		log:    log,
	}
	wc.epoch.Store(epoch)
	return wc
}

// Start spawns the connect-and-heartbeat goroutine.
func (wc *witnessClient) Start(ctx context.Context) {
	go wc.run(ctx)
}

// Stop terminates the witness client cleanly.
func (wc *witnessClient) Stop() {
	wc.stopOnce.Do(func() { close(wc.stop) })
	wc.mu.Lock()
	if wc.conn != nil {
		wc.conn.Close(nil)
		wc.conn = nil
	}
	wc.mu.Unlock()
}

// SetEpoch updates the epoch the client advertises in heartbeats.
func (wc *witnessClient) SetEpoch(e uint64) {
	wc.epoch.Store(e)
}

// Endorse sends a WitnessEndorse frame and reads back the status.
// Returns the witness's view of the cluster after recording the
// endorsement.
func (wc *witnessClient) Endorse(epoch uint64) (*WitnessStatus, error) {
	wc.mu.Lock()
	conn := wc.conn
	wc.mu.Unlock()
	if conn == nil {
		return nil, errors.New("witness client: not connected")
	}
	if err := conn.WriteFrame(&Frame{
		Type:           FrameWitnessEndorse,
		WitnessEndorse: &WitnessEndorse{NodeID: wc.nodeID, Epoch: epoch},
	}); err != nil {
		return nil, err
	}
	// The next frame should be a status response.
	resp, err := conn.ReadFrame()
	if err != nil {
		return nil, err
	}
	if resp.Type != FrameWitnessStatus || resp.WitnessStatus == nil {
		return nil, fmt.Errorf("witness: unexpected response to endorse: %s", resp.Type)
	}
	return resp.WitnessStatus, nil
}

// Query asks the witness for current status. Used by secondaries on
// peer-loss to decide whether to auto-promote.
func (wc *witnessClient) Query() (*WitnessStatus, error) {
	wc.mu.Lock()
	conn := wc.conn
	wc.mu.Unlock()
	if conn == nil {
		return nil, errors.New("witness client: not connected")
	}
	if err := conn.WriteFrame(&Frame{
		Type:         FrameWitnessQuery,
		WitnessQuery: &WitnessQuery{},
	}); err != nil {
		return nil, err
	}
	resp, err := conn.ReadFrame()
	if err != nil {
		return nil, err
	}
	if resp.Type != FrameWitnessStatus || resp.WitnessStatus == nil {
		return nil, fmt.Errorf("witness: unexpected response to query: %s", resp.Type)
	}
	return resp.WitnessStatus, nil
}

func (wc *witnessClient) run(ctx context.Context) {
	for {
		select {
		case <-wc.stop:
			return
		case <-ctx.Done():
			return
		default:
		}
		wc.session(ctx)
		select {
		case <-wc.stop:
			return
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (wc *witnessClient) session(ctx context.Context) {
	pc, err := dialPeer(ctx, wc.addr, wc.tlsCfg, 5*time.Second)
	if err != nil {
		return
	}
	defer pc.Close(nil)

	if err := pc.WriteFrame(&Frame{
		Type: FrameWitnessHello,
		WitnessHello: &WitnessHello{
			Role:     wc.role,
			NodeID:   wc.nodeID,
			Epoch:    wc.epoch.Load(),
			ProtoVer: ProtoVersion,
		},
	}); err != nil {
		return
	}

	wc.mu.Lock()
	wc.conn = pc
	wc.mu.Unlock()
	defer func() {
		wc.mu.Lock()
		if wc.conn == pc {
			wc.conn = nil
		}
		wc.mu.Unlock()
	}()

	// Heartbeat loop. Note: this runs alongside any explicit
	// Endorse/Query calls; the connection's framing is sequential
	// because we hold writeMu inside WriteFrame.
	t := time.NewTicker(HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-pc.Done():
			return
		case <-wc.stop:
			return
		case <-t.C:
			if err := pc.WriteFrame(&Frame{
				Type: FrameHeartbeat,
				Heartbeat: &Heartbeat{
					Epoch: wc.epoch.Load(),
					Now:   time.Now().UnixNano(),
				},
			}); err != nil {
				pc.Close(err)
				return
			}
		}
	}
}
