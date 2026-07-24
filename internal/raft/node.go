package raft

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

// role is a Raft node's current role.
type role uint8

const (
	roleFollower role = iota
	rolePreCandidate
	roleCandidate
	roleLeader
)

func (r role) String() string {
	switch r {
	case roleFollower:
		return "follower"
	case rolePreCandidate:
		return "pre-candidate"
	case roleCandidate:
		return "candidate"
	case roleLeader:
		return "leader"
	default:
		return fmt.Sprintf("role(%d)", uint8(r))
	}
}

// Node is one Raft member. A single goroutine — the run loop started by
// Start — owns all consensus state; callers interact through the methods
// below, which marshal requests into the loop over channels. Close stops
// the loop and joins it.
//
// Note: Node does not own its Storage or Transport — the caller passes
// them in and is responsible for closing them after Close returns.
type Node struct {
	cfg       Config
	log       *raftLog
	transport Transport
	fsm       FSM
	logger    *slog.Logger

	// run-loop-owned state
	role     role
	term     Term
	votedFor NodeID
	leaderID NodeID
	config   Configuration
	cfgIndex Index // log index of the EntryConfig in effect (0 = bootstrap)

	electionElapsed   int
	heartbeatElapsed  int
	randomizedTimeout int // election timeout for this cycle, in ticks

	progress map[NodeID]*peerProgress // leader-only
	votes    map[NodeID]bool          // current (pre)election tally
	preVote  bool                     // tally is for a PreVote round

	// applyDispatched is the highest index already handed to the apply
	// goroutine; the run loop uses it to decide which entries to ship on
	// the next commit advance. The apply goroutine's actual progress may
	// lag behind this — proposals (which it owns) carry their own state.
	applyDispatched Index
	// proposals holds futures the local proposer (this leader) is waiting
	// on. The run loop transfers proposal ownership to the apply
	// goroutine when the entry is sent to apply.
	proposals map[Index]*proposal

	rng *rand.Rand

	// lastSnapshotIdx is the apply goroutine's record of the highest log
	// index it has snapshotted. Only the apply goroutine touches it after
	// Start; the constructor seeds it from the on-disk snapshot meta.
	lastSnapshotIdx Index

	// rpcWG joins every goroutine sendRPC spawns. Close waits on it so
	// no in-flight RPC outlives the node — this also gives TSAN clean
	// happens-before edges between successive RPCs that reuse memory.
	rpcWG sync.WaitGroup

	// channels into the run loop
	tickc       chan struct{}
	recvc       chan rpcRequest
	rpcReplyc   chan rpcReply
	proposec    chan *proposal
	confchangec chan *confChange
	// controlc carries low-frequency "do this on the run loop" closures —
	// Status, TransferLeadership — so the run loop's select stays small.
	controlc  chan func()
	applyc    chan applyReq    // run loop → apply goroutine
	snapSavec chan snapSaveReq // apply goroutine → run loop
	stopc     chan struct{}
	donec     chan struct{}
	applyDone chan struct{} // closed when the apply goroutine exits
	stopOnce  sync.Once     // Close and fatal storage faults share stopc

	// counters records monotonic operational metrics (proposals, applies,
	// leader-change count). Updated from many goroutines under atomics;
	// readers call Snapshot for a consistent read.
	counters *Counters
}

// confChange is one membership change submitted to the run loop. It is
// turned into an EntryConfig by onConfChange and the future is resolved
// when the entry commits (or with ErrLeadershipLost on stepdown).
type confChange struct {
	add    bool // true = AddVoter; false = RemoveServer
	id     NodeID
	addr   string
	future *Future
}

// snapSaveReq asks the run loop to persist a snapshot that the apply
// goroutine has already captured + serialized. All storage writes happen
// on the run loop so the embedded memLog stays single-threaded.
type snapSaveReq struct {
	meta SnapshotMeta
	data []byte
}

// proposal pairs an unappended-to-the-log payload with the Future that
// will deliver its FSM result.
type proposal struct {
	data   []byte
	typ    EntryType
	future *Future
}

// applyReq is one item the run loop hands to the apply goroutine. It's
// either a batch of committed entries (`entries != nil`) or an FSM
// restore from a just-installed snapshot (`restoreData != nil`). They
// are mutually exclusive; the apply goroutine handles each in FIFO order
// so a restore correctly invalidates anything queued before it.
type applyReq struct {
	// applyEntries form
	entries   []Entry
	proposals map[Index]*proposal
	// configAtBatch is the cluster configuration in effect after applying
	// the batch; used to stamp snapshot meta if the batch triggers one.
	configAtBatch Configuration
	// applyRestore form
	restoreData []byte
	restoreMeta SnapshotMeta
}

// NodeStatus is a point-in-time view of a node's consensus state.
type NodeStatus struct {
	ID                NodeID
	Role              string
	Term              Term
	LeaderID          NodeID
	CommitIndex       Index
	LogFirstIndex     Index // one past the latest snapshot; equals 1 if none
	LastLogIndex      Index
	LastSnapshotIndex Index // 0 if no snapshot yet
	Voters            []NodeID
}

// peerProgress tracks one follower's replication state (leader-only).
type peerProgress struct {
	nextIndex        Index
	matchIndex       Index
	snapshotInFlight bool
}

// rpcRequest is an inbound RPC handed to the run loop with a reply channel.
type rpcRequest struct {
	from  NodeID
	msg   Message
	reply chan Message
}

// rpcReply is the result of an RPC this node sent, fed back to the run loop.
type rpcReply struct {
	from NodeID
	req  Message
	msg  Message
	err  error
}

// NewNode constructs a Node. config is the cluster configuration in
// effect at startup (from a snapshot, or — for a fresh bootstrap — the
// initial member set); it is overridden by any EntryConfig already in the
// persisted log. fsm receives committed entries in index order from a
// dedicated goroutine; pass NewNoopFSM() if the application has none.
// The node does no work until Start is called.
func NewNode(cfg Config, fsm FSM, storage Storage, transport Transport, config Configuration, logger *slog.Logger) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if fsm == nil {
		fsm = NewNoopFSM()
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(noopWriter{}, nil))
	}
	rl, err := newRaftLog(storage)
	if err != nil {
		return nil, err
	}
	n := newNode(cfg, fsm, rl, transport, config, logger)
	if err := n.recoverState(storage); err != nil {
		return nil, err
	}
	if err := n.restoreFSMFromSnapshot(storage); err != nil {
		return nil, err
	}
	return n, nil
}

func newNode(cfg Config, fsm FSM, rl *raftLog, transport Transport, config Configuration, logger *slog.Logger) *Node {
	return &Node{
		cfg: cfg, log: rl, transport: transport, fsm: fsm, logger: logger.With("node", cfg.ID),
		role: roleFollower, config: config.Clone(),
		proposals:   map[Index]*proposal{},
		rng:         rand.New(rand.NewSource(int64(crc([]byte(cfg.ID))) ^ time.Now().UnixNano())),
		tickc:       make(chan struct{}, 1),
		recvc:       make(chan rpcRequest),
		rpcReplyc:   make(chan rpcReply, 64),
		proposec:    make(chan *proposal),
		confchangec: make(chan *confChange),
		controlc:    make(chan func()),
		applyc:      make(chan applyReq, cfg.ApplyChanDepth),
		snapSavec:   make(chan snapSaveReq, 1),
		stopc:       make(chan struct{}),
		donec:       make(chan struct{}),
		applyDone:   make(chan struct{}),
		counters:    &Counters{},
	}
}

// restoreFSMFromSnapshot loads the latest persisted snapshot into the
// FSM (if any) and seeds applyDispatched + the log's committed bound so
// the apply pipeline picks up exactly the entries past the snapshot.
func (n *Node) restoreFSMFromSnapshot(storage Storage) error {
	meta, ok := storage.SnapshotMeta()
	if !ok {
		return nil
	}
	rc, err := storage.OpenSnapshot()
	if err != nil {
		return err
	}
	defer rc.Close()
	if err := n.fsm.Restore(rc); err != nil {
		return err
	}
	n.applyDispatched = meta.LastIncludedIndex
	n.lastSnapshotIdx = meta.LastIncludedIndex
	if n.log.committed < meta.LastIncludedIndex {
		n.log.committed = meta.LastIncludedIndex
	}
	return nil
}

// recoverState loads the persisted term/vote and adopts any
// configuration changes that postdate the recovery configuration.
func (n *Node) recoverState(storage Storage) error {
	hs, err := storage.LoadHardState()
	if err != nil {
		return err
	}
	n.term, n.votedFor = hs.CurrentTerm, hs.VotedFor
	if meta, ok := storage.SnapshotMeta(); ok && len(meta.Configuration.Voters) > 0 {
		n.config = meta.Configuration.Clone()
		n.cfgIndex = meta.LastIncludedIndex
	}
	return n.replayConfigEntries()
}

// replayConfigEntries adopts the last EntryConfig in the log (if any) —
// configuration takes effect on append, so the most recent one wins.
func (n *Node) replayConfigEntries() error {
	for i := n.log.lastIndex(); i >= n.log.firstIndex(); i-- {
		es, err := n.log.entries(i, i+1)
		if err != nil || len(es) == 0 {
			break
		}
		if es[0].Type == EntryConfig {
			cfg, err := decodeConfig(es[0].Data)
			if err != nil {
				return fmt.Errorf("raft: decode persisted config at %d: %w", i, err)
			}
			n.config, n.cfgIndex = cfg, i
			return nil
		}
	}
	return nil
}

// Start launches the run loop, the internal ticker, and the apply
// goroutine. It returns immediately; the node begins as a follower.
func (n *Node) Start() {
	n.transport.SetHandler(n.handleRPC)
	n.syncTransportPeers()
	n.resetElectionTimer()
	go n.run()
	go n.tickLoop()
	go n.runApply()
	n.dispatchPendingApply() // kick off any entries already committed-but-unapplied from disk
}

// Close stops the run loop and the apply goroutine, waits for both to
// exit, then joins every in-flight RPC goroutine. Idempotent.
func (n *Node) Close() error {
	n.requestStop()
	<-n.donec
	<-n.applyDone
	n.rpcWG.Wait()
	return nil
}

func (n *Node) requestStop() {
	n.stopOnce.Do(func() { close(n.stopc) })
}

func (n *Node) stopping() bool {
	select {
	case <-n.stopc:
		return true
	default:
		return false
	}
}

// tickLoop drives the run loop's logical clock at HeartbeatInterval. It
// exits when the node stops.
func (n *Node) tickLoop() {
	t := time.NewTicker(n.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-n.stopc:
			return
		case <-t.C:
			select {
			case n.tickc <- struct{}{}:
			default: // a tick is already queued; the loop is busy — fine
			}
		}
	}
}

// run is the single-goroutine event loop. Every case is a short handler.
func (n *Node) run() {
	defer n.shutdownRunLoop()
	for {
		select {
		case <-n.stopc:
			return
		case <-n.tickc:
			n.onTick()
		case req := <-n.recvc:
			n.onRPC(req)
		case rep := <-n.rpcReplyc:
			n.onRPCReply(rep)
		case p := <-n.proposec:
			n.onPropose(p)
		case cc := <-n.confchangec:
			n.onConfChange(cc)
		case fn := <-n.controlc:
			fn()
		case s := <-n.snapSavec:
			n.onSnapshotSave(s)
		}
		if n.stopping() {
			return
		}
	}
}

// onSnapshotSave persists a snapshot the apply goroutine captured. It is
// the only writer of storage's snapshot file outside of installSnapshot.
func (n *Node) onSnapshotSave(s snapSaveReq) {
	if err := n.log.storage.SaveSnapshot(s.meta, bytes.NewReader(s.data)); err != nil {
		n.failStorage("save snapshot", err)
	}
}

func (n *Node) shutdownRunLoop() {
	n.failPendingProposals(ErrStopped)
	close(n.applyc) // tells the apply goroutine to drain and exit
	close(n.donec)
}

// Status returns a snapshot of this node's consensus state. After Close
// it returns the zero value.
func (n *Node) Status() NodeStatus {
	replyc := make(chan NodeStatus, 1)
	select {
	case n.controlc <- func() { replyc <- n.snapshotStatus() }:
	case <-n.donec:
		return NodeStatus{ID: n.cfg.ID}
	}
	select {
	case st := <-replyc:
		return st
	case <-n.donec:
		return NodeStatus{ID: n.cfg.ID}
	}
}

func (n *Node) snapshotStatus() NodeStatus {
	st := NodeStatus{
		ID: n.cfg.ID, Role: n.role.String(), Term: n.term, LeaderID: n.leaderID,
		CommitIndex: n.log.committed, LogFirstIndex: n.log.firstIndex(),
		LastLogIndex: n.log.lastIndex(), Voters: n.config.IDs(),
	}
	if meta, ok := n.log.storage.SnapshotMeta(); ok {
		st.LastSnapshotIndex = meta.LastIncludedIndex
	}
	return st
}

// IsLeader reports whether this node currently believes it is the leader.
func (n *Node) IsLeader() bool { return n.Status().Role == roleLeader.String() }

// LeaderID returns the id of the node this one currently believes is
// leader, or "" if unknown.
func (n *Node) LeaderID() NodeID { return n.Status().LeaderID }

// handleRPC is the Transport's inbound handler. It runs on a transport
// goroutine, hands the request to the run loop, and waits for the reply.
// On stop it returns nil — the caller treats that as "no reply" and
// times out — because n.term cannot safely be read off the run loop.
func (n *Node) handleRPC(from NodeID, msg Message) Message {
	reply := make(chan Message, 1)
	select {
	case n.recvc <- rpcRequest{from: from, msg: msg, reply: reply}:
	case <-n.stopc:
		return nil
	}
	select {
	case m := <-reply:
		return m
	case <-n.stopc:
		return nil
	}
}

// ---------------------------------------------------------------------------
// tick
// ---------------------------------------------------------------------------

func (n *Node) onTick() {
	if n.role == roleLeader {
		n.tickHeartbeat()
		return
	}
	n.tickElection()
}

func (n *Node) tickElection() {
	n.electionElapsed++
	if n.electionElapsed >= n.randomizedTimeout && n.isVoter(n.cfg.ID) {
		n.startElection()
	}
}

func (n *Node) tickHeartbeat() {
	n.heartbeatElapsed++
	if n.heartbeatElapsed >= 1 {
		n.heartbeatElapsed = 0
		n.broadcastHeartbeat()
	}
}

func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	span := int(n.cfg.ElectionTimeoutMax-n.cfg.ElectionTimeoutMin) / int(n.cfg.HeartbeatInterval)
	min := int(n.cfg.ElectionTimeoutMin) / int(n.cfg.HeartbeatInterval)
	if min < 1 {
		min = 1
	}
	if span <= 0 {
		n.randomizedTimeout = min
		return
	}
	n.randomizedTimeout = min + n.rng.Intn(span+1)
}

// ---------------------------------------------------------------------------
// role transitions
// ---------------------------------------------------------------------------

// becomeFollower drops to follower at term t with the given leader (which
// may be empty). It persists if the term or vote changed and fails any
// pending proposals — a stepped-down leader's commands won't get the
// chance to commit, so the caller must learn and retry against the new
// leader.
func (n *Node) becomeFollower(t Term, leader NodeID) {
	changed := t != n.term
	if t > n.term {
		n.term, n.votedFor = t, ""
	}
	n.role, n.leaderID, n.progress = roleFollower, leader, nil
	n.clearVotes()
	n.failPendingProposals(ErrLeadershipLost)
	n.resetElectionTimer()
	if changed {
		n.persistHardState()
	}
}

func (n *Node) becomeLeader() {
	n.role, n.leaderID = roleLeader, n.cfg.ID
	n.clearVotes()
	n.progress = make(map[NodeID]*peerProgress, len(n.config.Voters))
	last := n.log.lastIndex()
	for id := range n.config.Voters {
		n.progress[id] = &peerProgress{nextIndex: last + 1}
	}
	n.heartbeatElapsed = 0
	n.counters.IncLeaderChange()
	n.logger.Info("became leader", "term", n.term)
	n.appendLeaderNoop()
	if n.stopping() {
		return
	}
	n.broadcastAppendEntries()
}

// appendLeaderNoop appends an empty entry of the new leader's term so a
// quorum acknowledging it sweeps any stale earlier-term entries into the
// committed range (Raft's commit-only-over-current-term workaround).
func (n *Node) appendLeaderNoop() {
	entry := Entry{Index: n.log.lastIndex() + 1, Term: n.term, Type: EntryNoOp}
	if err := n.log.append([]Entry{entry}); err != nil {
		n.failStorage("append leader no-op", err)
		return
	}
	n.advanceSelfProgress(entry.Index)
	n.maybeAdvanceCommit() // single-node clusters commit immediately
}

func (n *Node) clearVotes() {
	n.votes, n.preVote = nil, false
}

// ---------------------------------------------------------------------------
// hard state persistence
// ---------------------------------------------------------------------------

// persistHardState fsyncs term/vote/commit. A failure stops the node: merely
// stepping down is insufficient because a follower could still grant votes or
// acknowledge entries from state that will disappear after a restart.
func (n *Node) persistHardState() bool {
	hs := HardState{CurrentTerm: n.term, VotedFor: n.votedFor, CommitIndex: n.log.committed}
	if err := n.log.storage.SaveHardState(hs); err != nil {
		n.failStorage("save hard state", err)
		return false
	}
	return true
}

func (n *Node) failStorage(op string, err error) {
	n.logger.Error("fatal storage failure; stopping node", "operation", op, "err", err)
	n.requestStop()
}

// ---------------------------------------------------------------------------
// configuration helpers
// ---------------------------------------------------------------------------

func (n *Node) isVoter(id NodeID) bool { return n.config.Has(id) }

func (n *Node) quorum() int { return n.config.Quorum() }

// peerIDs returns every voter other than this node.
func (n *Node) peerIDs() []NodeID {
	out := make([]NodeID, 0, len(n.config.Voters))
	for id := range n.config.Voters {
		if id != n.cfg.ID {
			out = append(out, id)
		}
	}
	return out
}

func (n *Node) syncTransportPeers() {
	for id, addr := range n.config.Voters {
		if id != n.cfg.ID {
			n.transport.AddPeer(id, addr)
		}
	}
}

// ---------------------------------------------------------------------------
// sending RPCs (always off the run loop)
// ---------------------------------------------------------------------------

// sendRPC delivers req to `to` on a fresh goroutine and feeds the reply
// (or error) back into the run loop. The context bounds the wait so a
// dead peer doesn't leak the goroutine forever. Tracked through rpcWG
// so Close() can join every in-flight send.
func (n *Node) sendRPC(to NodeID, req Message) {
	n.rpcWG.Add(1)
	go n.doSendRPC(to, req)
}

func (n *Node) doSendRPC(to NodeID, req Message) {
	defer n.rpcWG.Done()
	ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout())
	defer cancel()
	resp, err := n.transport.Send(ctx, to, req)
	select {
	case n.rpcReplyc <- rpcReply{from: to, req: req, msg: resp, err: err}:
	case <-n.stopc:
	}
}

func (n *Node) rpcTimeout() time.Duration {
	// Generous: a slow reply should still come back rather than be
	// abandoned, but not so long that a partitioned peer's goroutine
	// lingers across many election cycles.
	return n.cfg.ElectionTimeoutMax
}

// ---------------------------------------------------------------------------
// misc
// ---------------------------------------------------------------------------

// noopWriter discards log output (used when the caller passes a nil logger).
type noopWriter struct{}

func (noopWriter) Write(p []byte) (int, error) { return len(p), nil }
