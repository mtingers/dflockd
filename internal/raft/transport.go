package raft

import "context"

// Transport carries Raft RPCs between nodes. It is request/response: the
// caller's Send blocks until a reply or an error. Implementations must be
// safe for concurrent Send calls and must deliver inbound RPCs to the
// handler installed by SetHandler. AddPeer/RemovePeer let the node keep
// the transport's notion of the cluster in sync with configuration
// changes.
//
// raft.Node owns exactly one Transport. The in-process MemTransport (for
// tests) and a framed-TCP implementation (production) both satisfy it.
type Transport interface {
	// Send delivers req to node `to` and returns its reply. The reply's
	// concrete type matches req's (RequestVoteReq->RequestVoteResp, etc.).
	// A non-nil error means the RPC did not complete (peer down, timeout,
	// transport closed) — the caller treats that as "no reply".
	Send(ctx context.Context, to NodeID, req Message) (Message, error)
	// SetHandler installs the function that answers inbound RPCs. Called
	// once, before the node starts.
	SetHandler(h func(from NodeID, req Message) Message)
	// AddPeer / RemovePeer adjust the set of reachable peers (used on
	// configuration changes). AddPeer with an address already present is
	// a no-op (it may update the address).
	AddPeer(id NodeID, addr string)
	RemovePeer(id NodeID)
	// LocalID returns this transport's node id.
	LocalID() NodeID
	// Close releases resources. Idempotent.
	Close() error
}

// Message is the closed set of Raft RPCs and their replies. Each concrete
// type carries the sender's term so a stale node can be detected and
// stepped down regardless of which RPC it sent.
type Message interface{ messageTerm() Term }

// ---------------------------------------------------------------------------
// RequestVote (also used for the PreVote phase, with PreVote=true; a
// PreVote does not persist a vote or advance any term).
// ---------------------------------------------------------------------------

type RequestVoteReq struct {
	Term         Term
	CandidateID  NodeID
	LastLogIndex Index
	LastLogTerm  Term
	PreVote      bool
}

type RequestVoteResp struct {
	Term        Term
	VoteGranted bool
	PreVote     bool
}

func (m *RequestVoteReq) messageTerm() Term  { return m.Term }
func (m *RequestVoteResp) messageTerm() Term { return m.Term }

// ---------------------------------------------------------------------------
// AppendEntries (heartbeat when Entries is empty).
// ---------------------------------------------------------------------------

type AppendEntriesReq struct {
	Term         Term
	LeaderID     NodeID
	PrevLogIndex Index
	PrevLogTerm  Term
	Entries      []Entry
	LeaderCommit Index
}

type AppendEntriesResp struct {
	Term    Term
	Success bool
	// MatchIndex is the follower's last index that now agrees with the
	// leader (set on success); the leader advances matchIndex to it.
	MatchIndex Index
	// ConflictIndex / ConflictTerm are the back-off hint on failure: the
	// follower's first index of the conflicting term (or its log end+1 if
	// it simply has no entry at PrevLogIndex).
	ConflictIndex Index
	ConflictTerm  Term
}

func (m *AppendEntriesReq) messageTerm() Term  { return m.Term }
func (m *AppendEntriesResp) messageTerm() Term { return m.Term }

// ---------------------------------------------------------------------------
// InstallSnapshot (whole snapshot in one frame; dflockd's state is small).
// ---------------------------------------------------------------------------

type InstallSnapshotReq struct {
	Term     Term
	LeaderID NodeID
	Meta     SnapshotMeta
	Data     []byte
}

type InstallSnapshotResp struct {
	Term Term
	// LastIndex is the follower's log end after installing (== Meta.LastIncludedIndex);
	// the leader sets nextIndex to LastIndex+1.
	LastIndex Index
}

func (m *InstallSnapshotReq) messageTerm() Term  { return m.Term }
func (m *InstallSnapshotResp) messageTerm() Term { return m.Term }

// ---------------------------------------------------------------------------
// TimeoutNow — sent by a leader transferring leadership; the recipient
// starts an election immediately (skipping its election timeout).
// ---------------------------------------------------------------------------

type TimeoutNowReq struct {
	Term     Term
	LeaderID NodeID
}

type TimeoutNowResp struct{ Term Term }

func (m *TimeoutNowReq) messageTerm() Term  { return m.Term }
func (m *TimeoutNowResp) messageTerm() Term { return m.Term }
