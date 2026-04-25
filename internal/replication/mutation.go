package replication

// Mutation is the internal record of one state change captured on the
// primary's lock manager and forwarded to the secondary. It is the
// in-memory equivalent of an entry in a Raft log: every primary-side
// state change has exactly one Mutation, identified by a monotonic
// per-epoch sequence number.
//
// Mutation is a pure value type. The lock manager passes one to its
// configured Hook; the Hook is responsible for assigning Seq and
// queueing the mutation onto the peer link. Hook implementations must
// be allocation-light because they are called under sh.mu — the
// outbound network write happens in a separate goroutine that drains
// the queue.
type Mutation struct {
	Kind               OpKind
	Key                string // includes "lock:"/"sem:" prefix
	Token              string
	ConnID             uint64
	Limit              int
	LeaseExpiresUnixNS int64
	LeaseTTLNS         int64
}

// Hook is the lock manager's outlet for state changes. The primary
// installs a Hook backed by the peer link. The secondary leaves Hook
// nil and instead receives state via the lock manager's Apply* entry
// points (see lock package).
//
// Capture is called synchronously while the shard lock is held. The
// returned seq is monotonic across all mutations on this Hook for the
// current epoch. Implementations MUST NOT block on I/O — append to an
// internal queue and return immediately. The replicator goroutine
// drains the queue and writes to the peer; the primary's request
// handler then calls AwaitAcked(seq) before acking the client.
type Hook interface {
	Capture(Mutation) (seq uint64)

	// Epoch returns the current epoch the Hook would assign to a new
	// Mutation. Called by the lock manager when it needs to stamp a
	// mutation that is reported via a non-Capture path.
	Epoch() uint64
}

// NoopHook is a Hook that drops every mutation. Used on standalone
// servers (no replication configured) and in tests that don't care
// about replication. Its Epoch is always 0.
type NoopHook struct{}

func (NoopHook) Capture(Mutation) uint64 { return 0 }
func (NoopHook) Epoch() uint64           { return 0 }
