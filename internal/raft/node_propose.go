package raft

import "context"

// Propose submits an application command to the cluster. The returned
// Future resolves once the entry is applied to this node's FSM (success)
// or rejected (ErrNotLeader before submission, ErrLeadershipLost if the
// leader steps down before commit, ErrStopped on Close). The caller may
// retry by calling Propose again on the new leader.
func (n *Node) Propose(ctx context.Context, data []byte) (*Future, error) {
	return n.submitProposal(ctx, &proposal{data: data, typ: EntryNormal, future: newFuture()})
}

// Barrier proposes a no-op entry and returns a Future that resolves once
// it is applied — useful as a linearizable read barrier (every prior
// proposal has applied by the time the barrier does).
func (n *Node) Barrier(ctx context.Context) (*Future, error) {
	return n.submitProposal(ctx, &proposal{typ: EntryNoOp, future: newFuture()})
}

func (n *Node) submitProposal(ctx context.Context, p *proposal) (*Future, error) {
	// An already-cancelled context must lose to the run loop deterministically;
	// without this check the select races (both cases ready) and the call
	// occasionally submits anyway.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case n.proposec <- p:
		return p.future, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.donec:
		return nil, ErrStopped
	}
}

// ---------------------------------------------------------------------------
// run-loop handler
// ---------------------------------------------------------------------------

// onPropose handles a proposal: if we're the leader, append it as a log
// entry, update our own progress, ship to followers, and (for single-
// node clusters) advance commit immediately.
func (n *Node) onPropose(p *proposal) {
	if n.role != roleLeader {
		p.future.resolve(nil, ErrNotLeader)
		return
	}
	entry := Entry{Index: n.log.lastIndex() + 1, Term: n.term, Type: p.typ, Data: p.data}
	if err := n.log.append([]Entry{entry}); err != nil {
		p.future.resolve(nil, err)
		return
	}
	n.proposals[entry.Index] = p
	n.advanceSelfProgress(entry.Index)
	n.broadcastAppendEntries()
	n.maybeAdvanceCommit()
}

func (n *Node) advanceSelfProgress(idx Index) {
	if me := n.progress[n.cfg.ID]; me != nil {
		me.matchIndex = idx
		me.nextIndex = idx + 1
	}
}

// broadcastAppendEntries ships any unsent log entries (and a heartbeat
// if there are none) to every follower. Called on Propose and on
// becomeLeader.
func (n *Node) broadcastAppendEntries() {
	for _, id := range n.peerIDs() {
		n.sendAppendEntries(id, false)
	}
}
