package raft

import (
	"context"
	"fmt"
)

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

// AddVoter proposes that node (id, addr) be added to the cluster as a
// voting member. The future resolves once the EntryConfig commits.
// Returns ErrNotLeader if this node isn't the leader at submission.
func (n *Node) AddVoter(ctx context.Context, id NodeID, addr string) (*Future, error) {
	return n.submitConfChange(ctx, &confChange{add: true, id: id, addr: addr, future: newFuture()})
}

// RemoveServer proposes removing node id from the cluster.
func (n *Node) RemoveServer(ctx context.Context, id NodeID) (*Future, error) {
	return n.submitConfChange(ctx, &confChange{add: false, id: id, future: newFuture()})
}

func (n *Node) submitConfChange(ctx context.Context, cc *confChange) (*Future, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case n.confchangec <- cc:
		return cc.future, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.donec:
		return nil, ErrStopped
	}
}

// onConfChange handles a membership-change request. Leader-only; on a
// follower it surfaces ErrNotLeader to the future. The new
// Configuration takes effect on append (Raft §4.3) — not on commit —
// so quorum is computed against the new set immediately.
func (n *Node) onConfChange(cc *confChange) {
	if n.role != roleLeader {
		cc.future.resolve(nil, ErrNotLeader)
		return
	}
	newCfg, err := buildNewConfig(n.config, cc)
	if err != nil {
		cc.future.resolve(nil, err)
		return
	}
	entry := Entry{Index: n.log.lastIndex() + 1, Term: n.term, Type: EntryConfig, Data: encodeConfig(nil, newCfg)}
	if err := n.log.append([]Entry{entry}); err != nil {
		cc.future.resolve(nil, err)
		return
	}
	n.adoptConfig(newCfg, entry.Index)
	n.proposals[entry.Index] = &proposal{typ: EntryConfig, future: cc.future}
	n.advanceSelfProgress(entry.Index)
	n.broadcastAppendEntries()
	n.maybeAdvanceCommit()
}

// buildNewConfig applies cc to oldCfg and returns the result, or an
// error if the change is a no-op (already present / already absent).
func buildNewConfig(oldCfg Configuration, cc *confChange) (Configuration, error) {
	newCfg := oldCfg.Clone()
	if cc.add {
		if _, exists := newCfg.Voters[cc.id]; exists {
			return Configuration{}, fmt.Errorf("raft: %w: %q already a voter", ErrConfigChangeInProgress, cc.id)
		}
		newCfg.Voters[cc.id] = cc.addr
		return newCfg, nil
	}
	if _, exists := newCfg.Voters[cc.id]; !exists {
		return Configuration{}, fmt.Errorf("raft: %w: %q not a voter", ErrUnknownPeer, cc.id)
	}
	delete(newCfg.Voters, cc.id)
	return newCfg, nil
}

// adoptConfig installs newCfg as this node's effective Configuration.
// Updates transport peers and (if leader) per-peer progress.
func (n *Node) adoptConfig(newCfg Configuration, idx Index) {
	old := n.config
	n.config = newCfg.Clone()
	n.cfgIndex = idx
	n.syncTransportForConfig(old, newCfg)
	if n.role == roleLeader {
		n.refreshProgress(newCfg)
	}
}

func (n *Node) syncTransportForConfig(old, new Configuration) {
	for id, addr := range new.Voters {
		if id == n.cfg.ID {
			continue
		}
		if _, was := old.Voters[id]; !was {
			n.transport.AddPeer(id, addr)
		}
	}
	for id := range old.Voters {
		if _, still := new.Voters[id]; !still {
			n.transport.RemovePeer(id)
		}
	}
}

// refreshProgress drops progress for removed peers and creates baseline
// progress for newly-added peers (leader-only).
func (n *Node) refreshProgress(newCfg Configuration) {
	last := n.log.lastIndex()
	for id := range newCfg.Voters {
		if _, ok := n.progress[id]; !ok {
			n.progress[id] = &peerProgress{nextIndex: last + 1}
		}
	}
	for id := range n.progress {
		if _, ok := newCfg.Voters[id]; !ok {
			delete(n.progress, id)
		}
	}
}
