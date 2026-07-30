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
	if len(data) > maxEntryDataBytes {
		return nil, fmt.Errorf("raft: %w: %d bytes exceeds max %d", ErrEntryTooLarge, len(data), maxEntryDataBytes)
	}
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
	if err := n.lifecycleErr(); err != nil {
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
		n.counters.IncProposalsFailed()
		p.future.resolve(nil, ErrNotLeader)
		return
	}
	if len(p.data) > maxEntryDataBytes {
		n.counters.IncProposalsFailed()
		p.future.resolve(nil, fmt.Errorf("raft: %w: %d bytes exceeds max %d", ErrEntryTooLarge, len(p.data), maxEntryDataBytes))
		return
	}
	entry := Entry{Index: n.log.lastIndex() + 1, Term: n.term, Type: p.typ, Data: p.data}
	if err := n.log.append([]Entry{entry}); err != nil {
		n.counters.IncProposalsFailed()
		p.future.resolve(nil, err)
		n.failStorage("append proposal", err)
		return
	}
	n.counters.IncProposals()
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
		n.sendAppendEntries(id)
	}
}

// AddVoter proposes that node (id, addr) be added to the cluster as a
// voting member. The future resolves once the EntryConfig commits.
// Returns ErrNotLeader if this node isn't the leader at submission.
func (n *Node) AddVoter(ctx context.Context, id NodeID, addr string) (*Future, error) {
	return n.submitConfChange(ctx, &confChange{add: true, id: id, addr: addr, future: newFuture()})
}

// AddVoterWithMetadata adds a voter and replicates its client-facing address
// in the same configuration entry.
func (n *Node) AddVoterWithMetadata(ctx context.Context, id NodeID, addr, clientAddr string) (*Future, error) {
	return n.submitConfChange(ctx, &confChange{
		add: true, id: id, addr: addr, clientAddr: clientAddr, future: newFuture(),
	})
}

// RemoveServer proposes removing node id from the cluster.
func (n *Node) RemoveServer(ctx context.Context, id NodeID) (*Future, error) {
	return n.submitConfChange(ctx, &confChange{add: false, id: id, future: newFuture()})
}

func (n *Node) submitConfChange(ctx context.Context, cc *confChange) (*Future, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := n.lifecycleErr(); err != nil {
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

// TransferLeadership asks this node (which must be the leader) to hand
// leadership to its most-caught-up follower by sending it a TimeoutNow,
// so the successor is elected within one round trip instead of waiting
// out an election timeout — useful for a graceful rolling restart.
// Returns ErrNotLeader if not leader, or an error if no follower is
// caught up enough. It does not block for the successor to win.
func (n *Node) TransferLeadership(ctx context.Context) error {
	if err := n.lifecycleErr(); err != nil {
		return err
	}
	done := make(chan error, 1)
	select {
	case n.controlc <- func() { n.onTransferLeadership(done) }:
	case <-ctx.Done():
		return ctx.Err()
	case <-n.donec:
		return ErrStopped
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-n.donec:
		return ErrStopped
	}
}

// onTransferLeadership picks the best successor and fires a TimeoutNow at
// it (run-loop handler).
func (n *Node) onTransferLeadership(done chan error) {
	if n.role != roleLeader {
		done <- ErrNotLeader
		return
	}
	target := n.bestTransferTarget()
	if target == "" {
		done <- fmt.Errorf("raft: no follower is caught up enough to transfer leadership to")
		return
	}
	n.logger.Info("transferring leadership", "to", target, "term", n.term)
	n.sendRPC(target, &TimeoutNowReq{Term: n.term, LeaderID: n.cfg.ID})
	done <- nil
}

// bestTransferTarget returns the voter (other than us) with the highest
// matchIndex, provided it's at least our committed index — anything less
// couldn't satisfy the election restriction (Raft §5.4.1). "" if none.
func (n *Node) bestTransferTarget() NodeID {
	var best NodeID
	var bestMatch Index
	for id := range n.config.Voters {
		if id == n.cfg.ID {
			continue
		}
		p := n.progress[id]
		if p == nil || p.matchIndex < n.log.committed {
			continue
		}
		if best == "" || p.matchIndex > bestMatch {
			best, bestMatch = id, p.matchIndex
		}
	}
	return best
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
	if n.cfgIndex > n.log.committed {
		cc.future.resolve(nil, ErrConfigChangeInProgress)
		return
	}
	newCfg, err := buildNewConfig(n.config, cc)
	if err != nil {
		cc.future.resolve(nil, err)
		return
	}
	data, err := encodeRPCConfig(newCfg)
	if err != nil {
		cc.future.resolve(nil, err)
		return
	}
	entry := Entry{Index: n.log.lastIndex() + 1, Term: n.term, Type: EntryConfig, Data: data}
	if err := n.log.append([]Entry{entry}); err != nil {
		cc.future.resolve(nil, err)
		n.failStorage("append configuration", err)
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
	if cc.id == "" {
		return Configuration{}, fmt.Errorf("raft: voter ID is required")
	}
	newCfg := oldCfg.Clone()
	if cc.add {
		if cc.addr == "" {
			return Configuration{}, fmt.Errorf("raft: voter address is required")
		}
		if _, exists := newCfg.Voters[cc.id]; exists {
			return Configuration{}, fmt.Errorf("raft: %w: %q", ErrAlreadyVoter, cc.id)
		}
		for id, addr := range newCfg.Voters {
			if addr == cc.addr {
				return Configuration{}, fmt.Errorf("raft: voter address %q already belongs to %q", cc.addr, id)
			}
		}
		newCfg.Voters[cc.id] = cc.addr
		switch {
		case newCfg.ClientAddrs != nil && cc.clientAddr == "":
			return Configuration{}, fmt.Errorf("raft: client address required for voter %q", cc.id)
		case newCfg.ClientAddrs != nil:
			for id, addr := range newCfg.ClientAddrs {
				if addr == cc.clientAddr {
					return Configuration{}, fmt.Errorf("raft: client address %q already belongs to %q", cc.clientAddr, id)
				}
			}
			newCfg.ClientAddrs[cc.id] = cc.clientAddr
		case cc.clientAddr != "":
			return Configuration{}, fmt.Errorf("raft: existing configuration has no replicated client metadata")
		}
		return newCfg, nil
	}
	if _, exists := newCfg.Voters[cc.id]; !exists {
		return Configuration{}, fmt.Errorf("raft: %w: %q not a voter", ErrUnknownPeer, cc.id)
	}
	if len(newCfg.Voters) == 1 {
		return Configuration{}, ErrLastVoter
	}
	delete(newCfg.Voters, cc.id)
	if newCfg.ClientAddrs != nil {
		delete(newCfg.ClientAddrs, cc.id)
	}
	return newCfg, nil
}

// adoptConfig installs newCfg as this node's effective Configuration.
// Updates transport peers and (if leader) per-peer progress.
func (n *Node) adoptConfig(newCfg Configuration, idx Index) {
	newCfg = n.withBootstrapClientMetadata(newCfg)
	old := n.config
	n.config = newCfg.Clone()
	n.cfgIndex = idx
	n.syncTransportForConfig(old, newCfg)
	if n.role == roleLeader {
		n.refreshProgress(newCfg)
	}
	n.publishLeadership()
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
