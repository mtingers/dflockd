package raft

import (
	"bytes"
	"fmt"
	"runtime/debug"
	"time"
)

// runApply is the dedicated FSM-apply goroutine: it drains committed
// entries the run loop hands over via applyc, in index order, and
// resolves the matching proposal Futures (the run loop transferred
// ownership of each *proposal at dispatch time). It is the only writer
// of FSM state. It exits when applyc is closed (run-loop shutdown).
func (n *Node) runApply() {
	defer close(n.applyDone)
	failed := false
	for req := range n.applyc {
		if failed {
			failApplyProposals(req, ErrStopped)
			continue
		}
		failed = !n.applyBatch(req)
	}
}

// applyBatch returns false after an unrecoverable FSM failure. Once that
// happens runApply drains later batches without touching the FSM so every
// transferred proposal future still resolves during shutdown.
func (n *Node) applyBatch(req applyReq) bool {
	if req.restoreData != nil {
		return n.restoreFSMFromBatch(req)
	}
	for _, e := range req.entries {
		if err := n.applyOne(e, req.proposals[e.Index]); err != nil {
			failApplyProposals(req, ErrStopped)
			return false
		}
	}
	n.maybeSnapshot(req)
	return true
}

// restoreFSMFromBatch services an InstallSnapshot by feeding the
// persisted snapshot bytes to FSM.Restore. A failure leaves the FSM
// indeterminate, so the node must stop rather than acknowledge or apply
// subsequent committed entries against unknown state.
func (n *Node) restoreFSMFromBatch(req applyReq) bool {
	if err := n.fsm.Restore(bytes.NewReader(req.restoreData)); err != nil {
		n.logger.Error("fatal FSM Restore failure; stopping node", "at_index", req.restoreMeta.LastIncludedIndex, "err", err)
		n.requestStop()
		return false
	}
	return true
}

// applyOne calls FSM.Apply for one entry (unless it's a NoOp/Config,
// which the FSM never sees) and resolves the proposer's future if any.
// A panic in Apply is contained, surfaced through the future, and stops
// the node before another committed entry can touch the divergent FSM.
func (n *Node) applyOne(e Entry, p *proposal) error {
	start := time.Now()
	result, applyErr := n.fsmApplySafely(e)
	if applyErr == nil {
		n.counters.IncApply(time.Since(start))
	} else {
		n.counters.IncApplyFailed()
	}
	if p != nil {
		p.future.resolve(result, applyErr)
	}
	return applyErr
}

func (n *Node) fsmApplySafely(e Entry) (result any, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("raft: FSM Apply panicked at index %d: %v", e.Index, r)
			n.logger.Error("fatal FSM Apply panic; stopping node",
				"index", e.Index, "term", e.Term, "recovered", r,
				"stack", string(debug.Stack()))
			n.requestStop()
		}
	}()
	if e.Type != EntryNormal {
		return nil, nil // NoOp / Config entries aren't surfaced to the FSM
	}
	return n.fsm.Apply(e), nil
}

func failApplyProposals(req applyReq, err error) {
	for _, p := range req.proposals {
		p.future.resolve(nil, err)
	}
}

// maybeSnapshot triggers a snapshot if enough entries have accumulated
// past the last one captured. Capture happens on the apply goroutine; storage
// implementations that support preparation do durable work off the Raft loop.
func (n *Node) maybeSnapshot(req applyReq) {
	if !n.snapshotThresholdReached(req) {
		return
	}
	last := req.entries[len(req.entries)-1]
	if err := n.captureAndQueueSnapshot(last.Index, last.Term, req.configAtBatch); err != nil {
		n.logger.Error("snapshot failed", "err", err, "at_index", last.Index)
	}
}

func (n *Node) snapshotThresholdReached(req applyReq) bool {
	if len(req.entries) == 0 || n.cfg.SnapshotThresholdEntries == 0 {
		return false
	}
	last := req.entries[len(req.entries)-1].Index
	return uint64(last-n.lastSnapshotIdx) >= n.cfg.SnapshotThresholdEntries
}

// captureAndQueueSnapshot serializes the FSM into a buffer and hands it
// to the run loop for persistence. Updates n.lastSnapshotIdx so the next
// threshold check is measured from the new high-water.
func (n *Node) captureAndQueueSnapshot(lastIdx Index, lastTerm Term, config Configuration) error {
	snap, err := n.fsm.Snapshot()
	if err != nil {
		return fmt.Errorf("FSM.Snapshot: %w", err)
	}
	defer snap.Release()
	var buf bytes.Buffer
	if err := snap.Persist(&buf); err != nil {
		return fmt.Errorf("FSMSnapshot.Persist: %w", err)
	}
	meta := SnapshotMeta{LastIncludedIndex: lastIdx, LastIncludedTerm: lastTerm, Configuration: config}
	select {
	case n.snapSavec <- snapSaveReq{meta: meta, data: buf.Bytes()}:
		n.lastSnapshotIdx = lastIdx
		return nil
	case <-n.stopc:
		return ErrStopped
	}
}

// ---------------------------------------------------------------------------
// run-loop side of the apply pipeline
// ---------------------------------------------------------------------------

// dispatchPendingApply ships any committed-but-undispatched entries to
// the apply goroutine, transferring ownership of any matching proposals.
func (n *Node) dispatchPendingApply() {
	if n.log.committed <= n.applyDispatched {
		return
	}
	entries, err := n.log.entries(n.applyDispatched+1, n.log.committed+1)
	if err != nil {
		n.logger.Error("dispatch apply: read entries failed", "err", err)
		return
	}
	n.shipApplyBatch(entries)
}

func (n *Node) shipApplyBatch(entries []Entry) {
	req := applyReq{entries: entries, proposals: n.takeMatchingProposals(entries), configAtBatch: n.config.Clone()}
	for {
		select {
		case n.applyc <- req:
			n.applyDispatched = entries[len(entries)-1].Index
			return
		case s := <-n.snapSavec:
			// Keep draining so the apply goroutine can't wedge on a full
			// snapSavec while we're wedged here on a full applyc.
			n.onSnapshotSave(s)
			if n.stopping() {
				for _, p := range req.proposals {
					p.future.resolve(nil, ErrStopped)
				}
				return
			}
		case <-n.stopc:
			// Shutdown in flight — fail the proposals we just claimed.
			for _, p := range req.proposals {
				p.future.resolve(nil, ErrStopped)
			}
			return
		}
	}
}

func (n *Node) takeMatchingProposals(entries []Entry) map[Index]*proposal {
	if len(n.proposals) == 0 {
		return nil
	}
	out := make(map[Index]*proposal, len(entries))
	for _, e := range entries {
		if p, ok := n.proposals[e.Index]; ok {
			out[e.Index] = p
			delete(n.proposals, e.Index)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// failPendingProposals resolves every proposal still in the run loop's
// map with err (used on stepdown / shutdown).
func (n *Node) failPendingProposals(err error) {
	for idx, p := range n.proposals {
		p.future.resolve(nil, err)
		delete(n.proposals, idx)
	}
}
