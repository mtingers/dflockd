package raft

import (
	"bytes"
	"errors"
)

// onSnapshotSave accepts a captured local snapshot. FileStorage prepares the
// snapshot file and compacted WAL generation off-loop; simpler Storage
// implementations keep the synchronous path.
func (n *Node) onSnapshotSave(req snapSaveReq) {
	store, ok := n.log.storage.(asyncSnapshotStorage)
	if !ok {
		n.saveSnapshotSync(req)
		return
	}
	if n.snapshotInFlight {
		n.deferSnapshot(req)
		return
	}
	n.startSnapshotPreparation(store, req)
}

func (n *Node) saveSnapshotSync(req snapSaveReq) {
	if err := n.log.storage.SaveSnapshot(req.meta, bytes.NewReader(req.data)); err != nil {
		n.failStorage("save snapshot", err)
	}
}

func (n *Node) deferSnapshot(req snapSaveReq) {
	if n.pendingSnapshot != nil &&
		n.pendingSnapshot.meta.LastIncludedIndex >= req.meta.LastIncludedIndex {
		return
	}
	pending := req
	n.pendingSnapshot = &pending
}

func (n *Node) startSnapshotPreparation(store asyncSnapshotStorage, req snapSaveReq) {
	tail, err := n.snapshotTail(req.meta.LastIncludedIndex)
	if errors.Is(err, errSnapshotSuperseded) {
		n.startPendingSnapshot()
		return
	}
	if err != nil {
		n.failStorage("read snapshot tail", err)
		return
	}
	through := req.meta.LastIncludedIndex
	if len(tail) > 0 {
		through = tail[len(tail)-1].Index
	}
	n.snapshotInFlight = true
	n.snapshotWG.Add(1)
	go n.prepareSnapshot(store, req, tail, through)
}

func (n *Node) snapshotTail(snapshotIndex Index) ([]Entry, error) {
	if snapshotIndex < n.log.firstIndex()-1 {
		return nil, errSnapshotSuperseded
	}
	last := n.log.lastIndex()
	if snapshotIndex >= last {
		return nil, nil
	}
	return n.log.entries(snapshotIndex+1, last+1)
}

func (n *Node) prepareSnapshot(store asyncSnapshotStorage, req snapSaveReq, tail []Entry, through Index) {
	defer n.snapshotWG.Done()
	prepared, err := store.prepareSnapshot(req.meta, req.data, tail)
	result := snapshotPrepareResult{
		store: store, req: req, prepared: prepared, baseTail: tail,
		through: through, err: err,
	}
	select {
	case n.snapDonec <- result:
	case <-n.stopc:
		if prepared != nil {
			store.abortPreparedSnapshot(prepared)
		}
	}
}

func (n *Node) onSnapshotPrepared(result snapshotPrepareResult) {
	n.snapshotInFlight = false
	if n.rejectPreparedSnapshot(result) {
		n.startPendingSnapshot()
		return
	}
	n.commitPreparedSnapshot(result)
}

func (n *Node) rejectPreparedSnapshot(result snapshotPrepareResult) bool {
	if result.err != nil {
		if result.prepared != nil {
			result.store.abortPreparedSnapshot(result.prepared)
		}
		if !errors.Is(result.err, errSnapshotSuperseded) {
			n.failStorage("prepare snapshot", result.err)
		}
		return true
	}
	if n.snapshotWasSuperseded(result.req.meta) {
		result.store.abortPreparedSnapshot(result.prepared)
		return true
	}
	if !n.preparedTailMatches(result) {
		result.store.abortPreparedSnapshot(result.prepared)
		n.deferSnapshot(result.req)
		return true
	}
	return false
}

func (n *Node) commitPreparedSnapshot(result snapshotPrepareResult) {
	delta, err := n.snapshotDelta(result.through)
	if err != nil {
		result.store.abortPreparedSnapshot(result.prepared)
		n.failStorage("read snapshot delta", err)
		return
	}
	if err := result.store.commitPreparedSnapshot(result.prepared, delta); err != nil {
		result.store.abortPreparedSnapshot(result.prepared)
		n.failStorage("commit snapshot", err)
		return
	}
	n.startPendingSnapshot()
}

func (n *Node) snapshotWasSuperseded(meta SnapshotMeta) bool {
	return n.log.firstIndex()-1 >= meta.LastIncludedIndex
}

func (n *Node) preparedTailMatches(result snapshotPrepareResult) bool {
	if len(result.baseTail) == 0 {
		return true
	}
	if n.log.lastIndex() < result.through {
		return false
	}
	current, err := n.log.entries(result.req.meta.LastIncludedIndex+1, result.through+1)
	return err == nil && equalEntries(current, result.baseTail)
}

func equalEntries(a, b []Entry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Index != b[i].Index || a[i].Term != b[i].Term ||
			a[i].Type != b[i].Type || !bytes.Equal(a[i].Data, b[i].Data) {
			return false
		}
	}
	return true
}

func (n *Node) snapshotDelta(through Index) ([]Entry, error) {
	last := n.log.lastIndex()
	if through >= last {
		return nil, nil
	}
	return n.log.entries(through+1, last+1)
}

func (n *Node) startPendingSnapshot() {
	if n.stopping() || n.pendingSnapshot == nil {
		return
	}
	next := *n.pendingSnapshot
	n.pendingSnapshot = nil
	n.onSnapshotSave(next)
}
