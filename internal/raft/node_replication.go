package raft

// This file holds the AppendEntries / InstallSnapshot / TimeoutNow paths.
// In this milestone the leader sends only heartbeats (empty AppendEntries)
// — log shipping, the apply pipeline, and snapshot transfer arrive in
// later phases — but the follower-side handlers and the leader's progress
// bookkeeping are written in full so the wire behaviour is already correct.

// ---------------------------------------------------------------------------
// Leader → follower
// ---------------------------------------------------------------------------

// broadcastHeartbeat sends an AppendEntries to every follower. (Currently
// always heartbeat-only; entry shipping is a later phase.)
func (n *Node) broadcastHeartbeat() {
	for _, id := range n.peerIDs() {
		n.sendAppendEntries(id, true)
	}
}

// sendAppendEntries sends an AppendEntries to `to`. If heartbeatOnly (or
// the follower is fully caught up) it carries no entries; otherwise it
// carries entries from the follower's nextIndex, capped by config.
// If the follower's nextIndex is at or before our log start, an
// InstallSnapshot is sent instead.
func (n *Node) sendAppendEntries(to NodeID, heartbeatOnly bool) {
	p := n.progress[to]
	if p == nil {
		return
	}
	if p.nextIndex < n.log.firstIndex() {
		n.sendInstallSnapshot(to)
		return
	}
	req := n.buildAppendEntries(p, heartbeatOnly)
	if req == nil {
		n.sendInstallSnapshot(to)
		return
	}
	n.sendRPC(to, req)
}

func (n *Node) buildAppendEntries(p *peerProgress, heartbeatOnly bool) *AppendEntriesReq {
	prev := p.nextIndex - 1
	prevTerm, err := n.log.term(prev)
	if err != nil {
		return nil // prev compacted away -> caller falls back to a snapshot
	}
	var entries []Entry
	if !heartbeatOnly {
		if entries, err = n.log.entriesFrom(p.nextIndex, n.cfg.MaxAppendEntries); err != nil {
			return nil
		}
	}
	return &AppendEntriesReq{Term: n.term, LeaderID: n.cfg.ID, PrevLogIndex: prev, PrevLogTerm: prevTerm, Entries: entries, LeaderCommit: n.log.committed}
}

// handleAppendEntriesResp updates the follower's progress and may advance
// the commit index. Stale replies (we're no longer leader at this term)
// are ignored.
func (n *Node) handleAppendEntriesResp(from NodeID, resp *AppendEntriesResp) {
	if n.role != roleLeader {
		return
	}
	p := n.progress[from]
	if p == nil {
		return
	}
	if resp.Success {
		n.onAppendSuccess(from, p, resp.MatchIndex)
		return
	}
	n.onAppendConflict(from, p, resp)
}

func (n *Node) onAppendSuccess(from NodeID, p *peerProgress, matchIndex Index) {
	if matchIndex > p.matchIndex {
		p.matchIndex = matchIndex
	}
	if next := p.matchIndex + 1; next > p.nextIndex {
		p.nextIndex = next
	}
	n.maybeAdvanceCommit()
}

func (n *Node) onAppendConflict(from NodeID, p *peerProgress, resp *AppendEntriesResp) {
	p.nextIndex = n.backoffNextIndex(p.nextIndex, resp)
	if p.nextIndex < 1 {
		p.nextIndex = 1
	}
	n.sendAppendEntries(from, false) // retry promptly from the new point
}

// backoffNextIndex picks the follower's new nextIndex from its conflict
// hint: jump to where its log ends (ConflictTerm 0), or back to the start
// of the conflicting term — but no further than one below the old value.
func (n *Node) backoffNextIndex(old Index, resp *AppendEntriesResp) Index {
	cand := resp.ConflictIndex
	if cand == 0 || cand >= old {
		cand = old - 1
	}
	if cand < 1 {
		cand = 1
	}
	return cand
}

// maybeAdvanceCommit raises commitIndex to the highest index replicated on
// a quorum, provided that entry is from the current term (Raft §5.4.2).
func (n *Node) maybeAdvanceCommit() {
	n.maybeCommitTo(n.quorumMatchIndex())
}

func (n *Node) maybeCommitTo(cand Index) {
	if cand <= n.log.committed {
		return
	}
	if t, err := n.log.term(cand); err != nil || t != n.term {
		return // can't commit an entry from an earlier term by counting replicas
	}
	n.log.commitTo(cand)
	n.persistHardState()
	n.dispatchPendingApply()
	n.broadcastHeartbeat() // let followers learn the new commit promptly
}

// quorumMatchIndex returns the largest index N such that a quorum of
// voters have matchIndex >= N.
func (n *Node) quorumMatchIndex() Index {
	matches := make([]Index, 0, len(n.config.Voters))
	for id := range n.config.Voters {
		matches = append(matches, n.matchIndexOf(id))
	}
	return kthLargest(matches, n.quorum())
}

func (n *Node) matchIndexOf(id NodeID) Index {
	if id == n.cfg.ID {
		return n.log.lastIndex()
	}
	if p := n.progress[id]; p != nil {
		return p.matchIndex
	}
	return 0
}

// kthLargest returns the k-th largest element of xs (1-indexed), or 0 if
// k is out of range. xs is sorted in place.
func kthLargest(xs []Index, k int) Index {
	if k < 1 || k > len(xs) {
		return 0
	}
	sortIndicesDesc(xs)
	return xs[k-1]
}

func sortIndicesDesc(xs []Index) {
	for i := 1; i < len(xs); i++ {
		for j := i; j > 0 && xs[j-1] < xs[j]; j-- {
			xs[j-1], xs[j] = xs[j], xs[j-1]
		}
	}
}

// ---------------------------------------------------------------------------
// Follower side: AppendEntries
// ---------------------------------------------------------------------------

func (n *Node) handleAppendEntries(from NodeID, req *AppendEntriesReq) *AppendEntriesResp {
	if req.Term < n.term {
		return &AppendEntriesResp{Term: n.term, Success: false}
	}
	n.becomeFollower(req.Term, req.LeaderID)
	return n.appendOrReject(req)
}

func (n *Node) appendOrReject(req *AppendEntriesReq) *AppendEntriesResp {
	if req.PrevLogIndex < n.log.firstIndex()-1 {
		// prevLogIndex predates our snapshot — tell the leader to resume
		// from just past it.
		return &AppendEntriesResp{Term: n.term, Success: false, ConflictIndex: n.log.firstIndex(), ConflictTerm: 0}
	}
	if !n.log.matchTerm(req.PrevLogIndex, req.PrevLogTerm) {
		ci, ct := n.log.conflictHint(req.PrevLogIndex)
		return &AppendEntriesResp{Term: n.term, Success: false, ConflictIndex: ci, ConflictTerm: ct}
	}
	return n.applyAppendEntries(req)
}

func (n *Node) applyAppendEntries(req *AppendEntriesReq) *AppendEntriesResp {
	through, err := n.log.appendFromLeader(req.PrevLogIndex, req.Entries)
	if err != nil {
		n.logger.Error("append from leader failed", "err", err)
		return &AppendEntriesResp{Term: n.term, Success: false}
	}
	n.advanceFollowerCommit(req.LeaderCommit, through)
	return &AppendEntriesResp{Term: n.term, Success: true, MatchIndex: through}
}

func (n *Node) advanceFollowerCommit(leaderCommit, lastNew Index) {
	target := leaderCommit
	if lastNew < target {
		target = lastNew
	}
	if target <= n.log.committed {
		return
	}
	n.log.commitTo(target)
	n.persistHardState()
	n.dispatchPendingApply()
}

// ---------------------------------------------------------------------------
// InstallSnapshot — wired but the leader-side send is a later phase.
// ---------------------------------------------------------------------------

func (n *Node) sendInstallSnapshot(to NodeID) {
	meta, ok := n.log.storage.SnapshotMeta()
	if !ok {
		return // nothing to send yet
	}
	rc, err := n.log.storage.OpenSnapshot()
	if err != nil {
		n.logger.Error("open snapshot for send failed", "err", err)
		return
	}
	data, err := readAllAndClose(rc)
	if err != nil {
		n.logger.Error("read snapshot for send failed", "err", err)
		return
	}
	n.sendRPC(to, &InstallSnapshotReq{Term: n.term, LeaderID: n.cfg.ID, Meta: meta, Data: data})
}

func (n *Node) handleInstallSnapshot(from NodeID, req *InstallSnapshotReq) *InstallSnapshotResp {
	if req.Term < n.term {
		return &InstallSnapshotResp{Term: n.term}
	}
	n.becomeFollower(req.Term, req.LeaderID)
	last, err := n.log.installSnapshot(req.Meta, req.Data)
	if err != nil {
		n.logger.Error("install snapshot failed", "err", err)
		return &InstallSnapshotResp{Term: n.term}
	}
	n.persistHardState()
	n.scheduleFSMRestore(req.Meta, req.Data)
	return &InstallSnapshotResp{Term: n.term, LastIndex: last}
}

// scheduleFSMRestore queues an FSM-restore through the apply pipeline so
// it's serialised with any in-flight Apply calls (the apply goroutine is
// the only writer of FSM state). After the send, advance applyDispatched
// to the snapshot index so subsequent dispatches start past it.
func (n *Node) scheduleFSMRestore(meta SnapshotMeta, data []byte) {
	select {
	case n.applyc <- applyReq{restoreData: data, restoreMeta: meta}:
		if meta.LastIncludedIndex > n.applyDispatched {
			n.applyDispatched = meta.LastIncludedIndex
		}
	case <-n.stopc:
	}
}

func (n *Node) handleInstallSnapshotResp(from NodeID, resp *InstallSnapshotResp) {
	if n.role != roleLeader {
		return
	}
	if p := n.progress[from]; p != nil && resp.LastIndex > 0 {
		p.matchIndex = resp.LastIndex
		p.nextIndex = resp.LastIndex + 1
		n.maybeAdvanceCommit()
	}
}

// ---------------------------------------------------------------------------
// TimeoutNow — a leadership-transfer target campaigns at once.
// ---------------------------------------------------------------------------

func (n *Node) handleTimeoutNow(req *TimeoutNowReq) *TimeoutNowResp {
	if req.Term >= n.term && n.isVoter(n.cfg.ID) {
		n.campaign()
	}
	return &TimeoutNowResp{Term: n.term}
}
