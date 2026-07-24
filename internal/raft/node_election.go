package raft

// startElection begins a new election cycle. With PreVote enabled the
// node first enters the pre-candidate phase (a "would I win?" poll that
// doesn't touch terms); a pre-vote quorum then promotes it to a real
// candidate. Without PreVote it campaigns directly.
func (n *Node) startElection() {
	if n.cfg.PreVote {
		n.becomePreCandidate()
		return
	}
	n.campaign()
}

func (n *Node) becomePreCandidate() {
	n.role, n.leaderID, n.progress = rolePreCandidate, "", nil
	n.preVote = true
	n.votes = map[NodeID]bool{n.cfg.ID: true} // we'd vote for ourselves
	n.resetElectionTimer()
	n.logger.Debug("starting pre-vote", "for_term", n.term+1)
	n.broadcastRequestVote(true)
	n.maybeWinPreVote()
}

// campaign transitions to candidate: bump the term, vote for self,
// persist, and solicit real votes.
func (n *Node) campaign() {
	n.role, n.leaderID, n.progress = roleCandidate, "", nil
	n.term, n.votedFor = n.term+1, n.cfg.ID
	n.persistHardState()
	n.preVote = false
	n.votes = map[NodeID]bool{n.cfg.ID: true}
	n.resetElectionTimer()
	n.logger.Info("campaigning", "term", n.term)
	n.broadcastRequestVote(false)
	n.maybeWinElection()
}

func (n *Node) broadcastRequestVote(preVote bool) {
	t := n.term
	if preVote {
		t = n.term + 1
	}
	req := &RequestVoteReq{Term: t, CandidateID: n.cfg.ID, LastLogIndex: n.log.lastIndex(), LastLogTerm: n.log.lastTerm(), PreVote: preVote}
	for _, id := range n.peerIDs() {
		n.sendRPC(id, req)
	}
}

// handleRequestVote answers a (pre)vote request.
func (n *Node) handleRequestVote(req *RequestVoteReq) *RequestVoteResp {
	if req.PreVote {
		return n.preVoteResp(req)
	}
	return &RequestVoteResp{Term: n.term, VoteGranted: n.grantVote(req)}
}

// preVoteResp answers a pre-vote. A grant echoes the candidate's
// proposed term so the candidate can tell this reply apart from one for
// an earlier round — our own term may legitimately be lower than the
// term being polled about. A rejection carries our term instead, so a
// candidate behind us learns it and steps down.
func (n *Node) preVoteResp(req *RequestVoteReq) *RequestVoteResp {
	if n.shouldGrantPreVote(req) {
		return &RequestVoteResp{Term: req.Term, VoteGranted: true, PreVote: true}
	}
	return &RequestVoteResp{Term: n.term, VoteGranted: false, PreVote: true}
}

// shouldGrantPreVote: the candidate's log must be at least as up-to-date,
// its hypothetical term must be at least ours, and we must not currently
// be hearing from a leader (else a partitioned node could disrupt a
// healthy cluster).
func (n *Node) shouldGrantPreVote(req *RequestVoteReq) bool {
	if req.Term < n.term || !n.log.isUpToDate(req.LastLogIndex, req.LastLogTerm) {
		return false
	}
	return n.leaderID == "" || n.electionElapsed >= n.minElectionTicks()
}

// grantVote decides a real vote: we may vote if we haven't voted this
// term (or already voted for this candidate) and the candidate's log is
// up-to-date. Granting persists the vote *before* the reply is sent and
// resets the election timer.
func (n *Node) grantVote(req *RequestVoteReq) bool {
	if req.Term < n.term {
		return false
	}
	if n.votedFor != "" && n.votedFor != req.CandidateID {
		return false
	}
	if !n.log.isUpToDate(req.LastLogIndex, req.LastLogTerm) {
		return false
	}
	n.votedFor = req.CandidateID
	n.persistHardState()
	n.resetElectionTimer()
	return true
}

// handleVoteResp counts a (pre)vote reply if it matches our current phase.
func (n *Node) handleVoteResp(from NodeID, resp *RequestVoteResp) {
	if resp.PreVote && n.role == rolePreCandidate {
		n.recordVote(from, resp.VoteGranted)
		n.maybeWinPreVote()
		return
	}
	if !resp.PreVote && n.role == roleCandidate {
		n.recordVote(from, resp.VoteGranted)
		n.maybeWinElection()
	}
}

func (n *Node) recordVote(from NodeID, granted bool) {
	if n.votes == nil {
		n.votes = map[NodeID]bool{}
	}
	if _, seen := n.votes[from]; !seen {
		n.votes[from] = granted
	}
}

func (n *Node) maybeWinPreVote() {
	if n.countGrants() >= n.quorum() {
		n.campaign()
	}
}

func (n *Node) maybeWinElection() {
	if n.countGrants() >= n.quorum() {
		n.becomeLeader()
	}
}

func (n *Node) countGrants() int {
	c := 0
	for id, granted := range n.votes {
		if granted && n.isVoter(id) {
			c++
		}
	}
	return c
}

// minElectionTicks is ElectionTimeoutMin expressed in heartbeat ticks
// (floored at 1).
func (n *Node) minElectionTicks() int {
	t := int(n.cfg.ElectionTimeoutMin) / int(n.cfg.HeartbeatInterval)
	if t < 1 {
		return 1
	}
	return t
}
