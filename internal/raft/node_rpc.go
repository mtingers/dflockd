package raft

// onRPC dispatches an inbound RPC. The universal Raft rule — "if the
// message's term exceeds ours, step down to follower at that term" — is
// applied first (except a PreVote, which is hypothetical and never moves
// terms), then the message-specific handler builds the reply.
func (n *Node) onRPC(req rpcRequest) {
	n.maybeStepDownForInbound(req.msg)
	req.reply <- n.dispatchRPC(req.from, req.msg)
}

func (n *Node) maybeStepDownForInbound(msg Message) {
	if isPreVoteReq(msg) {
		return
	}
	if t := msg.messageTerm(); t > n.term {
		n.becomeFollower(t, "")
	}
}

func isPreVoteReq(msg Message) bool {
	rv, ok := msg.(*RequestVoteReq)
	return ok && rv.PreVote
}

func (n *Node) dispatchRPC(from NodeID, msg Message) Message {
	switch m := msg.(type) {
	case *RequestVoteReq:
		return n.handleRequestVote(m)
	case *AppendEntriesReq:
		return n.handleAppendEntries(from, m)
	case *InstallSnapshotReq:
		return n.handleInstallSnapshot(from, m)
	case *TimeoutNowReq:
		return n.handleTimeoutNow(m)
	default:
		return staleReply(msg, n.term)
	}
}

// onRPCReply dispatches the reply to an RPC this node sent. A reply
// with a higher term steps us down; a reply that belongs to a round we
// are no longer running is discarded; otherwise the reply-specific
// handler runs.
func (n *Node) onRPCReply(rep rpcReply) {
	n.finishSnapshotSend(rep)
	if rep.err != nil || rep.msg == nil {
		return // treated as "no reply"; timers/heartbeats will retry
	}
	if n.stepDownForReply(rep.msg) {
		return
	}
	if !n.replyIsCurrentRound(rep.msg) {
		return
	}
	n.dispatchRPCReply(rep)
}

// finishSnapshotSend releases a current-term snapshot gate on every terminal
// outcome, including transport errors and timeouts where there is no response
// message to identify the RPC. A reply from an older leadership term must not
// release a snapshot started by the current leader.
func (n *Node) finishSnapshotSend(rep rpcReply) {
	req, ok := rep.req.(*InstallSnapshotReq)
	if !ok || req.Term != n.term {
		return
	}
	if p := n.progress[rep.from]; p != nil {
		p.snapshotInFlight = false
	}
}

// stepDownForReply applies the step-down-on-higher-term rule to a reply
// and reports whether it fired. A *granted* pre-vote response is
// exempt: we deliberately sent that request one term past our own, so
// the echoed term is expected — it only becomes real if the pre-vote
// round wins and campaign() bumps us there.
func (n *Node) stepDownForReply(msg Message) bool {
	if isGrantedPreVoteResp(msg) {
		return false
	}
	if t := msg.messageTerm(); t > n.term {
		n.becomeFollower(t, "")
		return true
	}
	return false
}

// replyIsCurrentRound reports whether a reply belongs to the round this
// node is currently running. Replies outlive their round routinely —
// sendRPC's timeout is ElectionTimeoutMax while a new election starts
// after as little as ElectionTimeoutMin — and acting on a stale one is
// unsafe: a vote granted in term N says nothing about term N+1 (the
// grantor may have voted for someone else there), and an AppendEntries
// success from a term we no longer lead describes a follower log that
// may since have been overwritten.
func (n *Node) replyIsCurrentRound(msg Message) bool {
	if rv, ok := msg.(*RequestVoteResp); ok {
		return rv.Term == n.roundTerm(rv.PreVote)
	}
	return msg.messageTerm() == n.term
}

// roundTerm is the term a (pre)vote reply for the round now in progress
// must carry: our own term for a real election, one past it for a
// pre-vote (which is a poll about a term we have not entered).
func (n *Node) roundTerm(preVote bool) Term {
	if preVote {
		return n.term + 1
	}
	return n.term
}

// isGrantedPreVoteResp reports whether msg is a pre-vote response that
// granted its vote.
func isGrantedPreVoteResp(msg Message) bool {
	rv, ok := msg.(*RequestVoteResp)
	return ok && rv.PreVote && rv.VoteGranted
}

func (n *Node) dispatchRPCReply(rep rpcReply) {
	switch m := rep.msg.(type) {
	case *RequestVoteResp:
		n.handleVoteResp(rep.from, m)
	case *AppendEntriesResp:
		n.handleAppendEntriesResp(rep.from, m)
	case *InstallSnapshotResp:
		n.handleInstallSnapshotResp(rep.from, m)
	case *TimeoutNowResp:
		// nothing to do: the target either started an election (we'll see
		// it) or didn't (we'll heartbeat as usual).
	}
}

// staleReply builds a "rejected, here's my term" reply matching req's type.
func staleReply(req Message, term Term) Message {
	switch req.(type) {
	case *RequestVoteReq:
		rv := req.(*RequestVoteReq)
		return &RequestVoteResp{Term: term, VoteGranted: false, PreVote: rv.PreVote}
	case *AppendEntriesReq:
		return &AppendEntriesResp{Term: term, Success: false}
	case *InstallSnapshotReq:
		return &InstallSnapshotResp{Term: term}
	case *TimeoutNowReq:
		return &TimeoutNowResp{Term: term}
	default:
		return &AppendEntriesResp{Term: term, Success: false}
	}
}
