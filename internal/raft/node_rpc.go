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

// onRPCReply dispatches the reply to an RPC this node sent. A reply with
// a higher term (other than a PreVote reply at our own term) steps us
// down; otherwise the reply-specific handler runs.
func (n *Node) onRPCReply(rep rpcReply) {
	if rep.err != nil || rep.msg == nil {
		return // treated as "no reply"; timers/heartbeats will retry
	}
	if t := rep.msg.messageTerm(); t > n.term {
		n.becomeFollower(t, "")
		return
	}
	n.dispatchRPCReply(rep)
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
