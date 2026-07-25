package main

import (
	"context"
	"fmt"
	"time"
)

const faultCleanupTimeout = 30 * time.Second

func (h *externalHarness) faultLoop(ctx context.Context) {
	defer h.cleanupFaults()
	timer := time.NewTimer(h.opts.FaultInterval)
	defer timer.Stop()
	for step := 0; ; step++ {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		if err := h.runFaultStep(ctx, step%4); err != nil {
			h.addViolation(err.Error())
			return
		}
		timer.Reset(h.opts.FaultInterval)
	}
}

func (h *externalHarness) runFaultStep(ctx context.Context, step int) error {
	switch step {
	case 0:
		return h.partitionLeader(ctx)
	case 1:
		return h.skewFollower(ctx)
	case 2:
		return h.restartLeader(ctx)
	default:
		return h.resetSkew(ctx)
	}
}

func (h *externalHarness) partitionLeader(ctx context.Context) error {
	leader, err := h.probe(ctx, h.members, h.auth)
	if err != nil {
		return err
	}
	// Mark before invoking the hook: a remote mutation can succeed even
	// when the SSH connection fails before reporting completion.
	h.partitioned = leader
	if err := h.hook.Run(ctx, "partition", leader, ""); err != nil {
		return err
	}
	h.partitions.Add(1)
	h.log.Info("soak: partitioned Raft leader", "node", leader)
	if !waitForContext(ctx, h.opts.FaultHold) {
		return nil
	}
	if err := h.hook.Run(ctx, "heal", leader, ""); err != nil {
		return err
	}
	h.partitioned = ""
	h.log.Info("soak: healed Raft partition", "node", leader)
	return nil
}

func (h *externalHarness) skewFollower(ctx context.Context) error {
	if h.opts.ClockSkew == 0 {
		return nil
	}
	leader, err := h.probe(ctx, h.members, h.auth)
	if err != nil {
		return err
	}
	target := h.memberAfter(leader)
	offset := h.opts.ClockSkew
	if h.skews.Load()%2 == 1 {
		offset = -offset
	}
	// As with partitions, make cleanup conservative on ambiguous hook
	// failures after the remote side may already have changed state.
	h.skewed = target
	if err := h.hook.Run(ctx, "skew", target, offset.String()); err != nil {
		return err
	}
	h.skews.Add(1)
	h.log.Info("soak: applied process clock offset", "node", target, "offset", offset)
	return nil
}

func (h *externalHarness) restartLeader(ctx context.Context) error {
	leader, err := h.probe(ctx, h.members, h.auth)
	if err != nil {
		return err
	}
	if err := h.hook.Run(ctx, "restart", leader, ""); err != nil {
		return err
	}
	h.restarts.Add(1)
	h.log.Info("soak: restarted Raft leader", "node", leader)
	return nil
}

func (h *externalHarness) resetSkew(ctx context.Context) error {
	if h.skewed == "" {
		return nil
	}
	target := h.skewed
	if err := h.hook.Run(ctx, "unskew", target, ""); err != nil {
		return err
	}
	h.skewed = ""
	h.log.Info("soak: reset process clock offset", "node", target)
	return nil
}

func (h *externalHarness) memberAfter(id string) string {
	for index, member := range h.members {
		if member.ID == id {
			return h.members[(index+1)%len(h.members)].ID
		}
	}
	return h.members[0].ID
}

func (h *externalHarness) cleanupFaults() {
	if h.hook == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), faultCleanupTimeout)
	defer cancel()
	if h.partitioned != "" {
		node := h.partitioned
		if err := h.hook.Run(ctx, "heal", node, ""); err != nil {
			h.ledger.addViolation(fmt.Sprintf("fault cleanup heal %s: %v", node, err))
		} else {
			h.partitioned = ""
		}
	}
	if h.skewed != "" {
		node := h.skewed
		if err := h.hook.Run(ctx, "unskew", node, ""); err != nil {
			h.ledger.addViolation(fmt.Sprintf("fault cleanup unskew %s: %v", node, err))
		} else {
			h.skewed = ""
		}
	}
}

func waitForContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
