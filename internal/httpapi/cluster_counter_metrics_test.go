package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mtingers/dflockd/internal/raft"
)

// TestMetrics_EmitsClusterCounters verifies the /metrics handler writes
// the Raft counters and apply-latency histogram in cluster mode.
func TestMetrics_EmitsClusterCounters(t *testing.T) {
	fc := &counterFakeCluster{httpFakeCluster: httpFakeCluster{leader: true}}
	raftCounters := raft.CountersSnapshot{
		Proposals:       42,
		ProposalsFailed: 3,
		Applies:         40,
		AppliesFailed:   0,
		ApplyNanosTotal: 1_000_000,
		LeaderChanges:   1,
	}
	raftCounters.ApplyDurationBuckets[0] = 10
	raftCounters.ApplyDurationBuckets[1] = 20
	raftCounters.ApplyDurationBuckets[len(raftCounters.ApplyDurationBuckets)-1] = 10
	fc.counters = raft.ClusterMetrics{
		Raft:              raftCounters,
		AdminAddVoter:     2,
		AdminAddVoterFail: 1,
		AdminRemoveServer: 1,
		AdminRemoveFail:   0,
	}
	hs := newClusterHTTPTest(t, fc)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	hs.handleMetrics(rec, req)

	body := rec.Body.String()
	mustContain := []string{
		"dflockd_raft_proposals_total 42",
		"dflockd_raft_proposals_failed_total 3",
		"dflockd_raft_apply_total 40",
		"dflockd_raft_apply_failed_total 0",
		`dflockd_raft_apply_duration_seconds_bucket{le="0.00005"} 10`,
		`dflockd_raft_apply_duration_seconds_bucket{le="0.0001"} 30`,
		`dflockd_raft_apply_duration_seconds_bucket{le="+Inf"} 40`,
		"dflockd_raft_apply_duration_seconds_sum 0.001",
		"dflockd_raft_apply_duration_seconds_count 40",
		"dflockd_raft_leader_changes_total 1",
		`dflockd_raft_admin_changes_total{op="add_voter"} 2`,
		`dflockd_raft_admin_changes_total{op="add_voter_failed"} 1`,
		`dflockd_raft_admin_changes_total{op="remove_server"} 1`,
		`dflockd_raft_admin_changes_total{op="remove_server_failed"} 0`,
	}
	for _, want := range mustContain {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in /metrics output", want)
		}
	}
	if t.Failed() {
		t.Logf("full /metrics body:\n%s", body)
	}
}

// counterFakeCluster lets the test inject a non-zero MetricsSnapshot.
type counterFakeCluster struct {
	httpFakeCluster
	counters raft.ClusterMetrics
}

func (c *counterFakeCluster) MetricsSnapshot() raft.ClusterMetrics {
	return c.counters
}
