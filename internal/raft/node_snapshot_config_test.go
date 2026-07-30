package raft

import "testing"

// installSnapshot keeps a tail whose term matches at the snapshot index, so a
// follower's log can still hold an EntryConfig ABOVE that index. Per §4.3 a
// configuration takes effect on append, so that entry — not the snapshot's
// metadata — is the effective configuration. Adopting the snapshot's config
// unconditionally would push cfgIndex below a config entry still in the log,
// and configurationAt's fast path would then serve the stale voter set for
// every later index.
func TestInstallSnapshotKeepsConfigurationFromRetainedTail(t *testing.T) {
	net := NewMemNetwork()
	storage := NewMemStorage()
	initial := Configuration{Voters: map[NodeID]string{"a": "a", "b": "b"}}
	withC := Configuration{Voters: map[NodeID]string{"a": "a", "b": "b", "c": "c"}}

	mustAppend(t, storage, []Entry{
		{Index: 1, Term: 1, Type: EntryNoOp},
		{Index: 2, Term: 1, Type: EntryNoOp},
		{Index: 3, Term: 1, Type: EntryConfig, Data: encodeConfig(nil, withC)},
	})

	n := mustNewNode(t, fastConfigID("b"), storage, net.Transport("b"), initial)
	defer n.Close()
	if !n.config.Has("c") || n.cfgIndex != 3 {
		t.Fatalf("setup: voters=%v cfgIndex=%d", n.config.Voters, n.cfgIndex)
	}

	resp := n.handleInstallSnapshot("a", &InstallSnapshotReq{
		Term:     2,
		LeaderID: "a",
		Meta: SnapshotMeta{
			LastIncludedIndex: 1,
			LastIncludedTerm:  1, // matches entry 1, so entries 2-3 are retained
			Configuration:     initial,
		},
		Data: []byte("snap"),
	})
	if resp == nil {
		t.Fatal("nil InstallSnapshot response")
	}
	if n.log.lastIndex() != 3 {
		t.Fatalf("retained tail lost: lastIndex=%d, want 3", n.log.lastIndex())
	}
	if !n.config.Has("c") {
		t.Fatalf("effective configuration regressed to the snapshot's: voters=%v cfgIndex=%d "+
			"(EntryConfig at index 3 is still in the log)", n.config.Voters, n.cfgIndex)
	}
	if n.cfgIndex != 3 {
		t.Fatalf("cfgIndex = %d, want 3 (the surviving config entry)", n.cfgIndex)
	}
}

// When the snapshot supersedes the whole log there is no surviving config
// entry, so the snapshot's own configuration must be adopted.
func TestInstallSnapshotAdoptsSnapshotConfigWhenLogIsSuperseded(t *testing.T) {
	net := NewMemNetwork()
	storage := NewMemStorage()
	initial := Configuration{Voters: map[NodeID]string{"a": "a", "b": "b"}}
	snapCfg := Configuration{Voters: map[NodeID]string{"a": "a", "b": "b", "d": "d"}}

	mustAppend(t, storage, []Entry{{Index: 1, Term: 1, Type: EntryNoOp}})
	n := mustNewNode(t, fastConfigID("b"), storage, net.Transport("b"), initial)
	defer n.Close()

	resp := n.handleInstallSnapshot("a", &InstallSnapshotReq{
		Term:     5,
		LeaderID: "a",
		Meta: SnapshotMeta{
			LastIncludedIndex: 9, // beyond our log: nothing is retained
			LastIncludedTerm:  4,
			Configuration:     snapCfg,
		},
		Data: []byte("snap"),
	})
	if resp == nil {
		t.Fatal("nil InstallSnapshot response")
	}
	if !n.config.Has("d") {
		t.Fatalf("snapshot configuration not adopted: voters=%v", n.config.Voters)
	}
	if n.cfgIndex != 9 {
		t.Fatalf("cfgIndex = %d, want 9 (the snapshot index)", n.cfgIndex)
	}
}
