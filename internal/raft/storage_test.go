package raft

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// --- shared helpers ---

func mkEntry(i Index, t Term, data string) Entry {
	return Entry{Index: i, Term: t, Type: EntryNormal, Data: []byte(data)}
}

func appendN(t *testing.T, s Storage, from Index, term Term, n int) {
	t.Helper()
	es := make([]Entry, n)
	for k := 0; k < n; k++ {
		es[k] = mkEntry(from+Index(k), term, "d")
	}
	if err := s.Append(es); err != nil {
		t.Fatalf("Append(%d..%d): %v", from, from+Index(n)-1, err)
	}
}

func readSnapshot(t *testing.T, s Storage) []byte {
	t.Helper()
	rc, err := s.OpenSnapshot()
	if err != nil {
		t.Fatalf("OpenSnapshot: %v", err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	return b
}

// --- conformance suite, run against both implementations ---

func TestStorageConformance(t *testing.T) {
	impls := map[string]func(t *testing.T) Storage{
		"mem":  func(t *testing.T) Storage { return NewMemStorage() },
		"file": func(t *testing.T) Storage { return mustOpenFileStorage(t, t.TempDir()) },
	}
	cases := map[string]func(*testing.T, Storage){
		"empty":            storageEmpty,
		"append-read":      storageAppendRead,
		"hardstate":        storageHardState,
		"truncate-suffix":  storageTruncateSuffix,
		"snapshot-compact": storageSnapshotCompact,
		"snapshot-forward": storageSnapshotForward,
		"non-contiguous":   storageNonContiguous,
		"range-errors":     storageRangeErrors,
	}
	for implName, newS := range impls {
		for caseName, fn := range cases {
			t.Run(implName+"/"+caseName, func(t *testing.T) {
				s := newS(t)
				defer s.Close()
				fn(t, s)
			})
		}
	}
}

func storageEmpty(t *testing.T, s Storage) {
	if s.FirstIndex() != 1 || s.LastIndex() != 0 {
		t.Fatalf("empty: first=%d last=%d, want 1/0", s.FirstIndex(), s.LastIndex())
	}
	if tm, err := s.Term(0); err != nil || tm != 0 {
		t.Fatalf("Term(0)=%d,%v want 0,nil", tm, err)
	}
	if _, err := s.Term(1); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Term(1) on empty: want ErrUnavailable, got %v", err)
	}
	if hs, err := s.LoadHardState(); err != nil || hs != (HardState{}) {
		t.Fatalf("LoadHardState empty: %+v %v", hs, err)
	}
	if _, ok := s.SnapshotMeta(); ok {
		t.Fatalf("SnapshotMeta on empty: want ok=false")
	}
	if _, err := s.OpenSnapshot(); !errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("OpenSnapshot empty: want ErrNoSnapshot, got %v", err)
	}
}

func storageAppendRead(t *testing.T, s Storage) {
	if err := s.Append([]Entry{mkEntry(1, 1, "a"), mkEntry(2, 1, "b"), mkEntry(3, 2, "c")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if s.FirstIndex() != 1 || s.LastIndex() != 3 {
		t.Fatalf("first=%d last=%d, want 1/3", s.FirstIndex(), s.LastIndex())
	}
	if tm, _ := s.Term(2); tm != 1 {
		t.Fatalf("Term(2)=%d want 1", tm)
	}
	if tm, _ := s.Term(3); tm != 2 {
		t.Fatalf("Term(3)=%d want 2", tm)
	}
	es, err := s.Entries(1, 4)
	if err != nil || len(es) != 3 || string(es[2].Data) != "c" {
		t.Fatalf("Entries(1,4) = %+v, %v", es, err)
	}
	es, _ = s.Entries(2, 3)
	if len(es) != 1 || es[0].Index != 2 {
		t.Fatalf("Entries(2,3) = %+v", es)
	}
	es, _ = s.Entries(2, 2)
	if len(es) != 0 {
		t.Fatalf("Entries(2,2) = %+v, want empty", es)
	}
}

func storageHardState(t *testing.T, s Storage) {
	for i, hs := range []HardState{
		{CurrentTerm: 1, VotedFor: "n1", CommitIndex: 0},
		{CurrentTerm: 2, VotedFor: "", CommitIndex: 5},
		{CurrentTerm: 7, VotedFor: "node-with-a-longer-id", CommitIndex: 42},
	} {
		if err := s.SaveHardState(hs); err != nil {
			t.Fatalf("SaveHardState #%d: %v", i, err)
		}
		got, err := s.LoadHardState()
		if err != nil || got != hs {
			t.Fatalf("LoadHardState #%d = %+v, %v; want %+v", i, got, err, hs)
		}
	}
}

func storageTruncateSuffix(t *testing.T, s Storage) {
	appendN(t, s, 1, 1, 5) // entries 1..5
	if err := s.TruncateSuffix(3); err != nil {
		t.Fatalf("TruncateSuffix(3): %v", err)
	}
	if s.LastIndex() != 2 {
		t.Fatalf("after TruncateSuffix(3): LastIndex=%d, want 2", s.LastIndex())
	}
	if _, err := s.Term(3); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Term(3) after truncate: want ErrUnavailable, got %v", err)
	}
	// Re-append from 3 with a new term (a follower fixing a divergent tail).
	if err := s.Append([]Entry{mkEntry(3, 9, "x"), mkEntry(4, 9, "y")}); err != nil {
		t.Fatalf("re-append: %v", err)
	}
	if tm, _ := s.Term(3); tm != 9 {
		t.Fatalf("Term(3) after re-append = %d, want 9", tm)
	}
	// Idempotent above the end.
	if err := s.TruncateSuffix(99); err != nil {
		t.Fatalf("TruncateSuffix(99): %v", err)
	}
	if s.LastIndex() != 4 {
		t.Fatalf("LastIndex after no-op truncate = %d, want 4", s.LastIndex())
	}
}

func storageSnapshotCompact(t *testing.T, s Storage) {
	appendN(t, s, 1, 1, 6) // 1..6
	meta := SnapshotMeta{LastIncludedIndex: 4, LastIncludedTerm: 1, Configuration: Configuration{Voters: map[NodeID]string{"a": "h:1", "b": "h:2"}}}
	if err := s.SaveSnapshot(meta, bytes.NewReader([]byte("FSMDATA"))); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if s.FirstIndex() != 5 || s.LastIndex() != 6 {
		t.Fatalf("after compact: first=%d last=%d, want 5/6", s.FirstIndex(), s.LastIndex())
	}
	if tm, err := s.Term(4); err != nil || tm != 1 {
		t.Fatalf("Term(4) (snapshot index) = %d, %v; want 1, nil", tm, err)
	}
	if _, err := s.Term(3); !errors.Is(err, ErrCompacted) {
		t.Fatalf("Term(3) after compact: want ErrCompacted, got %v", err)
	}
	if _, err := s.Entries(3, 6); !errors.Is(err, ErrCompacted) {
		t.Fatalf("Entries(3,6) after compact: want ErrCompacted, got %v", err)
	}
	es, err := s.Entries(5, 7)
	if err != nil || len(es) != 2 || es[0].Index != 5 {
		t.Fatalf("Entries(5,7) after compact = %+v, %v", es, err)
	}
	gm, ok := s.SnapshotMeta()
	if !ok || gm.LastIncludedIndex != 4 || len(gm.Configuration.Voters) != 2 {
		t.Fatalf("SnapshotMeta = %+v, ok=%v", gm, ok)
	}
	if got := readSnapshot(t, s); string(got) != "FSMDATA" {
		t.Fatalf("snapshot data = %q, want FSMDATA", got)
	}
	// Append continues from 7.
	appendN(t, s, 7, 2, 1)
	if s.LastIndex() != 7 {
		t.Fatalf("append after compact: LastIndex=%d, want 7", s.LastIndex())
	}
}

func storageSnapshotForward(t *testing.T, s Storage) {
	appendN(t, s, 1, 1, 3) // 1..3
	// A snapshot from a far-ahead leader: index 10, which we don't have.
	meta := SnapshotMeta{LastIncludedIndex: 10, LastIncludedTerm: 4}
	if err := s.SaveSnapshot(meta, bytes.NewReader([]byte("X"))); err != nil {
		t.Fatalf("SaveSnapshot forward: %v", err)
	}
	if s.FirstIndex() != 11 || s.LastIndex() != 10 {
		t.Fatalf("after forward snapshot: first=%d last=%d, want 11/10", s.FirstIndex(), s.LastIndex())
	}
	if _, err := s.Entries(11, 11); err != nil {
		t.Fatalf("Entries(11,11) empty range: %v", err)
	}
	appendN(t, s, 11, 5, 2) // resume at 11
	if s.LastIndex() != 12 {
		t.Fatalf("append after forward snapshot: LastIndex=%d, want 12", s.LastIndex())
	}
}

func storageNonContiguous(t *testing.T, s Storage) {
	appendN(t, s, 1, 1, 2)
	if err := s.Append([]Entry{mkEntry(4, 1, "gap")}); !errors.Is(err, ErrNonContiguous) {
		t.Fatalf("append with gap: want ErrNonContiguous, got %v", err)
	}
	if err := s.Append([]Entry{mkEntry(3, 1, "a"), mkEntry(5, 1, "b")}); !errors.Is(err, ErrNonContiguous) {
		t.Fatalf("append internally non-contiguous: want ErrNonContiguous, got %v", err)
	}
}

func storageRangeErrors(t *testing.T, s Storage) {
	appendN(t, s, 1, 1, 3)
	if _, err := s.Entries(1, 5); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Entries past end: want ErrUnavailable, got %v", err)
	}
	if _, err := s.Term(99); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Term past end: want ErrUnavailable, got %v", err)
	}
}

// --- FileStorage-specific ---

func mustOpenFileStorage(t *testing.T, dir string) *FileStorage {
	t.Helper()
	s, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("OpenFileStorage(%s): %v", dir, err)
	}
	return s
}

func TestFileStoragePersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	s := mustOpenFileStorage(t, dir)
	appendN(t, s, 1, 1, 4)
	mustSaveHard(t, s, HardState{CurrentTerm: 3, VotedFor: "n2", CommitIndex: 2})
	mustSaveSnap(t, s, SnapshotMeta{LastIncludedIndex: 2, LastIncludedTerm: 1, Configuration: Configuration{Voters: map[NodeID]string{"x": "h:9"}}}, []byte("snap-bytes"))
	appendN(t, s, 5, 2, 2) // 5..6 after the snapshot at 2
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2 := mustOpenFileStorage(t, dir)
	defer s2.Close()
	if s2.FirstIndex() != 3 || s2.LastIndex() != 6 {
		t.Fatalf("reopened: first=%d last=%d, want 3/6", s2.FirstIndex(), s2.LastIndex())
	}
	if hs, _ := s2.LoadHardState(); hs.CurrentTerm != 3 || hs.VotedFor != "n2" || hs.CommitIndex != 2 {
		t.Fatalf("reopened hardstate = %+v", hs)
	}
	if m, ok := s2.SnapshotMeta(); !ok || m.LastIncludedIndex != 2 || m.Configuration.Voters["x"] != "h:9" {
		t.Fatalf("reopened snapshot meta = %+v ok=%v", m, ok)
	}
	if got := readSnapshot(t, s2); string(got) != "snap-bytes" {
		t.Fatalf("reopened snapshot data = %q", got)
	}
	es, _ := s2.Entries(3, 7)
	if len(es) != 4 || es[0].Index != 3 {
		t.Fatalf("reopened entries = %+v", es)
	}
}

func TestFileStoragePreparedSnapshotCommitsConcurrentTail(t *testing.T) {
	dir := t.TempDir()
	s := mustOpenFileStorage(t, dir)
	appendN(t, s, 1, 1, 5)

	meta := SnapshotMeta{LastIncludedIndex: 3, LastIncludedTerm: 1}
	tail, err := s.Entries(4, 6)
	if err != nil {
		t.Fatalf("Entries for prepared tail: %v", err)
	}
	prepared, err := s.prepareSnapshot(meta, []byte("prepared"), tail)
	if err != nil {
		t.Fatalf("prepareSnapshot: %v", err)
	}
	appendN(t, s, 6, 2, 1)
	delta, err := s.Entries(6, 7)
	if err != nil {
		t.Fatalf("Entries for prepared delta: %v", err)
	}
	if err := s.commitPreparedSnapshot(prepared, delta); err != nil {
		t.Fatalf("commitPreparedSnapshot: %v", err)
	}
	mustSaveHard(t, s, HardState{CurrentTerm: 2, CommitIndex: 6})
	if s.FirstIndex() != 4 || s.LastIndex() != 6 {
		t.Fatalf("committed: first=%d last=%d, want 4/6", s.FirstIndex(), s.LastIndex())
	}
	if got := string(readSnapshot(t, s)); got != "prepared" {
		t.Fatalf("committed snapshot data = %q, want prepared", got)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2 := mustOpenFileStorage(t, dir)
	defer s2.Close()
	if s2.FirstIndex() != 4 || s2.LastIndex() != 6 {
		t.Fatalf("reopened: first=%d last=%d, want 4/6", s2.FirstIndex(), s2.LastIndex())
	}
	entries, err := s2.Entries(4, 7)
	if err != nil || len(entries) != 3 || entries[2].Index != 6 || entries[2].Term != 2 {
		t.Fatalf("reopened entries = %+v, %v; want indexes 4..6 with term(6)=2", entries, err)
	}
}

func TestFileStoragePreparedSnapshotIsCrashRecoverableBeforeCommit(t *testing.T) {
	dir := t.TempDir()
	s := mustOpenFileStorage(t, dir)
	appendN(t, s, 1, 1, 5)

	meta := SnapshotMeta{LastIncludedIndex: 3, LastIncludedTerm: 1}
	tail, err := s.Entries(4, 6)
	if err != nil {
		t.Fatalf("Entries for prepared tail: %v", err)
	}
	prepared, err := s.prepareSnapshot(meta, []byte("prepared"), tail)
	if err != nil {
		t.Fatalf("prepareSnapshot: %v", err)
	}
	mustSaveHard(t, s, HardState{CurrentTerm: 1, CommitIndex: 5})
	s.abortPreparedSnapshot(prepared)
	if _, ok := s.SnapshotMeta(); ok {
		t.Fatal("prepared snapshot became live before commit")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2 := mustOpenFileStorage(t, dir)
	defer s2.Close()
	if got, ok := s2.SnapshotMeta(); !ok || got.LastIncludedIndex != 3 {
		t.Fatalf("recovered snapshot = %+v, %v; want index 3", got, ok)
	}
	if got := string(readSnapshot(t, s2)); got != "prepared" {
		t.Fatalf("recovered snapshot data = %q, want prepared", got)
	}
	entries, err := s2.Entries(4, 6)
	if err != nil || len(entries) != 2 {
		t.Fatalf("recovered entries = %+v, %v; want indexes 4..5", entries, err)
	}
}

func TestFileStoragePreparedSnapshotDoesNotReplaceLiveReader(t *testing.T) {
	s := mustOpenFileStorage(t, t.TempDir())
	defer s.Close()
	appendN(t, s, 1, 1, 5)
	mustSaveSnap(t, s, SnapshotMeta{LastIncludedIndex: 2, LastIncludedTerm: 1}, []byte("live"))

	tail, err := s.Entries(5, 6)
	if err != nil {
		t.Fatalf("Entries for prepared tail: %v", err)
	}
	prepared, err := s.prepareSnapshot(
		SnapshotMeta{LastIncludedIndex: 4, LastIncludedTerm: 1},
		[]byte("prepared"),
		tail,
	)
	if err != nil {
		t.Fatalf("prepareSnapshot: %v", err)
	}
	defer s.abortPreparedSnapshot(prepared)

	if got, ok := s.SnapshotMeta(); !ok || got.LastIncludedIndex != 2 {
		t.Fatalf("live snapshot = %+v, %v; want index 2", got, ok)
	}
	if got := string(readSnapshot(t, s)); got != "live" {
		t.Fatalf("live snapshot data = %q, want live", got)
	}
}

func mustSaveHard(t *testing.T, s Storage, hs HardState) {
	t.Helper()
	if err := s.SaveHardState(hs); err != nil {
		t.Fatalf("SaveHardState: %v", err)
	}
}

func mustSaveSnap(t *testing.T, s Storage, meta SnapshotMeta, data []byte) {
	t.Helper()
	if err := s.SaveSnapshot(meta, bytes.NewReader(data)); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
}

func TestFileStorageDirLockRefusesSecondOpen(t *testing.T) {
	dir := t.TempDir()
	s := mustOpenFileStorage(t, dir)
	defer s.Close()
	if _, err := OpenFileStorage(dir); err == nil {
		t.Fatalf("second OpenFileStorage should fail while first holds the lock")
	}
	// After closing the first, a second open should succeed.
	s.Close()
	s2, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("reopen after close: %v", err)
	}
	s2.Close()
}

func TestFileStorageTornTailDiscarded(t *testing.T) {
	dir := t.TempDir()
	s := mustOpenFileStorage(t, dir)
	appendN(t, s, 1, 1, 3)
	mustSaveHard(t, s, HardState{CurrentTerm: 1, CommitIndex: 3})
	s.Close()

	// Corrupt the WAL: append garbage bytes after the last good record.
	walPath := filepath.Join(dir, walFileName)
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open wal for corruption: %v", err)
	}
	// A plausible-looking but CRC-invalid record header + body.
	junk := append(make([]byte, 4), make([]byte, 8)...) // len=0 in u32 -> rejected as payLen<=0
	be.PutUint32(junk[0:4], 50)                         // claims a 50-byte payload that isn't there
	if _, err := f.Write(junk); err != nil {
		t.Fatalf("write junk: %v", err)
	}
	f.Close()

	s2 := mustOpenFileStorage(t, dir)
	defer s2.Close()
	if s2.LastIndex() != 3 {
		t.Fatalf("after torn tail: LastIndex=%d, want 3 (junk discarded)", s2.LastIndex())
	}
	// And the file should have been truncated, so a new append works.
	appendN(t, s2, 4, 1, 1)
	if s2.LastIndex() != 4 {
		t.Fatalf("append after torn-tail recovery: LastIndex=%d, want 4", s2.LastIndex())
	}
}

func TestFileStorageFailedTruncateKeepsInMemoryLog(t *testing.T) {
	dir := t.TempDir()
	s := mustOpenFileStorage(t, dir)
	defer s.Close()
	appendN(t, s, 1, 1, 4)

	walPath := s.wal.path
	s.wal.path = filepath.Join(dir, "missing", walFileName)
	err := s.TruncateSuffix(3)
	s.wal.path = walPath
	if err == nil {
		t.Fatal("TruncateSuffix succeeded with an unusable WAL path")
	}

	if got := s.LastIndex(); got != 4 {
		t.Fatalf("LastIndex after failed truncate = %d, want 4", got)
	}
	entries, readErr := s.Entries(1, 5)
	if readErr != nil || len(entries) != 4 {
		t.Fatalf("Entries after failed truncate = %+v, %v; want four intact entries", entries, readErr)
	}
}

func TestFileStorageRejectsCorruptHardState(t *testing.T) {
	dir := t.TempDir()
	s := mustOpenFileStorage(t, dir)
	mustSaveHard(t, s, HardState{CurrentTerm: 5, VotedFor: "n1", CommitIndex: 3})
	s.Close()

	// The first save writes both slots. Corrupt both copies so no valid
	// durable state remains and startup must fail closed.
	hsPath := filepath.Join(dir, hardStateFileName)
	raw, _ := os.ReadFile(hsPath)
	raw[20] ^= 0xFF
	raw[hardStateSlotBytes+20] ^= 0xFF
	os.WriteFile(hsPath, raw, 0o600)

	if s2, err := OpenFileStorage(dir); err == nil {
		s2.Close()
		t.Fatal("OpenFileStorage accepted HardState with no valid slot")
	}
}

func TestFileStorageRejectsExistingAllZeroHardState(t *testing.T) {
	dir := t.TempDir()
	s := mustOpenFileStorage(t, dir)
	s.Close()

	hsPath := filepath.Join(dir, hardStateFileName)
	if err := os.WriteFile(hsPath, make([]byte, hardStateFileBytes), hardStateFilePerm); err != nil {
		t.Fatal(err)
	}
	if s2, err := OpenFileStorage(dir); err == nil {
		s2.Close()
		t.Fatal("OpenFileStorage accepted an existing all-zero HardState journal")
	}
}

func TestFileStorageRejectsConflictingSameGenerationSnapshot(t *testing.T) {
	dir := t.TempDir()
	s := mustOpenFileStorage(t, dir)
	defer s.Close()

	meta := SnapshotMeta{
		LastIncludedIndex: 2,
		LastIncludedTerm:  1,
		Configuration:     Configuration{Voters: map[NodeID]string{"n1": "r1"}, ClientAddrs: map[NodeID]string{"n1": "c1"}},
	}
	if err := s.SaveSnapshot(meta, bytes.NewReader([]byte("first"))); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	conflict := meta
	conflict.Configuration = Configuration{Voters: map[NodeID]string{"n1": "r1"}, ClientAddrs: map[NodeID]string{"n1": "changed"}}
	if _, err := s.prepareSnapshot(conflict, []byte("second"), nil); err == nil {
		t.Fatal("prepareSnapshot accepted conflicting data and metadata at the same index/term")
	}
}

func TestFileStorageRecoversPreviousHardStateSlot(t *testing.T) {
	dir := t.TempDir()
	s := mustOpenFileStorage(t, dir)
	first := HardState{CurrentTerm: 3, VotedFor: "n1"}
	mustSaveHard(t, s, first)
	mustSaveHard(t, s, HardState{CurrentTerm: 4, VotedFor: "n2"})
	s.Close()

	hsPath := filepath.Join(dir, hardStateFileName)
	raw, err := os.ReadFile(hsPath)
	if err != nil {
		t.Fatal(err)
	}
	raw[hardStateSlotBytes+20] ^= 0xFF // seq=3 is in slot 1; slot 0 still contains seq=2
	if err := os.WriteFile(hsPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	s2 := mustOpenFileStorage(t, dir)
	defer s2.Close()
	if got, _ := s2.LoadHardState(); got != first {
		t.Fatalf("recovered HardState = %+v, want %+v", got, first)
	}
}

func TestFileStorageRejectsBothCorruptHardStateSlots(t *testing.T) {
	dir := t.TempDir()
	s := mustOpenFileStorage(t, dir)
	mustSaveHard(t, s, HardState{CurrentTerm: 3, VotedFor: "n1"})
	mustSaveHard(t, s, HardState{CurrentTerm: 4, VotedFor: "n2"})
	s.Close()

	hsPath := filepath.Join(dir, hardStateFileName)
	raw, err := os.ReadFile(hsPath)
	if err != nil {
		t.Fatal(err)
	}
	raw[20] ^= 0xFF
	raw[hardStateSlotBytes+20] ^= 0xFF
	if err := os.WriteFile(hsPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if s2, err := OpenFileStorage(dir); err == nil {
		s2.Close()
		t.Fatal("OpenFileStorage accepted two corrupt HardState slots")
	}
}

func TestFileStorageRejectsCorruptCommittedWALWithoutTruncating(t *testing.T) {
	dir := t.TempDir()
	s := mustOpenFileStorage(t, dir)
	appendN(t, s, 1, 1, 3)
	mustSaveHard(t, s, HardState{CurrentTerm: 1, CommitIndex: 3})
	s.Close()

	corrupt := corruptWALRecord(t, dir, 3)
	if s2, err := OpenFileStorage(dir); err == nil {
		s2.Close()
		t.Fatal("OpenFileStorage accepted corruption at committed index 3")
	}
	after, err := os.ReadFile(filepath.Join(dir, walFileName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, corrupt) {
		t.Fatal("failed recovery modified the WAL before validation")
	}
}

func TestFileStorageTruncatesOnlyCorruptUncommittedWAL(t *testing.T) {
	dir := t.TempDir()
	s := mustOpenFileStorage(t, dir)
	appendN(t, s, 1, 1, 3)
	mustSaveHard(t, s, HardState{CurrentTerm: 1, CommitIndex: 2})
	s.Close()

	corruptWALRecord(t, dir, 3)
	s2 := mustOpenFileStorage(t, dir)
	defer s2.Close()
	if got := s2.LastIndex(); got != 2 {
		t.Fatalf("LastIndex after uncommitted-tail recovery = %d, want 2", got)
	}
	appendN(t, s2, 3, 1, 1)
}

func TestNewRaftLogRejectsCommitBeyondAvailableState(t *testing.T) {
	storage := NewMemStorage()
	if err := storage.SaveHardState(HardState{CurrentTerm: 1, CommitIndex: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := newRaftLog(storage); err == nil {
		t.Fatal("newRaftLog silently accepted commit index beyond the log")
	}
}

func corruptWALRecord(t *testing.T, dir string, index Index) []byte {
	t.Helper()
	path := filepath.Join(dir, walFileName)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	off := 0
	for off < len(raw) {
		entry, n, ok := decodeWALRecord(raw[off:])
		if !ok {
			t.Fatalf("WAL was already invalid at byte %d", off)
		}
		if entry.Index == index {
			raw[off+walRecordHeaderBytes] ^= 0xFF
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			return append([]byte(nil), raw...)
		}
		off += n
	}
	t.Fatalf("WAL record %d not found", index)
	return nil
}

// A snapshot file that exists but is corrupt must fail the open loudly —
// silently treating it as "no snapshot" would, after a log compaction,
// reset the node to empty state at term 0.
func TestFileStorageRejectsCorruptSnapshot(t *testing.T) {
	dir := t.TempDir()
	s := mustOpenFileStorage(t, dir)
	appendN(t, s, 1, 1, 5)
	mustSaveSnap(t, s, SnapshotMeta{LastIncludedIndex: 3, LastIncludedTerm: 1}, []byte("state"))
	s.Close()

	names, err := snapshotStore{dir: filepath.Join(dir, snapshotsSubdir)}.listNames()
	if err != nil || len(names) != 1 {
		t.Fatalf("listNames = %v, %v", names, err)
	}
	snapPath := filepath.Join(dir, snapshotsSubdir, names[0])
	raw, _ := os.ReadFile(snapPath)
	raw[len(raw)/2] ^= 0xFF // corrupt the middle; the trailing CRC will mismatch
	if err := os.WriteFile(snapPath, raw, 0o600); err != nil {
		t.Fatalf("rewrite corrupt snapshot: %v", err)
	}

	if s2, err := OpenFileStorage(dir); err == nil {
		s2.Close()
		t.Fatalf("OpenFileStorage with a corrupt snapshot should fail, got nil")
	}
}

func TestSnapshotNameRoundTrip(t *testing.T) {
	meta := SnapshotMeta{LastIncludedIndex: 12345, LastIncludedTerm: 67}
	name := snapshotName(meta)
	i, tm, ok := parseSnapshotName(name)
	if !ok || i != 12345 || tm != 67 {
		t.Fatalf("parseSnapshotName(%q) = %d,%d,%v", name, i, tm, ok)
	}
	if _, _, ok := parseSnapshotName("not-a-snapshot"); ok {
		t.Fatalf("parseSnapshotName on a non-snapshot name should fail")
	}
}

func TestSnapshotKeepsOnlyLatest(t *testing.T) {
	dir := t.TempDir()
	s := mustOpenFileStorage(t, dir)
	defer s.Close()
	appendN(t, s, 1, 1, 10)
	mustSaveSnap(t, s, SnapshotMeta{LastIncludedIndex: 3, LastIncludedTerm: 1}, []byte("v1"))
	mustSaveSnap(t, s, SnapshotMeta{LastIncludedIndex: 7, LastIncludedTerm: 1}, []byte("v2"))
	names, err := s.snaps.listNames()
	if err != nil {
		t.Fatalf("listNames: %v", err)
	}
	if len(names) != 1 {
		t.Fatalf("snapshot files = %v, want exactly 1", names)
	}
	if m, _ := s.SnapshotMeta(); m.LastIncludedIndex != 7 {
		t.Fatalf("latest snapshot index = %d, want 7", m.LastIncludedIndex)
	}
}

func TestSnapshotPersistsClientMemberMetadata(t *testing.T) {
	dir := t.TempDir()
	s := mustOpenFileStorage(t, dir)
	meta := SnapshotMeta{
		LastIncludedIndex: 1,
		LastIncludedTerm:  1,
		Configuration: Configuration{
			Voters:      map[NodeID]string{"a": "raft-a"},
			ClientAddrs: map[NodeID]string{"a": "client-a"},
		},
	}
	mustSaveSnap(t, s, meta, []byte("state"))
	mustSaveHard(t, s, HardState{CurrentTerm: 1, CommitIndex: 1})
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened := mustOpenFileStorage(t, dir)
	defer reopened.Close()
	got, ok := reopened.SnapshotMeta()
	if !ok || got.Configuration.ClientAddrs["a"] != "client-a" {
		t.Fatalf("reopened metadata = %+v, ok=%v", got.Configuration.ClientAddrs, ok)
	}
}

func TestSnapshotRejectsIncompleteClientMemberMetadata(t *testing.T) {
	meta := SnapshotMeta{
		LastIncludedIndex: 1,
		LastIncludedTerm:  1,
		Configuration: Configuration{
			Voters:      map[NodeID]string{"a": "raft-a", "b": "raft-b"},
			ClientAddrs: map[NodeID]string{"a": "client-a"},
		},
	}
	impls := map[string]func(*testing.T) Storage{
		"mem":  func(*testing.T) Storage { return NewMemStorage() },
		"file": func(t *testing.T) Storage { return mustOpenFileStorage(t, t.TempDir()) },
	}
	for name, newStorage := range impls {
		t.Run(name, func(t *testing.T) {
			s := newStorage(t)
			defer s.Close()
			if err := s.SaveSnapshot(meta, bytes.NewReader(nil)); err == nil {
				t.Fatal("SaveSnapshot accepted incomplete client metadata")
			}
		})
	}
}
