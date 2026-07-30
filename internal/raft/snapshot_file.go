package raft

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// A snapshot file is written atomically (temp + rename + dir fsync) and
// named snap-<20-digit index>-<20-digit term>. Its bytes are:
//
//	magic:8  lastIncludedIndex:u64  lastIncludedTerm:u64
//	configLen:u32  config  fsmLen:u64  fsm  crc:u64(over all preceding)
//
// Only the newest valid snapshot is kept; older ones are deleted after a
// successful write. dflockd's FSM is small, so a snapshot is read/written
// whole in memory.

const (
	snapshotFilePerm      = 0o600
	snapshotNamePrefix    = "snap-"
	snapshotHeaderMin     = 8 + 8 + 8 + 4             // magic + idx + term + configLen
	snapshotEnvelopeBytes = snapshotHeaderMin + 8 + 8 // fsmLen + CRC
	// A file carries configuration metadata in addition to the transferable
	// FSM payload, so its cap is derived separately from the wire frame.
	maxSnapshotFileBytes = snapshotEnvelopeBytes + maxConfigBytes + maxSnapshotDataBytes
)

var snapshotMagic = [8]byte{'d', 'f', 'l', 'r', 's', 'n', 'p', '1'}

// snapshotName formats the canonical filename for a snapshot.
func snapshotName(meta SnapshotMeta) string {
	return fmt.Sprintf("%s%020d-%020d", snapshotNamePrefix, meta.LastIncludedIndex, meta.LastIncludedTerm)
}

// encodeSnapshotFile builds the on-disk bytes for meta + fsm.
func encodeSnapshotFile(meta SnapshotMeta, fsm []byte) []byte {
	body := make([]byte, 0, snapshotHeaderMin+64+len(fsm)+16)
	body = append(body, snapshotMagic[:]...)
	body = be.AppendUint64(body, uint64(meta.LastIncludedIndex))
	body = be.AppendUint64(body, uint64(meta.LastIncludedTerm))
	cfg := encodeConfig(nil, meta.Configuration)
	body = be.AppendUint32(body, uint32(len(cfg)))
	body = append(body, cfg...)
	body = be.AppendUint64(body, uint64(len(fsm)))
	body = append(body, fsm...)
	return be.AppendUint64(body, crc(body))
}

// decodeSnapshotFile parses raw, validating magic and CRC.
func decodeSnapshotFile(raw []byte) (SnapshotMeta, []byte, error) {
	if err := checkSnapshotEnvelope(raw); err != nil {
		return SnapshotMeta{}, nil, err
	}
	meta := SnapshotMeta{LastIncludedIndex: Index(be.Uint64(raw[8:16])), LastIncludedTerm: Term(be.Uint64(raw[16:24]))}
	cfg, fsmOff, err := decodeSnapshotConfig(raw)
	if err != nil {
		return SnapshotMeta{}, nil, err
	}
	meta.Configuration = cfg
	return decodeSnapshotFSM(raw, meta, fsmOff)
}

func checkSnapshotEnvelope(raw []byte) error {
	if len(raw) < snapshotHeaderMin+16 || string(raw[0:8]) != string(snapshotMagic[:]) {
		return fmt.Errorf("raft: snapshot file truncated or bad magic")
	}
	if crc(raw[:len(raw)-8]) != be.Uint64(raw[len(raw)-8:]) {
		return fmt.Errorf("raft: snapshot file CRC mismatch")
	}
	return nil
}

func decodeSnapshotConfig(raw []byte) (Configuration, int, error) {
	cfgLen := int(be.Uint32(raw[24:28]))
	if cfgLen < 0 || cfgLen > len(raw) { // also guards 32-bit int overflow in off below
		return Configuration{}, 0, fmt.Errorf("raft: snapshot config length out of range")
	}
	off := 28 + cfgLen
	if off+8 > len(raw)-8 {
		return Configuration{}, 0, fmt.Errorf("raft: snapshot config length out of range")
	}
	cfg, err := decodeConfig(raw[28:off])
	return cfg, off, err
}

func decodeSnapshotFSM(raw []byte, meta SnapshotMeta, off int) (SnapshotMeta, []byte, error) {
	fsmLen := int(be.Uint64(raw[off : off+8]))
	if fsmLen < 0 || fsmLen > len(raw) { // also guards int overflow in end below
		return SnapshotMeta{}, nil, fmt.Errorf("raft: snapshot fsm length out of range")
	}
	start, end := off+8, off+8+fsmLen
	if end > len(raw)-8 {
		return SnapshotMeta{}, nil, fmt.Errorf("raft: snapshot fsm length out of range")
	}
	if fsmLen > maxSnapshotDataBytes {
		return SnapshotMeta{}, nil, fmt.Errorf("raft: snapshot data exceeds %d bytes", maxSnapshotDataBytes)
	}
	return meta, append([]byte(nil), raw[start:end]...), nil
}

// snapshotStore manages the snapshot directory.
type snapshotStore struct{ dir string }

func (s snapshotStore) save(meta SnapshotMeta, fsm []byte) error {
	name, err := s.write(meta, fsm)
	if err != nil {
		return err
	}
	return s.deleteAllExcept(name)
}

// write persists one snapshot without deleting the snapshot currently used by
// the live log. Asynchronous preparation uses this so OpenSnapshot can keep
// reading the old generation until the Raft loop publishes the new one.
func (s snapshotStore) write(meta SnapshotMeta, fsm []byte) (string, error) {
	if len(fsm) > maxSnapshotDataBytes {
		return "", fmt.Errorf("raft: snapshot data exceeds %d bytes", maxSnapshotDataBytes)
	}
	if _, err := encodeRPCConfig(meta.Configuration); err != nil {
		return "", fmt.Errorf("raft: snapshot config: %w", err)
	}
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return "", fmt.Errorf("mkdir %s: %w", s.dir, err)
	}
	name := snapshotName(meta)
	if err := writeFileAtomic(filepath.Join(s.dir, name), encodeSnapshotFile(meta, fsm), snapshotFilePerm); err != nil {
		return "", err
	}
	return name, nil
}

func (s snapshotStore) deleteAllExcept(keep string) error {
	names, err := s.listNames()
	if err != nil {
		return err
	}
	for _, n := range names {
		if n != keep {
			_ = os.Remove(filepath.Join(s.dir, n))
		}
	}
	return nil
}

func (s snapshotStore) listNames() ([]string, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read snapshot dir %s: %w", s.dir, err)
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), snapshotNamePrefix) && !strings.HasSuffix(e.Name(), ".tmp") {
			out = append(out, e.Name())
		}
	}
	return out, nil
}

// loadLatest returns the highest-index valid snapshot's meta + fsm bytes.
// ok is false if there is no snapshot at all. A snapshot that exists but
// is corrupt/oversized is a hard error — silently treating it as "no
// snapshot" would, after a log compaction, reset the node to empty state
// at term 0 (total data loss).
func (s snapshotStore) loadLatest() (SnapshotMeta, []byte, bool, error) {
	names, err := s.listNames()
	if err != nil {
		return SnapshotMeta{}, nil, false, err
	}
	return s.pickBest(names)
}

func (s snapshotStore) load(meta SnapshotMeta) ([]byte, error) {
	name := snapshotName(meta)
	raw, err := readSnapshotFile(filepath.Join(s.dir, name))
	if err != nil {
		return nil, err
	}
	got, fsm, err := decodeSnapshotFile(raw)
	if err != nil {
		return nil, fmt.Errorf("raft: snapshot %s is corrupt: %w", name, err)
	}
	if got.LastIncludedIndex != meta.LastIncludedIndex || got.LastIncludedTerm != meta.LastIncludedTerm ||
		!configurationsEqual(got.Configuration, meta.Configuration) {
		return nil, fmt.Errorf("raft: snapshot %s metadata mismatch", name)
	}
	return fsm, nil
}

func configurationsEqual(a, b Configuration) bool {
	aData, aErr := encodeRPCConfig(a)
	bData, bErr := encodeRPCConfig(b)
	return aErr == nil && bErr == nil && bytes.Equal(aData, bData)
}

// pickBest tries snapshot files in descending-index order and returns
// the first that reads + decodes. If that one is corrupt/oversized, it
// errors (rather than falling back to an older snapshot — the entries
// between them are not retained anywhere). Files that race a concurrent
// delete (ENOENT) or don't parse a valid name are skipped.
func (s snapshotStore) pickBest(names []string) (SnapshotMeta, []byte, bool, error) {
	for _, n := range sortSnapshotNamesDesc(names) {
		raw, err := readSnapshotFile(filepath.Join(s.dir, n))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return SnapshotMeta{}, nil, false, err
		}
		meta, fsm, err := decodeSnapshotFile(raw)
		if err != nil {
			return SnapshotMeta{}, nil, false, fmt.Errorf("raft: snapshot %s is corrupt: %w", n, err)
		}
		return meta, fsm, true, nil
	}
	return SnapshotMeta{}, nil, false, nil
}

// sortSnapshotNamesDesc returns names sorted by their encoded index,
// highest first; names that don't parse sort last.
func sortSnapshotNamesDesc(names []string) []string {
	out := append([]string(nil), names...)
	idx := func(n string) Index { i, _, _ := parseSnapshotName(n); return i }
	sort.Slice(out, func(a, b int) bool { return idx(out[a]) > idx(out[b]) })
	return out
}

func readSnapshotFile(path string) ([]byte, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err // caller distinguishes ENOENT
	}
	if fi.Size() > int64(maxSnapshotFileBytes) {
		return nil, fmt.Errorf("raft: snapshot %s is %d bytes (max %d)", path, fi.Size(), maxSnapshotFileBytes)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read snapshot %s: %w", path, err)
	}
	return raw, nil
}

// parseSnapshotName is the inverse of snapshotName (used by tests).
func parseSnapshotName(name string) (Index, Term, bool) {
	rest, ok := strings.CutPrefix(name, snapshotNamePrefix)
	if !ok {
		return 0, 0, false
	}
	i, t, ok := strings.Cut(rest, "-")
	if !ok {
		return 0, 0, false
	}
	idx, err1 := strconv.ParseUint(i, 10, 64)
	term, err2 := strconv.ParseUint(t, 10, 64)
	return Index(idx), Term(term), err1 == nil && err2 == nil
}
