package raft

import (
	"fmt"
	"os"
	"path/filepath"
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
	snapshotFilePerm   = 0o600
	snapshotNamePrefix = "snap-"
	snapshotHeaderMin  = 8 + 8 + 8 + 4 // magic + idx + term + configLen
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
	off := 28 + cfgLen
	if off+8 > len(raw)-8 {
		return Configuration{}, 0, fmt.Errorf("raft: snapshot config length out of range")
	}
	cfg, err := decodeConfig(raw[28:off])
	return cfg, off, err
}

func decodeSnapshotFSM(raw []byte, meta SnapshotMeta, off int) (SnapshotMeta, []byte, error) {
	fsmLen := int(be.Uint64(raw[off : off+8]))
	start, end := off+8, off+8+fsmLen
	if fsmLen < 0 || end > len(raw)-8 {
		return SnapshotMeta{}, nil, fmt.Errorf("raft: snapshot fsm length out of range")
	}
	return meta, append([]byte(nil), raw[start:end]...), nil
}

// snapshotStore manages the snapshot directory.
type snapshotStore struct{ dir string }

func (s snapshotStore) save(meta SnapshotMeta, fsm []byte) error {
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return fmt.Errorf("mkdir %s: %w", s.dir, err)
	}
	name := snapshotName(meta)
	if err := writeFileAtomic(filepath.Join(s.dir, name), encodeSnapshotFile(meta, fsm), snapshotFilePerm); err != nil {
		return err
	}
	return s.deleteAllExcept(name)
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
// ok is false if there is none.
func (s snapshotStore) loadLatest() (SnapshotMeta, []byte, bool, error) {
	names, err := s.listNames()
	if err != nil {
		return SnapshotMeta{}, nil, false, err
	}
	return s.pickBest(names)
}

func (s snapshotStore) pickBest(names []string) (SnapshotMeta, []byte, bool, error) {
	var bestMeta SnapshotMeta
	var bestFSM []byte
	found := false
	for _, n := range names {
		meta, fsm, ok := s.tryLoad(n)
		if ok && (!found || meta.LastIncludedIndex > bestMeta.LastIncludedIndex) {
			bestMeta, bestFSM, found = meta, fsm, true
		}
	}
	return bestMeta, bestFSM, found, nil
}

func (s snapshotStore) tryLoad(name string) (SnapshotMeta, []byte, bool) {
	raw, err := os.ReadFile(filepath.Join(s.dir, name))
	if err != nil {
		return SnapshotMeta{}, nil, false
	}
	meta, fsm, err := decodeSnapshotFile(raw)
	if err != nil {
		return SnapshotMeta{}, nil, false
	}
	return meta, fsm, true
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
