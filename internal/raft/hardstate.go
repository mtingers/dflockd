package raft

import (
	"bytes"
	"fmt"
	"math"
	"os"
)

// HardState is persisted in a fixed-size two-slot journal: each Save
// writes the slot not written last (chosen by sequence-number parity)
// and fsyncs it, so a crash mid-write leaves the previous slot intact.
// On load, the slot with the highest sequence number that has a valid
// magic and CRC wins. (Same shape as internal/lock's fence journal.)

const (
	hardStateSlotBytes = 512
	hardStateFileBytes = hardStateSlotBytes * 2
	hardStateCRCOff    = hardStateSlotBytes - 8
	hardStateFilePerm  = 0o600
)

var hardStateMagic = [8]byte{'d', 'f', 'l', 'r', 'f', 'h', 's', '1'}

type hardStateFile struct {
	path     string
	f        *os.File
	nextSeq  uint64
	pristine bool
}

// openHardStateFile opens (creating if absent) the journal at path. Creation
// persists a valid zero-state sentinel; every existing file must contain a
// valid slot, so total zeroing cannot masquerade as a new journal.
func openHardStateFile(path string) (*hardStateFile, HardState, error) {
	_, statErr := os.Stat(path)
	created := os.IsNotExist(statErr)
	if statErr != nil && !created {
		return nil, HardState{}, fmt.Errorf("stat hardstate %s: %w", path, statErr)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, hardStateFilePerm)
	if err != nil {
		return nil, HardState{}, fmt.Errorf("open hardstate %s: %w", path, err)
	}
	if err := prepareHardStateSize(f); err != nil {
		f.Close()
		return nil, HardState{}, err
	}
	if created {
		h := &hardStateFile{path: path, f: f, nextSeq: 1, pristine: true}
		if err := h.writeSlot(0, HardState{}); err != nil {
			f.Close()
			return nil, HardState{}, err
		}
		if err := fsyncDir(path); err != nil {
			f.Close()
			return nil, HardState{}, err
		}
		return h, HardState{}, nil
	}
	return loadHardStateSlots(f, path)
}

func prepareHardStateSize(f *os.File) error {
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat %s: %w", f.Name(), err)
	}
	if fi.Size() == hardStateFileBytes {
		return nil
	}
	if fi.Size() > hardStateFileBytes {
		return fmt.Errorf("raft: hardstate %s has unexpected size %d", f.Name(), fi.Size())
	}
	raw := make([]byte, fi.Size())
	if len(raw) > 0 {
		if _, err := f.ReadAt(raw, 0); err != nil {
			return fmt.Errorf("read partial hardstate %s: %w", f.Name(), err)
		}
		if !allZero(raw) {
			return fmt.Errorf("raft: hardstate %s is truncated with nonzero data", f.Name())
		}
	}
	return ensureSize(f, hardStateFileBytes)
}

func loadHardStateSlots(f *os.File, path string) (*hardStateFile, HardState, error) {
	buf := make([]byte, hardStateFileBytes)
	if _, err := f.ReadAt(buf, 0); err != nil {
		f.Close()
		return nil, HardState{}, fmt.Errorf("read hardstate %s: %w", path, err)
	}
	hs, seq, ok := bestHardStateSlot(buf)
	if !ok {
		f.Close()
		return nil, HardState{}, fmt.Errorf("raft: hardstate %s has no valid journal slot", path)
	}
	pristine := seq == 0
	if pristine && (hs != (HardState{}) || !allZero(buf[hardStateSlotBytes:])) {
		f.Close()
		return nil, HardState{}, fmt.Errorf("raft: hardstate %s has a damaged pristine journal", path)
	}
	nextSeq := seq + 1
	if seq == math.MaxUint64 {
		nextSeq = math.MaxUint64
	}
	return &hardStateFile{path: path, f: f, nextSeq: nextSeq, pristine: pristine}, hs, nil
}

// bestHardStateSlot returns the HardState from the higher-sequence valid slot.
func bestHardStateSlot(buf []byte) (HardState, uint64, bool) {
	var best HardState
	var bestSeq uint64
	var found bool
	for i := 0; i < 2; i++ {
		slot := buf[i*hardStateSlotBytes : (i+1)*hardStateSlotBytes]
		if hs, seq, ok := decodeHardStateSlot(slot); ok && (!found || seq > bestSeq) {
			best, bestSeq, found = hs, seq, true
		}
	}
	return best, bestSeq, found
}

func allZero(buf []byte) bool {
	for _, b := range buf {
		if b != 0 {
			return false
		}
	}
	return true
}

func decodeHardStateSlot(slot []byte) (HardState, uint64, bool) {
	if !bytes.Equal(slot[0:8], hardStateMagic[:]) {
		return HardState{}, 0, false
	}
	if crc(slot[:hardStateCRCOff]) != be.Uint64(slot[hardStateCRCOff:]) {
		return HardState{}, 0, false
	}
	return parseHardStateBody(slot)
}

func parseHardStateBody(slot []byte) (HardState, uint64, bool) {
	seq := be.Uint64(slot[8:16])
	hs := HardState{CurrentTerm: Term(be.Uint64(slot[16:24])), CommitIndex: Index(be.Uint64(slot[24:32]))}
	votedLen := int(be.Uint16(slot[32:34]))
	if 34+votedLen > hardStateCRCOff {
		return HardState{}, 0, false
	}
	hs.VotedFor = NodeID(slot[34 : 34+votedLen])
	return hs, seq, true
}

// save writes hs to the next slot and fsyncs.
func (h *hardStateFile) save(hs HardState) error {
	if h.pristine {
		// The first real state replaces the pristine sentinel in two durable
		// steps. After success, later corruption of either slot cannot roll a
		// previously nonzero term/vote/commit all the way back to zero.
		if err := h.writeSlot(h.nextSeq, hs); err != nil {
			return err
		}
		h.nextSeq++
		h.pristine = false
		if err := h.writeSlot(h.nextSeq, hs); err != nil {
			return err
		}
		h.nextSeq++
		return nil
	}
	if h.nextSeq == math.MaxUint64 {
		return fmt.Errorf("raft: hardstate journal sequence exhausted")
	}
	seq := h.nextSeq
	if err := h.writeSlot(seq, hs); err != nil {
		return err
	}
	h.nextSeq = seq + 1
	return nil
}

func (h *hardStateFile) writeSlot(seq uint64, hs HardState) error {
	slot, err := encodeHardStateSlot(seq, hs)
	if err != nil {
		return err
	}
	if _, err := h.f.WriteAt(slot, int64(seq%2)*hardStateSlotBytes); err != nil {
		return fmt.Errorf("write hardstate %s: %w", h.path, err)
	}
	if err := fsyncFile(h.f); err != nil {
		return err
	}
	return nil
}

func encodeHardStateSlot(seq uint64, hs HardState) ([]byte, error) {
	if len(hs.VotedFor) > hardStateCRCOff-34 {
		return nil, fmt.Errorf("raft: votedFor too long (%d bytes)", len(hs.VotedFor))
	}
	slot := make([]byte, hardStateSlotBytes)
	copy(slot[0:8], hardStateMagic[:])
	be.PutUint64(slot[8:16], seq)
	be.PutUint64(slot[16:24], uint64(hs.CurrentTerm))
	be.PutUint64(slot[24:32], uint64(hs.CommitIndex))
	be.PutUint16(slot[32:34], uint16(len(hs.VotedFor)))
	copy(slot[34:], hs.VotedFor)
	be.PutUint64(slot[hardStateCRCOff:], crc(slot[:hardStateCRCOff]))
	return slot, nil
}

func (h *hardStateFile) close() error {
	if h.f == nil {
		return nil
	}
	err := h.f.Close()
	h.f = nil
	return err
}

// ensureSize grows f to at least size bytes (a freshly created journal is
// zero-length; this pads it so the two slots have stable offsets).
func ensureSize(f *os.File, size int64) error {
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat %s: %w", f.Name(), err)
	}
	if fi.Size() >= size {
		return nil
	}
	if err := f.Truncate(size); err != nil {
		return fmt.Errorf("grow %s to %d: %w", f.Name(), size, err)
	}
	return fsyncFile(f)
}
