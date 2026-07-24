package raft

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// The write-ahead log is a single file of length-prefixed records:
//
//	recLen:u32  recCRC:u64(over payload)  payload
//
// where payload is an encoded Entry. Records are appended and fsync'd;
// suffix-truncate and post-snapshot compaction rewrite the whole file
// atomically (the inter-snapshot log is small, so rewriting it is cheap
// and far simpler than mid-file truncation with an offset index). A torn
// tail (partial write or bad CRC) is detected on open and discarded — it
// was never acknowledged, so dropping it is safe; the leader re-sends.

const walRecordHeaderBytes = 12 // u32 len + u64 crc
const walFilePerm = 0o600

// maxWALFileBytes bounds the WAL we'll read into memory on open. The
// inter-snapshot log is small by design (capped by SnapshotThresholdEntries
// × entry size); this rejects a corrupt/runaway file before it OOMs us.
const maxWALFileBytes = maxTCPFrameBytes // 64 MiB

var errWALClosed = errors.New("raft: wal file is closed")

type walFile struct {
	path string
	f    *os.File // append handle, positioned at EOF
}

// openWAL opens (creating if absent) the WAL at path, replays it into a
// slice of entries, truncates any torn tail, and leaves the file ready
// for appends.
func openWAL(path string) (*walFile, []Entry, error) {
	raw, err := readWALFile(path)
	if err != nil {
		return nil, nil, err
	}
	entries, good := parseWALRecords(raw)
	w := &walFile{path: path}
	if err := w.openAppendTruncated(int64(good)); err != nil {
		return nil, nil, err
	}
	if err := fsyncDir(path); err != nil { // make the dirent durable (file may be brand new)
		return nil, nil, err
	}
	return w, entries, nil
}

func readWALFile(path string) ([]byte, error) {
	fi, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("stat wal %s: %w", path, err)
	}
	if fi.Size() > maxWALFileBytes {
		return nil, fmt.Errorf("raft: wal %s is %d bytes (max %d) — refusing to load", path, fi.Size(), maxWALFileBytes)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read wal %s: %w", path, err)
	}
	return raw, nil
}

// parseWALRecords decodes as many whole, CRC-valid records as it can and
// returns the entries plus the byte offset just past the last good one
// (everything after that is a torn tail to be discarded).
func parseWALRecords(raw []byte) ([]Entry, int) {
	var entries []Entry
	off := 0
	for {
		e, n, ok := decodeWALRecord(raw[off:])
		if !ok {
			return entries, off
		}
		entries = append(entries, e)
		off += n
	}
}

// decodeWALRecord parses one record from the front of b. ok is false on a
// truncated header/body or a CRC mismatch (the caller treats it as the
// torn tail).
func decodeWALRecord(b []byte) (Entry, int, bool) {
	if len(b) < walRecordHeaderBytes {
		return Entry{}, 0, false
	}
	payLen := int(be.Uint32(b[0:4]))
	want := be.Uint64(b[4:12])
	end := walRecordHeaderBytes + payLen
	if payLen <= 0 || payLen > maxEntryDataBytes+64 || len(b) < end {
		return Entry{}, 0, false
	}
	return decodeWALPayload(b[walRecordHeaderBytes:end], want, end)
}

func decodeWALPayload(payload []byte, wantCRC uint64, recLen int) (Entry, int, bool) {
	if crc(payload) != wantCRC {
		return Entry{}, 0, false
	}
	e, n, err := decodeEntry(payload)
	if err != nil || n != len(payload) {
		return Entry{}, 0, false
	}
	return e, recLen, true
}

func (w *walFile) openAppendTruncated(size int64) error {
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_RDWR, walFilePerm)
	if err != nil {
		return fmt.Errorf("open wal %s: %w", w.path, err)
	}
	if err := truncateAndSeekEnd(f, size); err != nil {
		f.Close()
		return err
	}
	w.f = f
	return nil
}

func truncateAndSeekEnd(f *os.File, size int64) error {
	if err := f.Truncate(size); err != nil {
		return fmt.Errorf("truncate %s: %w", f.Name(), err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek %s: %w", f.Name(), err)
	}
	return nil
}

// appendEntries writes es as records and fsyncs. A partial write (e.g.
// ENOSPC) is rolled back so the next append can't bury the torn bytes
// mid-file (which parseWALRecords would treat as the tail and silently
// drop everything after it).
func (w *walFile) appendEntries(es []Entry) error {
	if len(es) == 0 {
		return nil
	}
	if w.f == nil {
		return errWALClosed
	}
	off, err := w.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("wal %s: tell: %w", w.path, err)
	}
	if _, err := w.f.Write(encodeWALRecords(nil, es)); err != nil {
		_ = w.f.Truncate(off)
		_, _ = w.f.Seek(off, io.SeekStart)
		return fmt.Errorf("append wal %s: %w", w.path, err)
	}
	return fsyncFile(w.f)
}

func encodeWALRecords(dst []byte, es []Entry) []byte {
	for _, e := range es {
		dst = encodeWALRecord(dst, e)
	}
	return dst
}

func encodeWALRecord(dst []byte, e Entry) []byte {
	payload := encodeEntry(nil, e)
	dst = be.AppendUint32(dst, uint32(len(payload)))
	dst = be.AppendUint64(dst, crc(payload))
	return append(dst, payload...)
}

// rewrite atomically replaces the WAL contents with exactly es, then
// reopens the append handle. Used by suffix-truncate and compaction.
//
// The old handle is kept until writeFileAtomic succeeds, so a failure
// there (ENOSPC, EIO) leaves w.f valid and pointing at the unchanged
// file. If the reopen after a successful rename fails, w.f is left nil
// — the on-disk WAL is correct (recovery will pick it up), and
// appendEntries returns errWALClosed rather than panicking on a nil
// dereference.
func (w *walFile) rewrite(es []Entry) error {
	body := encodeWALRecords(nil, es)
	if err := writeFileAtomic(w.path, body, walFilePerm); err != nil {
		return err // w.f still valid (old file unchanged)
	}
	// The rename replaced the inode our handle pointed at; reopen.
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("close wal before reopen: %w", err)
	}
	w.f = nil
	return w.openAppendTruncated(int64(len(body)))
}

// replacePrepared atomically installs an already-fsynced WAL generation.
// The current append handle remains usable until the rename succeeds.
func (w *walFile) replacePrepared(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat prepared wal %s: %w", path, err)
	}
	if err := os.Rename(path, w.path); err != nil {
		return fmt.Errorf("replace wal %s: %w", w.path, err)
	}
	if err := fsyncDir(w.path); err != nil {
		return err
	}
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("close wal before reopen: %w", err)
	}
	w.f = nil
	return w.openAppendTruncated(info.Size())
}

func (w *walFile) close() error {
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}
