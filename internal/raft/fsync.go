package raft

import (
	"fmt"
	"os"
	"path/filepath"
)

// fsyncFile flushes f's data and metadata to stable storage.
func fsyncFile(f *os.File) error {
	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync %s: %w", f.Name(), err)
	}
	return nil
}

// fsyncDir flushes the directory entry for path's parent so a prior
// create/rename in it is durable. On platforms where directory fsync is
// rejected the error is returned; callers that consider it best-effort
// should log-and-continue.
func fsyncDir(path string) error {
	dir := filepath.Dir(path)
	f, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir %s: %w", dir, err)
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync dir %s: %w", dir, err)
	}
	return nil
}

// writeFileAtomic writes data to a temp file beside path, fsyncs it,
// renames it over path, and fsyncs the directory. The rename is atomic,
// so a crash leaves either the old file or the new one — never a torn one.
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	tmp := path + ".tmp"
	if err := writeAndSyncTmp(tmp, data, perm); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename %s -> %s: %w", tmp, path, err)
	}
	return fsyncDir(path)
}

func writeAndSyncTmp(tmp string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, perm)
	if err != nil {
		return fmt.Errorf("create %s: %w", tmp, err)
	}
	if err := writeAllThenSync(f, data); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return err
	}
	return f.Close()
}

func writeAllThenSync(f *os.File, data []byte) error {
	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("write %s: %w", f.Name(), err)
	}
	return fsyncFile(f)
}
