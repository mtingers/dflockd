package raft

import "io"

// readAllAndClose drains rc and closes it, returning the first error.
func readAllAndClose(rc io.ReadCloser) ([]byte, error) {
	b, err := io.ReadAll(rc)
	if cerr := rc.Close(); err == nil {
		err = cerr
	}
	return b, err
}
