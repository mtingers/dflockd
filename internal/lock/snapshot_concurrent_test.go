package lock

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"
)

func TestSnapshotCopiesConcurrentDirectState(t *testing.T) {
	src := newApplyTestLM(t)
	dst := newApplyTestLM(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; ctx.Err() == nil; i++ {
			key := fmt.Sprintf("lock:k-%d", i%16)
			token, err := src.Acquire(ctx, key, 0, time.Minute, uint64(i+1), 1)
			if err == nil && token != "" {
				_, _ = src.Release(key, token)
			}
		}
	}()

	for i := 0; i < 200; i++ {
		var snapshot bytes.Buffer
		if err := src.Snapshot(&snapshot); err != nil {
			cancel()
			<-done
			t.Fatalf("Snapshot %d: %v", i, err)
		}
		if err := dst.Restore(bytes.NewReader(snapshot.Bytes())); err != nil {
			cancel()
			<-done
			t.Fatalf("Restore %d: %v", i, err)
		}
	}
	cancel()
	<-done
}
