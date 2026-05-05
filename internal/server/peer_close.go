package server

import (
	"bufio"
	"errors"
	"net"
	"time"
)

const (
	peerCloseWatchDelay = 10 * time.Millisecond
	peerCloseWatchPoll  = 50 * time.Millisecond
)

// aLongTimeAgo is a sentinel past time used to force an in-flight read
// to return immediately with a timeout. Same pattern net/http uses.
var aLongTimeAgo = time.Unix(1, 0)

// watchPeerClose polls the connection while a blocking lock op runs so
// a client that disconnects gets its waiter cancelled instead of
// hanging out for the full timeout. Returns a stop function that the
// caller MUST invoke before resuming protocol reads.
//
// Concurrency invariant: the bufio.Reader is shared with the main
// handler goroutine. This is only safe because the caller (ServeConn)
// guarantees:
//
//  1. The watcher goroutine is spawned only while the main goroutine
//     is inside handleRequest (i.e. not reading from `reader`).
//  2. stop() is called before the next ReadRequest and blocks on
//     `<-done`, so the watcher has fully exited before the main
//     goroutine touches `reader` again.
//
// Any change that breaks either invariant introduces a data race.
func watchPeerClose(reader *bufio.Reader, conn net.Conn, cancelConn func()) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)

		// Don't bother peeking on fast (uncontended) commands.
		timer := time.NewTimer(peerCloseWatchDelay)
		defer timer.Stop()
		select {
		case <-stop:
			return
		case <-timer.C:
		}

		for {
			select {
			case <-stop:
				_ = conn.SetReadDeadline(time.Time{})
				return
			default:
			}

			peekN := reader.Buffered() + 1
			if peekN > reader.Size() {
				// Pipelined data has filled the buffer behind a
				// blocking command. We can't observe EOF without
				// consuming it, and a full pipeline is the abuse case
				// — disconnect rather than queue waiters indefinitely.
				cancelConn()
				return
			}

			_ = conn.SetReadDeadline(time.Now().Add(peerCloseWatchPoll))
			_, err := reader.Peek(peekN)
			_ = conn.SetReadDeadline(time.Time{})

			select {
			case <-stop:
				return
			default:
			}
			switch {
			case err == nil:
				continue // more pipelined bytes; loop and re-peek further
			case isTimeoutErr(err):
				continue
			default:
				cancelConn()
				return
			}
		}
	}()

	return func() {
		close(stop)
		// Force any in-flight Peek to return immediately. Without this
		// the watcher sits in Peek up to peerCloseWatchPoll before
		// noticing close(stop), adding latency to every blocking
		// response.
		_ = conn.SetReadDeadline(aLongTimeAgo)
		<-done
		_ = conn.SetReadDeadline(time.Time{})
	}
}

func isTimeoutErr(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}
