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

// aLongTimeAgo is a sentinel past time used to force an in-flight
// read to return immediately with a timeout.
var aLongTimeAgo = time.Unix(1, 0)

// watchPeerClose polls the connection while a blocking lock op runs
// so a disconnected client gets its waiter cancelled. Returns a stop
// function the caller MUST invoke before resuming protocol reads.
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
func watchPeerClose(reader *bufio.Reader, conn net.Conn, cancelConn func()) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go runPeerWatch(reader, conn, cancelConn, stop, done)
	return func() { stopPeerWatch(conn, stop, done) }
}

// runPeerWatch is the watcher goroutine body.
func runPeerWatch(reader *bufio.Reader, conn net.Conn, cancelConn func(), stop, done chan struct{}) {
	defer close(done)
	if waitInitialDelay(stop) {
		peerWatchLoop(reader, conn, cancelConn, stop)
	}
}

func peerWatchLoop(reader *bufio.Reader, conn net.Conn, cancelConn func(), stop <-chan struct{}) {
	for {
		if !peerWatchStep(reader, conn, cancelConn, stop) {
			return
		}
	}
}

func peerWatchStep(reader *bufio.Reader, conn net.Conn, cancelConn func(), stop <-chan struct{}) bool {
	if isStopRequested(stop) {
		clearReadDeadline(conn)
		return false
	}
	return peekStep(reader, conn, cancelConn, stop)
}

// waitInitialDelay sleeps peerCloseWatchDelay so fast/uncontended
// commands aren't poked. Returns false if stopped during the delay.
func waitInitialDelay(stop <-chan struct{}) bool {
	timer := time.NewTimer(peerCloseWatchDelay)
	defer timer.Stop()
	return waitTimer(stop, timer.C)
}

func waitTimer(stop <-chan struct{}, timer <-chan time.Time) bool {
	return waitForTimer(stop, timer)
}

func isStopRequested(stop <-chan struct{}) bool {
	return channelReady(stop)
}

func channelReady(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// peekStep performs one peek+classify cycle. Returns false to exit
// the loop (peer gone, full pipeline, or stop requested mid-peek).
func peekStep(reader *bufio.Reader, conn net.Conn, cancelConn func(), stop <-chan struct{}) bool {
	peekN, ok := peekSize(reader)
	if !ok {
		return cancelFullPeek(cancelConn)
	}
	return timedPeekStep(reader, conn, peekN, cancelConn, stop)
}

func cancelFullPeek(cancelConn func()) bool {
	cancelConn()
	return false
}

func timedPeekStep(reader *bufio.Reader, conn net.Conn, peekN int, cancelConn func(), stop <-chan struct{}) bool {
	err := timedPeek(reader, conn, peekN)
	if isStopRequested(stop) {
		return false
	}
	return classifyPeek(err, cancelConn)
}

func peekSize(reader *bufio.Reader) (int, bool) {
	peekN := reader.Buffered() + 1
	return peekN, peekN <= reader.Size()
}

// timedPeek peeks under a short read deadline and clears the deadline
// before returning.
func timedPeek(reader *bufio.Reader, conn net.Conn, peekN int) error {
	_ = conn.SetReadDeadline(time.Now().Add(peerCloseWatchPoll))
	_, err := reader.Peek(peekN)
	_ = conn.SetReadDeadline(time.Time{})
	return err
}

// classifyPeek decides what to do with a peek result. Returns true
// to keep looping, false to exit (peer gone).
func classifyPeek(err error, cancelConn func()) bool {
	if peekKeptAlive(err) {
		return true
	}
	cancelConn()
	return false
}

func peekKeptAlive(err error) bool {
	return err == nil || isTimeoutErr(err)
}

// stopPeerWatch closes stop, jolts any in-flight Peek with the
// long-ago deadline so it returns immediately, and waits for the
// watcher to exit.
func stopPeerWatch(conn net.Conn, stop, done chan struct{}) {
	close(stop)
	_ = conn.SetReadDeadline(aLongTimeAgo)
	<-done
	clearReadDeadline(conn)
}

func clearReadDeadline(conn net.Conn) {
	_ = conn.SetReadDeadline(time.Time{})
}

func isTimeoutErr(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}
