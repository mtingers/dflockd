package client_test

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/mtingers/dflockd/client"
)

// TestSignalConnCloseUnblocksOnFullRespCh stands up a fake TCP server that
// emits unsolicited non-"sig" lines (the kind that go to respCh) without
// any client commands. With heartbeats disabled and no in-flight sendCmd,
// the second line fills the size-1 respCh buffer and the third blocks the
// readLoop. Before the fix, sc.Close() would deadlock on `<-sc.done`
// because closing the conn unblocks readLine but readLoop is parked in
// the channel send, not in readLine. After the fix, the readLoop's send
// races against closeCh, so Close returns within the timeout.
func TestSignalConnCloseUnblocksOnFullRespCh(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		// Emit several unsolicited response-shaped lines fast, with no
		// command on the wire. Two go into the buffered respCh; the third
		// blocks readLoop. Then idle until conn close.
		_, _ = conn.Write([]byte("ok\nok\nok\n"))
		buf := make([]byte, 1024)
		for {
			conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			if _, err := conn.Read(buf); err != nil {
				return
			}
		}
	}()

	c, err := client.Dial(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	sc := client.NewSignalConn(c, client.WithHeartbeatInterval(0))

	// Give the readLoop time to consume the unsolicited responses and
	// park on the third send.
	time.Sleep(50 * time.Millisecond)

	// Close must complete promptly. Before the fix this hangs forever.
	closed := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		closed <- sc.Close()
	}()

	select {
	case <-closed:
		// good
	case <-time.After(2 * time.Second):
		t.Fatal("SignalConn.Close did not return within 2s — readLoop is deadlocked on a full respCh")
	}
	wg.Wait()
	<-serverDone
}
