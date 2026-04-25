package replication

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// peerConn is a framed connection to the peer node. It serialises
// writes via writeMu and reads in a single dedicated goroutine so the
// rest of the replicator never has to coordinate I/O directly.
//
// peerConn does not own retries or reconnects — those are the
// replicator's job. peerConn is one bound, unbound either side, and
// then disposed.
type peerConn struct {
	conn    net.Conn
	writeMu sync.Mutex

	closeOnce sync.Once
	closed    chan struct{}
	closeErr  atomic.Pointer[error]
}

func newPeerConn(c net.Conn) *peerConn {
	return &peerConn{conn: c, closed: make(chan struct{})}
}

func (p *peerConn) WriteFrame(f *Frame) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	return WriteFrame(p.conn, f)
}

func (p *peerConn) ReadFrame() (*Frame, error) {
	return ReadFrame(p.conn)
}

func (p *peerConn) Close(cause error) {
	p.closeOnce.Do(func() {
		if cause != nil {
			p.closeErr.Store(&cause)
		}
		_ = p.conn.Close()
		close(p.closed)
	})
}

func (p *peerConn) Done() <-chan struct{} { return p.closed }

func (p *peerConn) Cause() error {
	if e := p.closeErr.Load(); e != nil {
		return *e
	}
	return nil
}

// dialPeer establishes a peer connection to addr. tlsCfg is nil for
// plain TCP. The returned peerConn is ready for use; the caller is
// responsible for the handshake exchange.
func dialPeer(ctx context.Context, addr string, tlsCfg *tls.Config, dialTimeout time.Duration) (*peerConn, error) {
	if dialTimeout <= 0 {
		dialTimeout = 5 * time.Second
	}
	d := &net.Dialer{Timeout: dialTimeout, KeepAlive: 15 * time.Second}
	var conn net.Conn
	var err error
	if tlsCfg != nil {
		conn, err = tls.DialWithDialer(d, "tcp", addr, tlsCfg)
	} else {
		conn, err = d.DialContext(ctx, "tcp", addr)
	}
	if err != nil {
		return nil, fmt.Errorf("dial peer %s: %w", addr, err)
	}
	return newPeerConn(conn), nil
}

// acceptPeer is called from the listener loop on the receiving side.
// It wraps the inbound conn and returns a peerConn ready for handshake.
func acceptPeer(c net.Conn) *peerConn {
	return newPeerConn(c)
}

// ---------------------------------------------------------------------------
// Listener for the secondary side
// ---------------------------------------------------------------------------

// runPeerListener accepts inbound peer connections on lis. For each
// accepted conn, accepted is called (synchronously per conn). The
// caller is responsible for goroutine management inside accepted.
func runPeerListener(ctx context.Context, lis net.Listener, accepted func(*peerConn)) error {
	go func() {
		<-ctx.Done()
		_ = lis.Close()
	}()
	for {
		c, err := lis.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("peer accept: %w", err)
		}
		accepted(acceptPeer(c))
	}
}

// drainFramesUntilClose reads frames in a loop until the conn closes
// or ctx is cancelled. Each frame is forwarded to onFrame. Read
// errors close the peerConn with the underlying cause.
func drainFramesUntilClose(ctx context.Context, p *peerConn, onFrame func(*Frame) error) {
	defer p.Close(nil)
	for {
		f, err := p.ReadFrame()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return
			}
			p.Close(err)
			return
		}
		if err := onFrame(f); err != nil {
			p.Close(err)
			return
		}
		if ctx.Err() != nil {
			return
		}
	}
}
