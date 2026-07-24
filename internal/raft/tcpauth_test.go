package raft

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/mtingers/dflockd/internal/testutil"
)

func TestTCPTransportRequiresClusterSecret(t *testing.T) {
	if tr, err := NewTCPTransport("a", "127.0.0.1:0", nil); err == nil {
		tr.Close()
		t.Fatal("NewTCPTransport accepted a missing cluster secret")
	}
	if tr, err := NewTCPTransport("a", "127.0.0.1:0", nil, WithClusterSecret("short")); err == nil {
		tr.Close()
		t.Fatal("NewTCPTransport accepted a short cluster secret")
	}
}

func TestHandshakeFrameRoundTrip(t *testing.T) {
	secret := []byte(testClusterSecret)
	client, err := newClientHello(secret, "node-a")
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeHello(encodeHello(client))
	if err != nil || got != client {
		t.Fatalf("hello round-trip = %#v, %v; want %#v", got, err, client)
	}
	server, err := newServerHello(secret, client, "node-b")
	if err != nil {
		t.Fatal(err)
	}
	proof := clientFinalProof(secret, client, server)
	gotProof, err := decodeAuth(encodeAuth(proof))
	if err != nil || gotProof != proof {
		t.Fatalf("auth round-trip = %x, %v; want %x", gotProof, err, proof)
	}
}

func TestTCPTransportRejectsWrongClusterSecret(t *testing.T) {
	otherSecret := "abcdef0123456789abcdef0123456789"
	trA, err := NewTCPTransport("a", "127.0.0.1:0", nil, WithClusterSecret(testClusterSecret))
	if err != nil {
		t.Fatal(err)
	}
	trB, err := NewTCPTransport("b", "127.0.0.1:0", nil, WithClusterSecret(otherSecret))
	if err != nil {
		t.Fatal(err)
	}
	defer trA.Close()
	defer trB.Close()
	trA.AddPeer("b", trB.ListenAddr())
	trB.SetHandler(func(NodeID, Message) Message { return &RequestVoteResp{} })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := trA.Send(ctx, "b", &RequestVoteReq{Term: 1}); err == nil {
		t.Fatal("peer with the wrong cluster secret completed an RPC")
	}
}

func TestSecureSessionRejectsTamperAndReplay(t *testing.T) {
	secret := []byte(testClusterSecret)
	client, err := newClientHello(secret, "a")
	if err != nil {
		t.Fatal(err)
	}
	server, err := newServerHello(secret, client, "b")
	if err != nil {
		t.Fatal(err)
	}
	clientSession, err := newSecureSession(secret, client, server, true)
	if err != nil {
		t.Fatal(err)
	}
	serverSession, err := newSecureSession(secret, client, server, false)
	if err != nil {
		t.Fatal(err)
	}

	plaintext := []byte("raft-rpc")
	protected, err := clientSession.seal(plaintext)
	if err != nil {
		t.Fatal(err)
	}
	tampered := append([]byte(nil), protected...)
	tampered[len(tampered)-1] ^= 1
	if _, err := serverSession.open(tampered); err == nil {
		t.Fatal("tampered frame authenticated")
	}
	got, err := serverSession.open(protected)
	if err != nil || !bytes.Equal(got, plaintext) {
		t.Fatalf("open valid frame = %q, %v", got, err)
	}
	if _, err := serverSession.open(protected); err == nil {
		t.Fatal("replayed frame authenticated")
	}
}

func TestTCPTransportTLSBindsCertificateToNodeID(t *testing.T) {
	cfg := testutil.MutualTLSNodes(t, "a")
	trA, err := newTestTCPTransport("a", "127.0.0.1:0", nil, WithTLS(cfg["a"]))
	if err != nil {
		t.Fatal(err)
	}
	// This transport claims b in its hello while presenting a's certificate.
	trB, err := newTestTCPTransport("b", "127.0.0.1:0", nil, WithTLS(cfg["a"]))
	if err != nil {
		t.Fatal(err)
	}
	defer trA.Close()
	defer trB.Close()
	trA.AddPeer("b", trB.ListenAddr())
	trB.SetHandler(func(NodeID, Message) Message { return &RequestVoteResp{} })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := trA.Send(ctx, "b", &RequestVoteReq{Term: 1}); err == nil {
		t.Fatal("peer whose certificate identity mismatched its NodeID completed an RPC")
	}
}
