package raft

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"fmt"
	"net"
	"time"
)

const (
	minClusterSecretBytes = 32
	handshakeValueBytes   = sha256.Size
	secureFrameOverhead   = 1 + 8 + 16 // kind + sequence + AES-GCM tag

	proofClientHello  byte = 1
	proofServerHello  byte = 2
	proofClientFinal  byte = 3
	keyClientToServer byte = 4
	keyServerToClient byte = 5
)

type handshakeHello struct {
	id    NodeID
	nonce [handshakeValueBytes]byte
	proof [handshakeValueBytes]byte
}

func newClientHello(secret []byte, id NodeID) (handshakeHello, error) {
	h := handshakeHello{id: id}
	if _, err := rand.Read(h.nonce[:]); err != nil {
		return handshakeHello{}, fmt.Errorf("raft: generate client handshake nonce: %w", err)
	}
	h.proof = handshakeDigest(secret, proofClientHello, h, handshakeHello{})
	return h, nil
}

func newServerHello(secret []byte, client handshakeHello, id NodeID) (handshakeHello, error) {
	h := handshakeHello{id: id}
	if _, err := rand.Read(h.nonce[:]); err != nil {
		return handshakeHello{}, fmt.Errorf("raft: generate server handshake nonce: %w", err)
	}
	h.proof = handshakeDigest(secret, proofServerHello, client, h)
	return h, nil
}

func verifyClientHello(secret []byte, client handshakeHello) error {
	want := handshakeDigest(secret, proofClientHello, client, handshakeHello{})
	if !hmac.Equal(client.proof[:], want[:]) {
		return fmt.Errorf("raft: client handshake authentication failed")
	}
	return nil
}

func verifyServerHello(secret []byte, client, server handshakeHello) error {
	want := handshakeDigest(secret, proofServerHello, client, server)
	if !hmac.Equal(server.proof[:], want[:]) {
		return fmt.Errorf("raft: server handshake authentication failed")
	}
	return nil
}

func clientFinalProof(secret []byte, client, server handshakeHello) [handshakeValueBytes]byte {
	return handshakeDigest(secret, proofClientFinal, client, server)
}

func verifyClientFinal(secret []byte, client, server handshakeHello, proof [handshakeValueBytes]byte) error {
	want := clientFinalProof(secret, client, server)
	if !hmac.Equal(proof[:], want[:]) {
		return fmt.Errorf("raft: client final authentication failed")
	}
	return nil
}

func handshakeDigest(secret []byte, purpose byte, client, server handshakeHello) [handshakeValueBytes]byte {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte("dflockd-raft-handshake\x00"))
	_, _ = mac.Write([]byte(tcpProtoVersion))
	_, _ = mac.Write([]byte{purpose})
	writeHandshakeParty(mac, client)
	writeHandshakeParty(mac, server)
	var out [handshakeValueBytes]byte
	copy(out[:], mac.Sum(nil))
	return out
}

type byteWriter interface {
	Write([]byte) (int, error)
}

func writeHandshakeParty(w byteWriter, h handshakeHello) {
	id := appendString16(nil, string(h.id))
	_, _ = w.Write(id)
	_, _ = w.Write(h.nonce[:])
}

func encodeAuth(proof [handshakeValueBytes]byte) []byte {
	body := make([]byte, 1+handshakeValueBytes)
	body[0] = frameAuth
	copy(body[1:], proof[:])
	return body
}

func decodeAuth(body []byte) ([handshakeValueBytes]byte, error) {
	var proof [handshakeValueBytes]byte
	if len(body) != 1+handshakeValueBytes || first(body) != frameAuth {
		return proof, fmt.Errorf("raft: invalid auth frame")
	}
	copy(proof[:], body[1:])
	return proof, nil
}

type secureSession struct {
	readAEAD  cipher.AEAD
	writeAEAD cipher.AEAD
	readSeq   uint64
	writeSeq  uint64
}

func newSecureSession(secret []byte, client, server handshakeHello, clientSide bool) (*secureSession, error) {
	c2s, err := handshakeAEAD(secret, keyClientToServer, client, server)
	if err != nil {
		return nil, err
	}
	s2c, err := handshakeAEAD(secret, keyServerToClient, client, server)
	if err != nil {
		return nil, err
	}
	if clientSide {
		return &secureSession{readAEAD: s2c, writeAEAD: c2s}, nil
	}
	return &secureSession{readAEAD: c2s, writeAEAD: s2c}, nil
}

func handshakeAEAD(secret []byte, purpose byte, client, server handshakeHello) (cipher.AEAD, error) {
	key := handshakeDigest(secret, purpose, client, server)
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, fmt.Errorf("raft: initialize session cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("raft: initialize session AEAD: %w", err)
	}
	return aead, nil
}

func (s *secureSession) seal(plaintext []byte) ([]byte, error) {
	if s.writeSeq == ^uint64(0) {
		return nil, fmt.Errorf("raft: secure frame sequence exhausted")
	}
	seq := s.writeSeq + 1
	body := make([]byte, 1+8, 1+8+len(plaintext)+s.writeAEAD.Overhead())
	body[0] = frameSecure
	be.PutUint64(body[1:9], seq)
	nonce := secureNonce(s.writeAEAD.NonceSize(), seq)
	body = s.writeAEAD.Seal(body, nonce, plaintext, body[:9])
	s.writeSeq = seq
	return body, nil
}

func (s *secureSession) open(body []byte) ([]byte, error) {
	if len(body) < 1+8+s.readAEAD.Overhead() || first(body) != frameSecure {
		return nil, fmt.Errorf("raft: invalid secure frame")
	}
	if s.readSeq == ^uint64(0) {
		return nil, fmt.Errorf("raft: secure frame sequence exhausted")
	}
	seq := be.Uint64(body[1:9])
	if seq != s.readSeq+1 {
		return nil, fmt.Errorf("raft: secure frame sequence %d, want %d", seq, s.readSeq+1)
	}
	nonce := secureNonce(s.readAEAD.NonceSize(), seq)
	plaintext, err := s.readAEAD.Open(nil, nonce, body[9:], body[:9])
	if err != nil {
		return nil, fmt.Errorf("raft: secure frame authentication failed: %w", err)
	}
	s.readSeq = seq
	return plaintext, nil
}

func secureNonce(size int, seq uint64) []byte {
	nonce := make([]byte, size)
	be.PutUint64(nonce[size-8:], seq)
	return nonce
}

func writeSecureFrameTo(conn net.Conn, session *secureSession, body []byte, deadline time.Duration) error {
	protected, err := session.seal(body)
	if err != nil {
		return err
	}
	return writeFrameTo(conn, protected, deadline)
}

func readSecureFrame(conn net.Conn, session *secureSession, deadline time.Duration) ([]byte, error) {
	protected, err := readFrame(conn, deadline)
	if err != nil {
		return nil, err
	}
	return session.open(protected)
}

func verifyPeerTLSIdentity(conn net.Conn, id NodeID) error {
	tlsConn, ok := conn.(*tls.Conn)
	if !ok {
		return nil
	}
	state := tlsConn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return fmt.Errorf("raft: TLS peer presented no certificate")
	}
	certID := NodeID(state.PeerCertificates[0].Subject.CommonName)
	if certID == "" {
		return fmt.Errorf("raft: TLS peer certificate has no Common Name")
	}
	if certID != id {
		return fmt.Errorf("raft: TLS peer certificate identity %q does not match node ID %q", certID, id)
	}
	return nil
}
