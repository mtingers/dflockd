package raft

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// NewMutualTLSConfig builds a *tls.Config for the Raft transport that
// works in both roles: when dialing a peer it presents the cert in
// certFile and verifies the peer against caFile; when accepting it does
// the same and additionally *requires* the dialer to present a cert
// (RequireAndVerifyClientCert). TLS 1.3 minimum. All three paths must be
// non-empty; pass empty strings to disable TLS (returns nil, nil).
// The TCP transport separately requires each verified leaf certificate's
// Common Name to exactly match the peer's hello NodeID.
func NewMutualTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	if certFile == "" && keyFile == "" && caFile == "" {
		return nil, nil
	}
	if certFile == "" || keyFile == "" || caFile == "" {
		return nil, fmt.Errorf("raft: mutual TLS needs cert, key and CA — got cert=%q key=%q ca=%q", certFile, keyFile, caFile)
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("raft: load TLS keypair: %w", err)
	}
	pool, err := loadCertPool(caFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool, // verify the peer when we dial
		ClientCAs:    pool, // verify the peer when we accept
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

func loadCertPool(caFile string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("raft: read TLS CA %s: %w", caFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("raft: TLS CA %s contains no usable certificates", caFile)
	}
	return pool, nil
}
