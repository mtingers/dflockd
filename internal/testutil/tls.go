// Package testutil holds shared test helpers.
package testutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"
)

// genSelfSigned creates an ephemeral ECDSA P-256 self-signed cert valid
// for 127.0.0.1 / localhost with the given extended key usages, and
// returns its PEM bytes, a usable tls.Certificate, and a cert pool that
// trusts it.
func genSelfSigned(t *testing.T, ekus ...x509.ExtKeyUsage) (certPEM []byte, cert tls.Certificate, pool *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "dflockd-test"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  ekus,
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err = tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	pool = x509.NewCertPool()
	pool.AppendCertsFromPEM(certPEM)
	return certPEM, cert, pool
}

// SelfSignedTLS generates an ephemeral cert valid for 127.0.0.1 /
// localhost. Returns a server tls.Config and a client tls.Config whose
// root pool trusts the generated cert.
func SelfSignedTLS(t *testing.T) (serverCfg, clientCfg *tls.Config) {
	t.Helper()
	_, cert, pool := genSelfSigned(t, x509.ExtKeyUsageServerAuth)
	serverCfg = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	clientCfg = &tls.Config{RootCAs: pool}
	return serverCfg, clientCfg
}

// MutualTLS returns one *tls.Config usable on both ends of a connection:
// it presents an ephemeral cert (with server- and client-auth EKUs) and
// trusts that same cert as the CA, requiring the peer to present a
// verified cert. Suitable for testing a mutual-TLS transport on loopback.
func MutualTLS(t *testing.T) *tls.Config {
	t.Helper()
	_, cert, pool := genSelfSigned(t, x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth)
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}
}
