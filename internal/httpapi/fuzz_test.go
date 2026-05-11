package httpapi

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mtingers/dflockd/internal/protocol"
)

func FuzzRESTValidators(f *testing.F) {
	for _, seed := range []string{
		"k",
		"deploy-job",
		"",
		"bad key",
		"bad\nkey",
		strings.Repeat("x", protocol.MaxLineBytes+1),
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, value string) {
		if err := validateRESTKey(value); err == nil {
			assertValidRESTKey(t, value)
		}
		if err := validateProtocolField("token", value); err == nil {
			assertValidProtocolField(t, value)
		}
	})
}

func assertValidRESTKey(t *testing.T, key string) {
	t.Helper()
	if key == "" {
		t.Fatal("validateRESTKey accepted empty key")
	}
	assertValidProtocolField(t, key)
}

func assertValidProtocolField(t *testing.T, value string) {
	t.Helper()
	if len(value) > protocol.MaxLineBytes {
		t.Fatalf("validator accepted overlong value of %d bytes", len(value))
	}
	if strings.ContainsAny(value, " \t\n\r") {
		t.Fatalf("validator accepted whitespace-bearing value %q", value)
	}
}

func FuzzDecodeJSONBody(f *testing.F) {
	for _, seed := range []struct {
		body       []byte
		allowEmpty bool
	}{
		{[]byte(`{"acquire_timeout_s":1}`), false},
		{[]byte(`{"acquire_timeout_s":1,"lease_ttl_s":60}`), false},
		{[]byte(`{"unknown":1}`), false},
		{[]byte(`{not json`), false},
		{[]byte(``), true},
		{[]byte(`[]`), false},
		{[]byte(`null`), false},
	} {
		f.Add(seed.body, seed.allowEmpty)
	}

	f.Fuzz(func(t *testing.T, body []byte, allowEmpty bool) {
		if len(body) > 2*maxRequestBody {
			t.Skip("body too large for this in-process fuzz target")
		}
		req := httptest.NewRequest(http.MethodPost, "/v1/locks/k", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		var decoded acquireRequest
		_ = decodeJSONBody(rec, req, &decoded, allowEmpty)
	})
}
