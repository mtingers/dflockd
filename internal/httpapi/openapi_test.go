package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/mtingers/dflockd/internal/config"
)

// TestOpenAPI_DocsCopyMatchesEmbedded fails when docs/openapi.json
// has drifted from the embedded spec. Run `make openapi-sync` to
// re-sync before commit.
func TestOpenAPI_DocsCopyMatchesEmbedded(t *testing.T) {
	docsCopy, err := os.ReadFile(repoRelPath(t, "docs/openapi.json"))
	if err != nil {
		t.Fatalf("read docs/openapi.json: %v", err)
	}
	if !bytes.Equal(docsCopy, openAPISpec) {
		t.Fatalf("docs/openapi.json has drifted from internal/httpapi/openapi.json — run `make openapi-sync`")
	}
}

// TestOpenAPI_EmbeddedIsValidJSON guards against an embed-time typo.
func TestOpenAPI_EmbeddedIsValidJSON(t *testing.T) {
	var v any
	if err := json.Unmarshal(openAPISpec, &v); err != nil {
		t.Fatalf("embedded openapi.json is not valid JSON: %v", err)
	}
}

// TestOpenAPI_RoutesMatchSpec asserts that every registered route
// appears as a path in the spec, and vice versa. Catches the case
// where someone adds a handler but forgets to update the spec.
func TestOpenAPI_RoutesMatchSpec(t *testing.T) {
	spec := decodeSpec(t)
	specPaths := pathsInSpec(spec)
	codePaths := pathsInRoutes()
	for p := range codePaths {
		if !specPaths[p] {
			t.Errorf("route %q is registered but not in openapi.json", p)
		}
	}
	for p := range specPaths {
		if !codePaths[p] {
			t.Errorf("openapi.json documents %q but no route is registered", p)
		}
	}
}

// TestOpenAPI_EndpointServesEmbedded fires the live HTTP server,
// fetches /v1/openapi.json, and checks the bytes match the embed.
// Auth is enabled in this test to verify the endpoint is exempt.
func TestOpenAPI_EndpointServesEmbedded(t *testing.T) {
	base, stop := startHTTP(t, withAuthToken("super-secret"))
	defer stop()

	resp, err := http.Get(base + "/v1/openapi.json")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}
	got, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(got, openAPISpec) {
		t.Fatal("served bytes don't match embedded openapi.json")
	}
}

// withAuthToken is a config mod for tests that need auth enabled.
func withAuthToken(token string) func(*config.Config) {
	return func(c *config.Config) { c.AuthToken = token }
}

func decodeSpec(t *testing.T) map[string]any {
	t.Helper()
	var spec map[string]any
	if err := json.Unmarshal(openAPISpec, &spec); err != nil {
		t.Fatal(err)
	}
	return spec
}

func pathsInSpec(spec map[string]any) map[string]bool {
	out := map[string]bool{}
	paths, _ := spec["paths"].(map[string]any)
	for p := range paths {
		out[p] = true
	}
	return out
}

func pathsInRoutes() map[string]bool {
	out := map[string]bool{}
	for _, r := range Routes() {
		out[r.Pattern] = true
	}
	return out
}

// repoRelPath returns an absolute path inside the repo. The test
// runs from internal/httpapi, so we walk up to find the repo root.
func repoRelPath(t *testing.T, rel string) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	dir := filepath.Dir(file)
	for i := 0; i < 6; i++ { // up to repo root
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return filepath.Join(dir, rel)
		}
		dir = filepath.Dir(dir)
	}
	t.Fatalf("couldn't find go.mod above %s", file)
	return ""
}

