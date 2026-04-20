package httpapi

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// openAPIDoc is a structural view of the spec sufficient for the drift
// test. We deliberately don't depend on an OpenAPI validator library
// (would violate the zero-deps policy) — we only verify shape invariants.
type openAPIDoc struct {
	OpenAPI    string                             `json:"openapi"`
	Info       map[string]any                     `json:"info"`
	Servers    []any                              `json:"servers"`
	Paths      map[string]map[string]openAPIOp    `json:"paths"`
	Components map[string]any                     `json:"components"`
}

type openAPIOp struct {
	Summary     string `json:"summary"`
	Description string `json:"description"`
	OperationID string `json:"operationId"`
}

// TestOpenAPI_ValidJSON ensures the embedded spec parses.
func TestOpenAPI_ValidJSON(t *testing.T) {
	var doc openAPIDoc
	if err := json.Unmarshal(openAPISpec, &doc); err != nil {
		t.Fatalf("unmarshal openapi.json: %v", err)
	}
	if doc.OpenAPI == "" {
		t.Fatal("missing openapi version")
	}
	if doc.Info == nil {
		t.Fatal("missing info")
	}
	if title, _ := doc.Info["title"].(string); title == "" {
		t.Fatal("missing info.title")
	}
	if len(doc.Paths) == 0 {
		t.Fatal("no paths defined")
	}
	if doc.Components == nil {
		t.Fatal("missing components")
	}
}

// TestOpenAPI_VersionIs31 enforces that we're publishing OpenAPI 3.1
// (the drift test relies on JSON Schema 2020-12 semantics).
func TestOpenAPI_VersionIs31(t *testing.T) {
	var doc openAPIDoc
	if err := json.Unmarshal(openAPISpec, &doc); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(doc.OpenAPI, "3.1") {
		t.Fatalf("openapi version: %q, want 3.1.x", doc.OpenAPI)
	}
}

// TestOpenAPI_HandlersCoveredBySpec walks every path registered by the
// server (Routes()) and asserts the spec documents it.
func TestOpenAPI_HandlersCoveredBySpec(t *testing.T) {
	var doc openAPIDoc
	if err := json.Unmarshal(openAPISpec, &doc); err != nil {
		t.Fatal(err)
	}
	registered := Routes()

	for _, rt := range registered {
		p, ok := doc.Paths[rt.Pattern]
		if !ok {
			t.Errorf("spec missing path %q", rt.Pattern)
			continue
		}
		for _, m := range rt.Methods {
			method := strings.ToLower(m)
			if _, ok := p[method]; !ok {
				t.Errorf("spec missing %s %s", m, rt.Pattern)
			}
		}
	}
}

// TestOpenAPI_SpecPathsHaveHandlers asserts the reverse — no orphan
// documentation for unimplemented endpoints.
func TestOpenAPI_SpecPathsHaveHandlers(t *testing.T) {
	var doc openAPIDoc
	if err := json.Unmarshal(openAPISpec, &doc); err != nil {
		t.Fatal(err)
	}
	registered := make(map[string]map[string]bool)
	for _, rt := range Routes() {
		m := make(map[string]bool)
		for _, method := range rt.Methods {
			m[strings.ToLower(method)] = true
		}
		registered[rt.Pattern] = m
	}

	for pattern, methods := range doc.Paths {
		regMethods, ok := registered[pattern]
		if !ok {
			t.Errorf("spec has orphan path %q (no handler)", pattern)
			continue
		}
		for method := range methods {
			if !regMethods[method] {
				t.Errorf("spec has orphan operation: %s %s (no handler)", strings.ToUpper(method), pattern)
			}
		}
	}
}

// TestOpenAPI_AllOpsHaveDescription enforces that every operation has
// meaningful prose — the whole point of hand-authoring this spec.
func TestOpenAPI_AllOpsHaveDescription(t *testing.T) {
	var doc openAPIDoc
	if err := json.Unmarshal(openAPISpec, &doc); err != nil {
		t.Fatal(err)
	}
	for pattern, methods := range doc.Paths {
		for method, op := range methods {
			if op.Summary == "" {
				t.Errorf("missing summary: %s %s", strings.ToUpper(method), pattern)
			}
			if op.Description == "" || len(op.Description) < 30 {
				t.Errorf("missing/short description: %s %s", strings.ToUpper(method), pattern)
			}
			if op.OperationID == "" {
				t.Errorf("missing operationId: %s %s", strings.ToUpper(method), pattern)
			}
		}
	}
}

// TestOpenAPI_OperationIDsAreUnique catches typos where two operations
// accidentally share an operationId (a classic codegen hazard).
func TestOpenAPI_OperationIDsAreUnique(t *testing.T) {
	var doc openAPIDoc
	if err := json.Unmarshal(openAPISpec, &doc); err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]string)
	for pattern, methods := range doc.Paths {
		for method, op := range methods {
			key := strings.ToUpper(method) + " " + pattern
			if prev, ok := seen[op.OperationID]; ok {
				t.Errorf("duplicate operationId %q at %s (also at %s)", op.OperationID, key, prev)
			}
			seen[op.OperationID] = key
		}
	}
}

// TestOpenAPI_PathParamsDeclared catches paths that reference a
// {placeholder} without declaring it in parameters.
func TestOpenAPI_PathParamsDeclared(t *testing.T) {
	var raw map[string]any
	if err := json.Unmarshal(openAPISpec, &raw); err != nil {
		t.Fatal(err)
	}
	paths, _ := raw["paths"].(map[string]any)
	re := regexp.MustCompile(`\{([^}]+)\}`)
	for pattern, pathVal := range paths {
		declared := collectParameterNames(pathVal)
		for _, m := range re.FindAllStringSubmatch(pattern, -1) {
			name := m[1]
			if !declared[name] {
				t.Errorf("path %q uses {%s} but doesn't declare it as a parameter", pattern, name)
			}
		}
	}
}

// collectParameterNames walks a path item and returns the set of
// "in: path" parameter names, resolving $refs to components/parameters.
func collectParameterNames(pathVal any) map[string]bool {
	names := make(map[string]bool)
	m, ok := pathVal.(map[string]any)
	if !ok {
		return names
	}
	// Check both path-level parameters and op-level parameters.
	for _, v := range m {
		op, ok := v.(map[string]any)
		if !ok {
			continue
		}
		params, ok := op["parameters"].([]any)
		if !ok {
			continue
		}
		for _, p := range params {
			pm, ok := p.(map[string]any)
			if !ok {
				continue
			}
			if ref, ok := pm["$ref"].(string); ok {
				// Very simple resolution for our own refs.
				switch ref {
				case "#/components/parameters/KeyPath":
					names["key"] = true
				case "#/components/parameters/SessionIdPath":
					names["id"] = true
				}
				continue
			}
			if in, _ := pm["in"].(string); in != "path" {
				continue
			}
			if name, _ := pm["name"].(string); name != "" {
				names[name] = true
			}
		}
	}
	return names
}

// TestOpenAPI_DocsMirrorInSync ensures that docs/openapi.json matches the
// embedded internal/httpapi/openapi.json. Run `make openapi-sync` if this
// fails.
func TestOpenAPI_DocsMirrorInSync(t *testing.T) {
	// Walk up from this file's directory to find the repo root. We assume
	// the working directory is internal/httpapi when `go test` runs.
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root := cwd
	for i := 0; i < 6; i++ {
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
			break
		}
		root = filepath.Dir(root)
	}
	docsSpec, err := os.ReadFile(filepath.Join(root, "docs", "openapi.json"))
	if err != nil {
		t.Skipf("docs/openapi.json not found (run `make openapi-sync`): %v", err)
		return
	}
	if !bytes.Equal(bytes.TrimSpace(docsSpec), bytes.TrimSpace(openAPISpec)) {
		t.Fatal("docs/openapi.json is out of sync with internal/httpapi/openapi.json — run `make openapi-sync`")
	}
}

// TestOpenAPI_SchemaListStable dumps the set of defined schemas so that a
// future contributor who drops one gets a diff-friendly failure.
func TestOpenAPI_SchemaListStable(t *testing.T) {
	var raw map[string]any
	if err := json.Unmarshal(openAPISpec, &raw); err != nil {
		t.Fatal(err)
	}
	components, _ := raw["components"].(map[string]any)
	schemas, _ := components["schemas"].(map[string]any)
	var got []string
	for name := range schemas {
		got = append(got, name)
	}
	sort.Strings(got)
	want := []string{
		"AcquireRequest",
		"AcquireResponse",
		"CreateSessionResponse",
		"EnqueueRequest",
		"EnqueueResponse",
		"ErrorResponse",
		"IdleInfo",
		"LockInfo",
		"ReleaseRequest",
		"RenewRequest",
		"RenewResponse",
		"SemAcquireRequest",
		"SemEnqueueRequest",
		"SemInfo",
		"SignalChannelInfo",
		"SignalRequest",
		"SignalResponse",
		"StatsResponse",
		"WaitRequest",
		"WaitResponse",
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("schemas changed:\n got:  %v\n want: %v", got, want)
	}
}
