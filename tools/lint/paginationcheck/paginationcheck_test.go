package main

import (
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestBadDir verifies that the checker finds violations in testdata/bad/.
func TestBadDir(t *testing.T) {
	violations, err := check("testdata/bad")
	if err != nil {
		t.Fatalf("check(testdata/bad): unexpected error: %v", err)
	}
	if len(violations) == 0 {
		t.Fatal("expected at least one violation in testdata/bad, got none")
	}
	joined := strings.Join(violations, "\n")
	// Both flagged methods must appear in the output.
	for _, want := range []string{"ListFoo", "DescribeBar"} {
		if !strings.Contains(joined, want) {
			t.Errorf("expected violation for %q; violations:\n%s", want, joined)
		}
	}
	// CreateFoo is not a List/Describe method — must NOT be reported.
	if strings.Contains(joined, "CreateFoo") {
		t.Errorf("CreateFoo should not be flagged; violations:\n%s", joined)
	}
	t.Logf("violations found in testdata/bad (%d):\n%s", len(violations), joined)
}

// TestProviderDir verifies that the checker runs without panic against the
// real provider tree. We expect violations there; this test just ensures the
// tool is stable and does not error out.
func TestProviderDir(t *testing.T) {
	violations, err := check("../../../internal/aws/provider")
	if err != nil {
		t.Fatalf("check(internal/aws/provider): unexpected error: %v", err)
	}
	t.Logf("violations found in internal/aws/provider (%d)", len(violations))
	// Deliberately not asserting zero — that would break as providers evolve.
}

// TestCheckFile_NoPagination verifies that a method without pagination keywords
// is flagged when checkFile is called directly.
func TestCheckFile_NoPagination(t *testing.T) {
	fset := token.NewFileSet()
	violations, err := checkFile(fset, "testdata/bad/bad.go")
	if err != nil {
		t.Fatalf("checkFile: %v", err)
	}
	if len(violations) == 0 {
		t.Fatal("expected violations from testdata/bad/bad.go, got none")
	}
}

// TestCheckFile_WithPagination verifies that a method whose body contains
// "NextToken" (as a map key or field name) is NOT flagged.
func TestCheckFile_WithPagination(t *testing.T) {
	src := `package p
type MyProvider struct{}
func (p *MyProvider) ListItems(tok string) []string {
	token, _ := nr.Params["NextToken"].(string)
	_ = token
	return nil
}
`
	tmp := writeTempFile(t, "ok.go", src)

	fset := token.NewFileSet()
	violations, err := checkFile(fset, tmp)
	if err != nil {
		t.Fatalf("checkFile: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("expected no violations for method with NextToken, got: %v", violations)
	}
}

// TestCheckFile_NonProviderReceiver verifies that List* methods on types whose
// name does not end in "Provider" are never flagged.
func TestCheckFile_NonProviderReceiver(t *testing.T) {
	src := `package p
type MyStore struct{}
func (s *MyStore) ListAll() []string { return nil }
`
	tmp := writeTempFile(t, "store.go", src)

	fset := token.NewFileSet()
	violations, err := checkFile(fset, tmp)
	if err != nil {
		t.Fatalf("checkFile: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("expected no violations for non-Provider receiver, got: %v", violations)
	}
}

// TestCheckFile_IsTruncated verifies that "IsTruncated" counts as pagination.
func TestCheckFile_IsTruncated(t *testing.T) {
	src := `package p
type S3Provider struct{}
func (p *S3Provider) ListBuckets() interface{} {
	var IsTruncated bool
	_ = IsTruncated
	return nil
}
`
	tmp := writeTempFile(t, "istruncated.go", src)

	fset := token.NewFileSet()
	violations, err := checkFile(fset, tmp)
	if err != nil {
		t.Fatalf("checkFile: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("expected no violations when IsTruncated is present, got: %v", violations)
	}
}

// writeTempFile creates a file named name inside a test temp dir and returns
// its absolute path.
func writeTempFile(t *testing.T, name, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("writeTempFile: %v", err)
	}
	return path
}
