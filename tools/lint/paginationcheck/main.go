// paginationcheck is a static-analysis CLI that inspects all List*/Describe*
// methods on *Provider receivers under a given directory tree and reports any
// that appear to lack pagination support.
//
// Usage:
//
//	go run tools/lint/paginationcheck/main.go [dir]          # dir defaults to internal/aws/provider
//	go run tools/lint/paginationcheck/main.go ./internal/aws/provider/...
//
// Exit code 1 when violations are found, 0 otherwise.
package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

// paginationKeywords are substrings whose presence in a method body's raw
// source text indicates the implementation handles pagination in some form.
var paginationKeywords = []string{
	"Paginate",
	"Marker",
	"NextToken",
	"IsTruncated",
	"pagination.",
}

func main() {
	dir := "internal/aws/provider"
	if len(os.Args) > 1 {
		dir = os.Args[1]
	}
	// Strip trailing /... used by Make targets — we always walk recursively.
	dir = strings.TrimSuffix(dir, "/...")
	dir = strings.TrimSuffix(dir, "...")

	violations, err := check(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "paginationcheck: %v\n", err)
		os.Exit(2)
	}

	for _, v := range violations {
		fmt.Println(v)
	}

	if len(violations) > 0 {
		os.Exit(1)
	}
}

// check walks root recursively, parses every non-test .go file, and returns
// one diagnostic string per method that appears to be missing pagination.
func check(root string) ([]string, error) {
	fset := token.NewFileSet()

	var goFiles []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		if strings.HasSuffix(path, "_test.go") {
			return nil
		}
		goFiles = append(goFiles, path)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walking %q: %w", root, err)
	}

	var violations []string
	for _, path := range goFiles {
		vs, err := checkFile(fset, path)
		if err != nil {
			// Non-fatal: report and continue.
			fmt.Fprintf(os.Stderr, "paginationcheck: parse error in %s: %v\n", path, err)
			continue
		}
		violations = append(violations, vs...)
	}
	return violations, nil
}

// checkFile parses a single Go source file and returns one diagnostic per
// List*/Describe* method on a *Provider receiver that lacks pagination keywords.
// The raw source bytes are used for keyword matching so that indirect helpers
// (e.g. a field named "cursor") are also detected.
func checkFile(fset *token.FileSet, path string) ([]string, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	f, err := parser.ParseFile(fset, path, src, 0)
	if err != nil {
		return nil, err
	}

	var violations []string

	ast.Inspect(f, func(n ast.Node) bool {
		fd, ok := n.(*ast.FuncDecl)
		if !ok {
			return true
		}

		// Must be a method (has a receiver).
		if fd.Recv == nil || len(fd.Recv.List) == 0 {
			return true
		}

		// Method name must start with List or Describe.
		if !strings.HasPrefix(fd.Name.Name, "List") && !strings.HasPrefix(fd.Name.Name, "Describe") {
			return true
		}

		// Receiver type must end in "Provider" (pointer or value).
		recvType := extractTypeName(fd.Recv.List[0].Type)
		if !strings.HasSuffix(recvType, "Provider") {
			return true
		}

		// Body must exist and be non-trivial.
		if fd.Body == nil || len(fd.Body.List) == 0 {
			return true
		}

		// Slice the raw source to get the body text.
		bodyStart := fset.Position(fd.Body.Pos()).Offset
		bodyEnd := fset.Position(fd.Body.End()).Offset
		if bodyEnd > len(src) {
			bodyEnd = len(src)
		}
		bodyText := string(src[bodyStart:bodyEnd])

		for _, kw := range paginationKeywords {
			if strings.Contains(bodyText, kw) {
				return true // pagination found — not a violation
			}
		}

		pos := fset.Position(fd.Pos())
		violations = append(violations, fmt.Sprintf(
			"%s:%d: %s may be missing pagination",
			pos.Filename, pos.Line, fd.Name.Name,
		))

		return true
	})

	return violations, nil
}

// extractTypeName returns the base identifier from a receiver type expression.
// Handles *T, T, and generic T[P] (returns "").
func extractTypeName(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.StarExpr:
		return extractTypeName(t.X)
	case *ast.Ident:
		return t.Name
	case *ast.IndexExpr:
		return extractTypeName(t.X)
	default:
		return ""
	}
}
