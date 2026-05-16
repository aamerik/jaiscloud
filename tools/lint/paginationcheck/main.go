//go:build ignore

// paginationcheck is a heuristic linter that checks whether List*/Describe*
// provider methods use pagination (contain Paginate, Marker, or NextToken).
//
// Usage:
//
//	go run tools/lint/paginationcheck/main.go ./internal/aws/provider/...
//
// This is a simple string-pattern check, not a full AST-based analysis.
// False positives/negatives are expected; it serves as a development gate.
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

func main() {
	patterns := os.Args[1:]
	if len(patterns) == 0 {
		patterns = []string{"./internal/aws/provider/..."}
	}

	dirs, err := expandGlobs(patterns)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error expanding patterns: %v\n", err)
		os.Exit(1)
	}

	fset := token.NewFileSet()
	violations := 0

	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			path := filepath.Join(dir, entry.Name())
			f, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				fmt.Fprintf(os.Stderr, "parse %s: %v\n", path, err)
				continue
			}
			violations += checkFile(fset, f)
		}
	}

	if violations > 0 {
		fmt.Fprintf(os.Stderr, "\npaginationcheck: %d violation(s) found\n", violations)
		os.Exit(1)
	}
	fmt.Println("paginationcheck: OK — no pagination violations found")
}

func checkFile(fset *token.FileSet, f *ast.File) int {
	count := 0
	ast.Inspect(f, func(n ast.Node) bool {
		fn, ok := n.(*ast.FuncDecl)
		if !ok {
			return true
		}
		// Must be on a receiver (method), not a standalone function.
		if fn.Recv == nil || len(fn.Recv.List) == 0 {
			return true
		}
		// Receiver type must end in "Provider".
		recvTypeName := receiverTypeName(fn.Recv)
		if !strings.HasSuffix(recvTypeName, "Provider") {
			return true
		}
		// Only check List* and Describe* methods.
		name := fn.Name.Name
		if !strings.HasPrefix(name, "List") && !strings.HasPrefix(name, "Describe") {
			return true
		}
		// Skip stub/no-op bodies (body is nil or very short).
		if fn.Body == nil || len(fn.Body.List) == 0 {
			return true
		}

		// Heuristic: stringify the AST node and look for pagination patterns.
		body := nodeToString(fn.Body)
		hasPagination := strings.Contains(body, "Paginate") ||
			strings.Contains(body, "Marker") ||
			strings.Contains(body, "NextToken") ||
			strings.Contains(body, "nextToken") ||
			strings.Contains(body, "PageToken") ||
			strings.Contains(body, "pageToken") ||
			strings.Contains(body, "cursor") ||
			strings.Contains(body, "Cursor")

		if !hasPagination {
			pos := fset.Position(fn.Pos())
			fmt.Printf("%s: provider method %s.%s may be missing pagination (no Paginate/Marker/NextToken found)\n",
				pos, recvTypeName, name)
			count++
		}
		return true
	})
	return count
}

// receiverTypeName returns the base type name of the method receiver
// (e.g. "*FooProvider" → "FooProvider").
func receiverTypeName(recv *ast.FieldList) string {
	if recv == nil || len(recv.List) == 0 {
		return ""
	}
	field := recv.List[0]
	switch t := field.Type.(type) {
	case *ast.StarExpr:
		if ident, ok := t.X.(*ast.Ident); ok {
			return ident.Name
		}
	case *ast.Ident:
		return t.Name
	}
	return ""
}

// nodeToString produces a rough string representation of an AST node
// suitable for pattern matching.
func nodeToString(node ast.Node) string {
	var sb strings.Builder
	ast.Inspect(node, func(n ast.Node) bool {
		if ident, ok := n.(*ast.Ident); ok {
			sb.WriteString(ident.Name)
			sb.WriteString(" ")
		}
		return true
	})
	return sb.String()
}

// expandGlobs resolves directory patterns, supporting the "..." suffix.
func expandGlobs(patterns []string) ([]string, error) {
	var dirs []string
	for _, p := range patterns {
		recursive := strings.HasSuffix(p, "/...")
		base := strings.TrimSuffix(p, "/...")
		base = strings.TrimSuffix(base, "...")

		if recursive {
			err := filepath.WalkDir(base, func(path string, d os.DirEntry, err error) error {
				if err != nil {
					return nil
				}
				if d.IsDir() {
					dirs = append(dirs, path)
				}
				return nil
			})
			if err != nil {
				return nil, fmt.Errorf("walk %s: %w", base, err)
			}
		} else {
			dirs = append(dirs, base)
		}
	}
	return dirs, nil
}
