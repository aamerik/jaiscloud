// Package bad is golden-file testdata for the paginationcheck linter.
// It intentionally contains List* and Describe* methods on *Provider receivers
// that do NOT use any pagination keywords — the linter must flag them.
package bad

import "fmt"

// FooProvider is a dummy provider used for linter testing.
type FooProvider struct{}

// ListFoo returns a list of foos without any pagination support.
// The linter should flag this method.
func (p *FooProvider) ListFoo() []string {
	return []string{"a", "b", "c"}
}

// DescribeBar returns a bar description without any pagination support.
// The linter should flag this method.
func (p *FooProvider) DescribeBar(name string) string {
	return fmt.Sprintf("bar-%s", name)
}

// CreateFoo is not a List/Describe method — the linter must NOT flag it.
func (p *FooProvider) CreateFoo() error {
	return nil
}

// listPrivate is unexported — the linter ignores case, so this WILL be flagged
// if it starts with "List" regardless of export status. However the name starts
// with lowercase "l" so it does NOT match the "List" prefix check.
func (p *FooProvider) listPrivate() {}
