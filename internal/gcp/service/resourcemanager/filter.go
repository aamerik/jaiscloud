package resourcemanager

import (
	"fmt"
	"strings"
)

// projectFilter is a compiled v1 ListProjects filter expression
// (cloudresourcemanager.projects.list.filter): a disjunction of field:value
// clauses. The Discovery document states that "if multiple fields are included
// in a filter query, the query will return results that match any of the
// fields", so any matching clause admits the project. An empty filter matches
// every project.
type projectFilter struct {
	clauses []projectClause
}

// match reports whether p satisfies the filter.
func (f projectFilter) match(p Project) bool {
	if len(f.clauses) == 0 {
		return true
	}
	for _, c := range f.clauses {
		if c.match(p) {
			return true
		}
	}
	return false
}

// projectClause is one compiled filter clause.
type projectClause interface {
	match(Project) bool
}

// projectAnyClause matches every project (a bare `*` value).
type projectAnyClause struct{}

func (projectAnyClause) match(Project) bool { return true }

// projectValueClause matches a string-valued project attribute. Matching is
// case-insensitive; prefix selects a prefix match (the trailing-`*` form)
// rather than an exact match.
type projectValueClause struct {
	get    func(Project) string
	value  string
	prefix bool
}

func (c projectValueClause) match(p Project) bool {
	got := c.get(p)
	if c.prefix {
		return strings.HasPrefix(strings.ToLower(got), c.value)
	}
	return strings.EqualFold(got, c.value)
}

// projectLabelExistsClause matches projects carrying a label key (the
// `labels.<key>:*` form).
type projectLabelExistsClause struct {
	key string
}

func (c projectLabelExistsClause) match(p Project) bool {
	_, ok := p.Labels[c.key]
	return ok
}

// compileProjectFilter parses the bounded v1 project-list filter grammar:
//
//	clause := field ":" value
//	filter := clause ( <whitespace> clause )*
//	field  := "name" | "id" | "labels." <key> | "lifecycleState"
//	value  := <token> | <double-quoted string>
//
// Field names and values are matched case-insensitively, clauses are OR-ed, a
// trailing `*` in a value is a prefix match, and a value of exactly `*` matches
// any value (for `labels.<key>` it means the key is present). Anything outside
// the grammar is InvalidArgument rather than a silently wrong (unfiltered)
// page. `parent.type`/`parent.id` are rejected: they need the org/folder
// ancestry index the emulator does not model.
func compileProjectFilter(filter string) (projectFilter, error) {
	if strings.TrimSpace(filter) == "" {
		return projectFilter{}, nil
	}
	toks, err := tokenizeProjectFilter(filter)
	if err != nil {
		return projectFilter{}, invalidArgument("invalid project list filter: " + err.Error())
	}
	p := &projectFilterParser{toks: toks}
	f, err := p.parse()
	if err != nil {
		return projectFilter{}, invalidArgument("invalid project list filter: " + err.Error())
	}
	return f, nil
}

// ─── tokenizer ────────────────────────────────────────────────────────────────

type projectFilterTokenKind int

const (
	pjEOF projectFilterTokenKind = iota
	pjIdent
	pjString
	pjColon
)

type projectFilterToken struct {
	kind projectFilterTokenKind
	text string
}

// tokenizeProjectFilter splits an expression into identifiers, quoted strings
// and the `:` separator. `*` is an ordinary identifier character so wildcard
// values (`how*`, `*`) stay one token; whitespace separates clauses, and an
// unterminated string or unexpected character is an error.
func tokenizeProjectFilter(s string) ([]projectFilterToken, error) {
	var toks []projectFilterToken
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == ':':
			toks = append(toks, projectFilterToken{kind: pjColon, text: ":"})
			i++
		case c == '"':
			j := i + 1
			var b strings.Builder
			closed := false
			for j < len(s) {
				if s[j] == '\\' && j+1 < len(s) {
					b.WriteByte(s[j+1])
					j += 2
					continue
				}
				if s[j] == '"' {
					closed = true
					j++
					break
				}
				b.WriteByte(s[j])
				j++
			}
			if !closed {
				return nil, fmt.Errorf("unterminated quoted string")
			}
			toks = append(toks, projectFilterToken{kind: pjString, text: b.String()})
			i = j
		case isProjectFilterIdentPart(c):
			j := i + 1
			for j < len(s) && isProjectFilterIdentPart(s[j]) {
				j++
			}
			toks = append(toks, projectFilterToken{kind: pjIdent, text: s[i:j]})
			i = j
		default:
			return nil, fmt.Errorf("unexpected character %q", string(c))
		}
	}
	toks = append(toks, projectFilterToken{kind: pjEOF})
	return toks, nil
}

func isProjectFilterIdentPart(c byte) bool {
	return c == '_' || c == '-' || c == '.' || c == '*' ||
		(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// ─── parser ───────────────────────────────────────────────────────────────────

type projectFilterParser struct {
	toks []projectFilterToken
	i    int
}

func (p *projectFilterParser) peek() projectFilterToken { return p.toks[p.i] }

func (p *projectFilterParser) next() projectFilterToken {
	t := p.toks[p.i]
	if t.kind != pjEOF {
		p.i++
	}
	return t
}

// parse consumes `field:value` clauses until end of input.
func (p *projectFilterParser) parse() (projectFilter, error) {
	var f projectFilter
	for {
		field := p.next()
		if field.kind != pjIdent {
			return projectFilter{}, fmt.Errorf("expected filter field, got %s", projectTokenText(field))
		}
		op := p.next()
		if op.kind != pjColon {
			return projectFilter{}, fmt.Errorf("expected \":\" after %q, got %s", field.text, projectTokenText(op))
		}
		value := p.next()
		if value.kind != pjIdent && value.kind != pjString {
			return projectFilter{}, fmt.Errorf("expected value after %q, got %s", field.text, projectTokenText(value))
		}
		clause, err := newProjectClause(field.text, value.text)
		if err != nil {
			return projectFilter{}, err
		}
		f.clauses = append(f.clauses, clause)

		switch p.peek().kind {
		case pjEOF:
			return f, nil
		case pjIdent:
			// Next clause.
		default:
			return projectFilter{}, fmt.Errorf("unexpected token %s", projectTokenText(p.peek()))
		}
	}
}

// newProjectClause builds the predicate for a single field:value pair.
func newProjectClause(field, value string) (projectClause, error) {
	if value == "" {
		return nil, fmt.Errorf("empty value for %q", field)
	}
	switch strings.ToLower(field) {
	case "name":
		return newProjectValueClause(value, func(p Project) string { return p.DisplayName }), nil
	case "id":
		return newProjectValueClause(value, func(p Project) string { return p.ProjectID }), nil
	case "lifecyclestate":
		return newProjectValueClause(value, func(p Project) string { return p.State }), nil
	}
	// The "labels." prefix is matched case-insensitively (filter rules are), but
	// the label key that follows is data and keeps its case.
	const labelPrefix = "labels."
	if len(field) > len(labelPrefix) && strings.EqualFold(field[:len(labelPrefix)], labelPrefix) {
		key := field[len(labelPrefix):]
		if value == "*" {
			return projectLabelExistsClause{key: key}, nil
		}
		return newProjectValueClause(value, func(p Project) string { return p.Labels[key] }), nil
	}
	return nil, fmt.Errorf("unsupported filter field %q (only name, id, labels.<key> and lifecycleState are supported)", field)
}

// newProjectValueClause builds a value clause, applying the shared wildcard
// rules: a bare `*` matches any value, a trailing `*` is a case-insensitive
// prefix match, and anything else is a case-insensitive exact match.
func newProjectValueClause(value string, get func(Project) string) projectClause {
	if value == "*" {
		return projectAnyClause{}
	}
	if prefix, ok := strings.CutSuffix(value, "*"); ok {
		return projectValueClause{get: get, value: strings.ToLower(prefix), prefix: true}
	}
	return projectValueClause{get: get, value: value}
}

// projectTokenText renders a token for an error message.
func projectTokenText(t projectFilterToken) string {
	if t.kind == pjEOF {
		return "end of filter"
	}
	return fmt.Sprintf("%q", t.text)
}
