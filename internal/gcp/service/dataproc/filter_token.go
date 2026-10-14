package dataproc

import (
	"fmt"
	"strings"
)

// Shared tokenizer for the Dataproc list-filter grammars. Both jobs.list and
// clusters.list use the same documented syntax — `field = value` clauses joined
// by the logical AND (explicit "AND" or implicit whitespace), values a bare
// identifier, a double-quoted string, or `*` — so the tokenizer lives here and
// each surface's parser (jobfilter.go, clusterfilter.go) supplies its own field
// set and predicates.

type filterTokenKind int

const (
	filterEOF filterTokenKind = iota
	filterIdent
	filterString
	filterOp
	filterStar
)

type filterToken struct {
	kind filterTokenKind
	text string
}

// tokenizeFilter splits an expression into identifiers, quoted strings,
// comparison operators and the `*` wildcard. Whitespace separates tokens; an
// unterminated string or an unexpected character is an error.
func tokenizeFilter(s string) ([]filterToken, error) {
	var toks []filterToken
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '*':
			toks = append(toks, filterToken{kind: filterStar, text: "*"})
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
			toks = append(toks, filterToken{kind: filterString, text: b.String()})
			i = j
		case c == '=' || c == '!' || c == '<' || c == '>':
			if i+1 < len(s) {
				switch s[i : i+2] {
				case "!=", "<=", ">=":
					toks = append(toks, filterToken{kind: filterOp, text: s[i : i+2]})
					i += 2
					continue
				}
			}
			if c == '!' {
				return nil, fmt.Errorf("unexpected character %q", string(c))
			}
			toks = append(toks, filterToken{kind: filterOp, text: string(c)})
			i++
		case isFilterIdentStart(c):
			j := i + 1
			for j < len(s) && isFilterIdentPart(s[j]) {
				j++
			}
			toks = append(toks, filterToken{kind: filterIdent, text: s[i:j]})
			i = j
		default:
			return nil, fmt.Errorf("unexpected character %q", string(c))
		}
	}
	toks = append(toks, filterToken{kind: filterEOF})
	return toks, nil
}

func isFilterIdentStart(c byte) bool {
	return c == '_' ||
		(c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9')
}

func isFilterIdentPart(c byte) bool {
	return isFilterIdentStart(c) || c == '-' || c == '.'
}

// filterTokenValue returns an identifier or quoted-string token's text.
func filterTokenValue(t filterToken) (string, bool) {
	switch t.kind {
	case filterIdent, filterString:
		return t.text, true
	}
	return "", false
}

// filterTokenText renders a token for an error message.
func filterTokenText(t filterToken) string {
	if t.kind == filterEOF {
		return "end of filter"
	}
	return fmt.Sprintf("%q", t.text)
}

// filterParser is the shared clause-sequencing parser: it consumes a
// conjunction of clauses separated by an explicit "AND" keyword or by
// whitespace (implicit AND), delegating each clause to a surface-specific
// clause function that returns that surface's predicate type as an `any`. A
// trailing "AND" with no following clause is an error.
type filterParser struct {
	toks []filterToken
	i    int
}

func (p *filterParser) peek() filterToken { return p.toks[p.i] }

func (p *filterParser) next() filterToken {
	t := p.toks[p.i]
	if t.kind != filterEOF {
		p.i++
	}
	return t
}

// parseClauses parses a conjunction of clauses, returning one predicate per
// clause in source order. An empty token stream (already consumed to EOF) is
// rejected by clause.
func (p *filterParser) parseClauses(clause func(*filterParser) (any, error)) ([]any, error) {
	var preds []any
	for {
		pred, err := clause(p)
		if err != nil {
			return nil, err
		}
		preds = append(preds, pred)

		t := p.peek()
		switch {
		case t.kind == filterEOF:
			return preds, nil
		case t.kind == filterIdent && strings.EqualFold(t.text, "AND"):
			p.next()
			if p.peek().kind == filterEOF {
				return nil, fmt.Errorf("dangling AND")
			}
			// Loop and parse the next clause.
		case t.kind == filterIdent:
			// Implicit AND: the next token starts another field.
		default:
			return nil, fmt.Errorf("unexpected token %s", filterTokenText(t))
		}
	}
}
