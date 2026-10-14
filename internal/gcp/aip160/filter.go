// Package aip160 provides the shared tokenizer and clause sequencer for the
// AIP-160 list-filter grammars the emulator implements.
//
// Several Google list surfaces document their `filter` parameter as AIP-160
// (dataproc jobs.list and clusters.list, eventarc triggers.list, ...). They
// share the same lexical form — `field op value` clauses joined by the logical
// AND, written explicitly (" AND ") or implicitly (whitespace), with values a
// bare identifier, a double-quoted string, or the `*` wildcard — and differ
// only in which fields they accept and how a clause evaluates. This package
// owns the shared part: each surface supplies its own field set and predicates
// through Parser.ParseClauses, and rejects anything outside its grammar so a
// filter is never silently ignored (which would return an unfiltered list to a
// client that asked for a subset).
package aip160

import (
	"fmt"
	"strings"
)

// Kind classifies a filter token.
type Kind int

const (
	// KindEOF is the synthetic end-of-input token.
	KindEOF Kind = iota
	// KindIdent is a bare identifier (a field name or an unquoted value).
	KindIdent
	// KindString is a double-quoted string value.
	KindString
	// KindOp is a comparison operator (`=`, `!=`, `<`, `<=`, `>`, `>=`, `:`).
	KindOp
	// KindStar is the `*` wildcard.
	KindStar
)

// Token is one lexical unit of a filter expression.
type Token struct {
	Kind Kind
	Text string
}

// Tokenize splits a filter expression into identifiers, quoted strings,
// comparison operators and the `*` wildcard. Whitespace separates tokens; an
// unterminated string or an unexpected character is an error.
func Tokenize(s string) ([]Token, error) {
	var toks []Token
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '*':
			toks = append(toks, Token{Kind: KindStar, Text: "*"})
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
			toks = append(toks, Token{Kind: KindString, Text: b.String()})
			i = j
		case c == '=' || c == '!' || c == '<' || c == '>' || c == ':':
			if i+1 < len(s) {
				switch s[i : i+2] {
				case "!=", "<=", ">=":
					toks = append(toks, Token{Kind: KindOp, Text: s[i : i+2]})
					i += 2
					continue
				}
			}
			if c == '!' {
				return nil, fmt.Errorf("unexpected character %q", string(c))
			}
			toks = append(toks, Token{Kind: KindOp, Text: string(c)})
			i++
		case isIdentStart(c):
			j := i + 1
			for j < len(s) && isIdentPart(s[j]) {
				j++
			}
			toks = append(toks, Token{Kind: KindIdent, Text: s[i:j]})
			i = j
		default:
			return nil, fmt.Errorf("unexpected character %q", string(c))
		}
	}
	toks = append(toks, Token{Kind: KindEOF})
	return toks, nil
}

func isIdentStart(c byte) bool {
	return c == '_' ||
		(c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9')
}

func isIdentPart(c byte) bool {
	return isIdentStart(c) || c == '-' || c == '.'
}

// TokenValue returns an identifier or quoted-string token's text.
func TokenValue(t Token) (string, bool) {
	switch t.Kind {
	case KindIdent, KindString:
		return t.Text, true
	}
	return "", false
}

// TokenText renders a token for an error message.
func TokenText(t Token) string {
	if t.Kind == KindEOF {
		return "end of filter"
	}
	return fmt.Sprintf("%q", t.Text)
}

// Parser is the shared clause-sequencing parser over a token stream. It
// consumes a conjunction of clauses separated by an explicit "AND" keyword or
// by whitespace (implicit AND), delegating each clause to a surface-specific
// clause function that returns that surface's predicate type as an `any`. A
// trailing "AND" with no following clause is an error.
type Parser struct {
	toks []Token
	i    int
}

// NewParser returns a Parser positioned at the start of toks.
func NewParser(toks []Token) *Parser { return &Parser{toks: toks} }

// Peek returns the current token without consuming it.
func (p *Parser) Peek() Token { return p.toks[p.i] }

// Next consumes and returns the current token; at end of input it returns the
// EOF token without advancing.
func (p *Parser) Next() Token {
	t := p.toks[p.i]
	if t.Kind != KindEOF {
		p.i++
	}
	return t
}

// ParseClauses parses a conjunction of clauses, returning one predicate per
// clause in source order. An empty token stream (already consumed to EOF) is
// rejected by clause.
func (p *Parser) ParseClauses(clause func(*Parser) (any, error)) ([]any, error) {
	var preds []any
	for {
		pred, err := clause(p)
		if err != nil {
			return nil, err
		}
		preds = append(preds, pred)

		t := p.Peek()
		switch {
		case t.Kind == KindEOF:
			return preds, nil
		case t.Kind == KindIdent && strings.EqualFold(t.Text, "AND"):
			p.Next()
			if p.Peek().Kind == KindEOF {
				return nil, fmt.Errorf("dangling AND")
			}
			// Loop and parse the next clause.
		case t.Kind == KindIdent:
			// Implicit AND: the next token starts another field.
		default:
			return nil, fmt.Errorf("unexpected token %s", TokenText(t))
		}
	}
}
