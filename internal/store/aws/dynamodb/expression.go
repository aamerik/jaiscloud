package dynamodb

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// ExpressionError wraps a DynamoDB expression validation failure.
// The store package cannot import model, so callers use errors.As to detect this type.
type ExpressionError struct{ Message string }

func (e *ExpressionError) Error() string { return "ValidationException: " + e.Message }

// EvalFilter evaluates a DynamoDB filter, condition, or key-condition expression.
// Returns (true, nil) for a match, (false, nil) for no match,
// (*ExpressionError, ...) for a parse/validation failure.
func EvalFilter(item map[string]any, expr string, names map[string]string, values map[string]any) (bool, error) {
	if expr == "" {
		return true, nil
	}
	ast, err := parseFilterExpr(expr)
	if err != nil {
		return false, &ExpressionError{Message: err.Error()}
	}
	return ast.eval(item, names, values)
}

// ─── Token types ─────────────────────────────────────────────────────────────

type tokenKind int

const (
	tkEOF tokenKind = iota
	tkIdent     // keyword, function name, or attribute name
	tkExprName  // #name
	tkExprVal   // :name
	tkNumber    // digits (list indices)
	tkDot       // .
	tkLBracket  // [
	tkRBracket  // ]
	tkLParen    // (
	tkRParen    // )
	tkComma     // ,
	tkEq        // =
	tkNeq       // <>
	tkLt        // <
	tkLe        // <=
	tkGt        // >
	tkGe        // >=
	tkPlus      // +
	tkMinus     // -
)

type token struct {
	kind tokenKind
	val  string
}

func tokenize(s string) ([]token, error) {
	var toks []token
	i := 0
	for i < len(s) {
		c := s[i]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			i++
			continue
		}
		switch c {
		case '.':
			toks = append(toks, token{tkDot, "."})
			i++
		case '[':
			toks = append(toks, token{tkLBracket, "["})
			i++
		case ']':
			toks = append(toks, token{tkRBracket, "]"})
			i++
		case '(':
			toks = append(toks, token{tkLParen, "("})
			i++
		case ')':
			toks = append(toks, token{tkRParen, ")"})
			i++
		case ',':
			toks = append(toks, token{tkComma, ","})
			i++
		case '+':
			toks = append(toks, token{tkPlus, "+"})
			i++
		case '-':
			toks = append(toks, token{tkMinus, "-"})
			i++
		case '=':
			toks = append(toks, token{tkEq, "="})
			i++
		case '<':
			if i+1 < len(s) && s[i+1] == '>' {
				toks = append(toks, token{tkNeq, "<>"})
				i += 2
			} else if i+1 < len(s) && s[i+1] == '=' {
				toks = append(toks, token{tkLe, "<="})
				i += 2
			} else {
				toks = append(toks, token{tkLt, "<"})
				i++
			}
		case '>':
			if i+1 < len(s) && s[i+1] == '=' {
				toks = append(toks, token{tkGe, ">="})
				i += 2
			} else {
				toks = append(toks, token{tkGt, ">"})
				i++
			}
		case '#':
			j := i + 1
			for j < len(s) && (isAlphaNum(s[j]) || s[j] == '_') {
				j++
			}
			toks = append(toks, token{tkExprName, s[i:j]})
			i = j
		case ':':
			j := i + 1
			for j < len(s) && (isAlphaNum(s[j]) || s[j] == '_') {
				j++
			}
			toks = append(toks, token{tkExprVal, s[i:j]})
			i = j
		default:
			if isDigit(c) {
				j := i
				for j < len(s) && isDigit(s[j]) {
					j++
				}
				toks = append(toks, token{tkNumber, s[i:j]})
				i = j
			} else if isAlpha(c) || c == '_' {
				j := i
				for j < len(s) && (isAlphaNum(s[j]) || s[j] == '_') {
					j++
				}
				toks = append(toks, token{tkIdent, s[i:j]})
				i = j
			} else {
				return nil, fmt.Errorf("unexpected character '%c' in expression", c)
			}
		}
	}
	toks = append(toks, token{tkEOF, ""})
	return toks, nil
}

func isAlpha(c byte) bool  { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') }
func isDigit(c byte) bool  { return c >= '0' && c <= '9' }
func isAlphaNum(c byte) bool { return isAlpha(c) || isDigit(c) }

// ─── AST node types ───────────────────────────────────────────────────────────

type exprNode interface {
	eval(item map[string]any, names map[string]string, values map[string]any) (bool, error)
}

type operandNode interface {
	resolve(item map[string]any, names map[string]string, values map[string]any) (any, bool, error)
	// bool return: true if the path/value was found/present
}

// orNode short-circuits on true.
type orNode struct{ left, right exprNode }

func (n *orNode) eval(item map[string]any, names map[string]string, values map[string]any) (bool, error) {
	l, err := n.left.eval(item, names, values)
	if err != nil {
		return false, err
	}
	if l {
		return true, nil
	}
	return n.right.eval(item, names, values)
}

// andNode short-circuits on false.
type andNode struct{ left, right exprNode }

func (n *andNode) eval(item map[string]any, names map[string]string, values map[string]any) (bool, error) {
	l, err := n.left.eval(item, names, values)
	if err != nil {
		return false, err
	}
	if !l {
		return false, nil
	}
	return n.right.eval(item, names, values)
}

type notNode struct{ inner exprNode }

func (n *notNode) eval(item map[string]any, names map[string]string, values map[string]any) (bool, error) {
	v, err := n.inner.eval(item, names, values)
	if err != nil {
		return false, err
	}
	return !v, nil
}

type compareNode struct {
	left, right operandNode
	op          string
}

func (n *compareNode) eval(item map[string]any, names map[string]string, values map[string]any) (bool, error) {
	l, _, err := n.left.resolve(item, names, values)
	if err != nil {
		return false, err
	}
	r, _, err := n.right.resolve(item, names, values)
	if err != nil {
		return false, err
	}
	return compareValues(l, r, n.op), nil
}

type betweenNode struct{ target, lo, hi operandNode }

func (n *betweenNode) eval(item map[string]any, names map[string]string, values map[string]any) (bool, error) {
	t, _, err := n.target.resolve(item, names, values)
	if err != nil {
		return false, err
	}
	l, _, err := n.lo.resolve(item, names, values)
	if err != nil {
		return false, err
	}
	h, _, err := n.hi.resolve(item, names, values)
	if err != nil {
		return false, err
	}
	return compareValues(t, l, ">=") && compareValues(t, h, "<="), nil
}

type inNode struct {
	target operandNode
	list   []operandNode
}

func (n *inNode) eval(item map[string]any, names map[string]string, values map[string]any) (bool, error) {
	t, _, err := n.target.resolve(item, names, values)
	if err != nil {
		return false, err
	}
	for _, elem := range n.list {
		v, _, err := elem.resolve(item, names, values)
		if err != nil {
			return false, err
		}
		if compareValues(t, v, "=") {
			return true, nil
		}
	}
	return false, nil
}

type funcNode struct {
	name string
	args []operandNode
}

func (n *funcNode) eval(item map[string]any, names map[string]string, values map[string]any) (bool, error) {
	lower := strings.ToLower(n.name)
	switch lower {
	case "attribute_exists":
		if len(n.args) != 1 {
			return false, &ExpressionError{Message: "attribute_exists requires 1 argument"}
		}
		pn, ok := n.args[0].(*pathOperandNode)
		if !ok {
			return false, &ExpressionError{Message: "attribute_exists requires a path argument"}
		}
		resolved := ResolveParts(pn.parts, names)
		_, exists := ResolvePath(item, resolved)
		return exists, nil

	case "attribute_not_exists":
		if len(n.args) != 1 {
			return false, &ExpressionError{Message: "attribute_not_exists requires 1 argument"}
		}
		pn, ok := n.args[0].(*pathOperandNode)
		if !ok {
			return false, &ExpressionError{Message: "attribute_not_exists requires a path argument"}
		}
		resolved := ResolveParts(pn.parts, names)
		_, exists := ResolvePath(item, resolved)
		return !exists, nil

	case "attribute_type":
		if len(n.args) != 2 {
			return false, &ExpressionError{Message: "attribute_type requires 2 arguments"}
		}
		v, _, err := n.args[0].resolve(item, names, values)
		if err != nil {
			return false, err
		}
		typeRef, _, err := n.args[1].resolve(item, names, values)
		if err != nil {
			return false, err
		}
		expected, _ := AttrVal(typeRef)
		if expected == "" {
			if s, ok := typeRef.(string); ok {
				expected = s
			}
		}
		actual := AttrType(v)
		return actual == expected, nil

	case "begins_with":
		if len(n.args) != 2 {
			return false, &ExpressionError{Message: "begins_with requires 2 arguments"}
		}
		v, _, err := n.args[0].resolve(item, names, values)
		if err != nil {
			return false, err
		}
		prefix, _, err := n.args[1].resolve(item, names, values)
		if err != nil {
			return false, err
		}
		vs, _ := AttrVal(v)
		ps, _ := AttrVal(prefix)
		return strings.HasPrefix(vs, ps), nil

	case "contains":
		if len(n.args) != 2 {
			return false, &ExpressionError{Message: "contains requires 2 arguments"}
		}
		v, _, err := n.args[0].resolve(item, names, values)
		if err != nil {
			return false, err
		}
		sub, _, err := n.args[1].resolve(item, names, values)
		if err != nil {
			return false, err
		}
		return dynContains(v, sub), nil
	}
	return false, &ExpressionError{Message: "unknown function: " + n.name}
}

// ─── Operand nodes ────────────────────────────────────────────────────────────

type pathOperandNode struct{ parts []PathPart }

func (n *pathOperandNode) resolve(item map[string]any, names map[string]string, values map[string]any) (any, bool, error) {
	resolved := ResolveParts(n.parts, names)
	v, ok := ResolvePath(item, resolved)
	return v, ok, nil
}

type valOperandNode struct{ ref string }

func (n *valOperandNode) resolve(item map[string]any, names map[string]string, values map[string]any) (any, bool, error) {
	v, ok := values[n.ref]
	if !ok {
		return nil, false, &ExpressionError{Message: "value " + n.ref + " not found in ExpressionAttributeValues"}
	}
	return v, true, nil
}

type sizeOperandNode struct{ inner operandNode }

func (n *sizeOperandNode) resolve(item map[string]any, names map[string]string, values map[string]any) (any, bool, error) {
	v, ok, err := n.inner.resolve(item, names, values)
	if err != nil {
		return nil, false, err
	}
	if !ok || v == nil {
		return nil, false, nil
	}
	sz := dynSize(v)
	return map[string]any{"N": fmt.Sprintf("%d", sz)}, true, nil
}

// ─── Helper functions ─────────────────────────────────────────────────────────

func compareValues(a, b any, op string) bool {
	if a == nil || b == nil {
		switch op {
		case "=":
			return a == b
		case "<>":
			return a != b
		}
		return false
	}
	an, aIsN := ParseNumeric(a)
	bn, bIsN := ParseNumeric(b)
	if aIsN && bIsN {
		switch op {
		case "=":
			return an == bn
		case "<>":
			return an != bn
		case "<":
			return an < bn
		case "<=":
			return an <= bn
		case ">":
			return an > bn
		case ">=":
			return an >= bn
		}
	}
	// Handle BOOL type explicitly — AttrVal returns "" for both true and false
	// which would incorrectly make all BOOL equality checks return true.
	if am, ok := a.(map[string]any); ok {
		if boolA, okA := am["BOOL"].(bool); okA {
			if bm, ok := b.(map[string]any); ok {
				if boolB, okB := bm["BOOL"].(bool); okB {
					switch op {
					case "=":
						return boolA == boolB
					case "<>":
						return boolA != boolB
					}
					return false
				}
			}
		}
	}
	as, _ := AttrVal(a)
	bs, _ := AttrVal(b)
	switch op {
	case "=":
		return as == bs
	case "<>":
		return as != bs
	case "<":
		return as < bs
	case "<=":
		return as <= bs
	case ">":
		return as > bs
	case ">=":
		return as >= bs
	}
	return false
}

func dynContains(container, sub any) bool {
	cm, ok := container.(map[string]any)
	if !ok {
		return false
	}
	sm, ok := sub.(map[string]any)
	if !ok {
		return false
	}
	if cs, ok := cm["S"].(string); ok {
		if ss, ok := sm["S"].(string); ok {
			return strings.Contains(cs, ss)
		}
		return false
	}
	if list, ok := cm["L"].([]any); ok {
		for _, elem := range list {
			if fmt.Sprintf("%v", elem) == fmt.Sprintf("%v", sm) {
				return true
			}
		}
		return false
	}
	for _, setKey := range []string{"SS", "NS", "BS"} {
		if set, ok := cm[setKey].([]any); ok {
			singleKey := setKey[:1]
			sv, ok := sm[singleKey].(string)
			if !ok {
				continue
			}
			for _, elem := range set {
				if fmt.Sprintf("%v", elem) == sv {
					return true
				}
			}
			return false
		}
	}
	return false
}

func dynSize(v any) int {
	m, ok := v.(map[string]any)
	if !ok {
		return 0
	}
	if s, ok := m["S"].(string); ok {
		return len(s)
	}
	if s, ok := m["N"].(string); ok {
		return len(s)
	}
	if s, ok := m["B"].(string); ok {
		return len(s)
	}
	if list, ok := m["L"].([]any); ok {
		return len(list)
	}
	if mp, ok := m["M"].(map[string]any); ok {
		return len(mp)
	}
	for _, setKey := range []string{"SS", "NS", "BS"} {
		if set, ok := m[setKey].([]any); ok {
			return len(set)
		}
	}
	return 0
}

// ─── Parser ───────────────────────────────────────────────────────────────────

type filterParser struct {
	tokens []token
	pos    int
}

func parseFilterExpr(s string) (exprNode, error) {
	toks, err := tokenize(s)
	if err != nil {
		return nil, err
	}
	p := &filterParser{tokens: toks}
	node, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.peek().kind != tkEOF {
		return nil, fmt.Errorf("unexpected token %q after expression", p.peek().val)
	}
	return node, nil
}

func (p *filterParser) peek() token {
	if p.pos < len(p.tokens) {
		return p.tokens[p.pos]
	}
	return token{tkEOF, ""}
}

func (p *filterParser) consume() token {
	t := p.peek()
	p.pos++
	return t
}

func (p *filterParser) expectKwd(kwd string) error {
	t := p.consume()
	if t.kind != tkIdent || !strings.EqualFold(t.val, kwd) {
		return fmt.Errorf("expected keyword %q, got %q", kwd, t.val)
	}
	return nil
}

func (p *filterParser) parseOr() (exprNode, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.peek().kind == tkIdent && strings.EqualFold(p.peek().val, "OR") {
		p.consume()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &orNode{left, right}
	}
	return left, nil
}

func (p *filterParser) parseAnd() (exprNode, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.peek().kind == tkIdent && strings.EqualFold(p.peek().val, "AND") {
		p.consume()
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = &andNode{left, right}
	}
	return left, nil
}

func (p *filterParser) parseNot() (exprNode, error) {
	if p.peek().kind == tkIdent && strings.EqualFold(p.peek().val, "NOT") {
		p.consume()
		inner, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &notNode{inner}, nil
	}
	return p.parseComparison()
}

func (p *filterParser) parseComparison() (exprNode, error) {
	// Parenthesized sub-expression.
	if p.peek().kind == tkLParen {
		p.consume()
		inner, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if p.peek().kind != tkRParen {
			return nil, fmt.Errorf("expected ')'")
		}
		p.consume()
		return inner, nil
	}

	// Boolean function calls.
	if p.peek().kind == tkIdent {
		lower := strings.ToLower(p.peek().val)
		switch lower {
		case "attribute_exists", "attribute_not_exists", "attribute_type", "begins_with", "contains":
			name := p.consume().val
			if p.peek().kind != tkLParen {
				return nil, fmt.Errorf("expected '(' after %s", name)
			}
			p.consume()
			args, err := p.parseOperandList()
			if err != nil {
				return nil, err
			}
			if p.peek().kind != tkRParen {
				return nil, fmt.Errorf("expected ')' closing %s arguments", name)
			}
			p.consume()
			return &funcNode{name: name, args: args}, nil
		}
	}

	// Comparison: operand op operand
	left, err := p.parseOperand()
	if err != nil {
		return nil, err
	}

	t := p.peek()
	switch t.kind {
	case tkEq:
		p.consume()
		right, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		return &compareNode{left, right, "="}, nil
	case tkNeq:
		p.consume()
		right, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		return &compareNode{left, right, "<>"}, nil
	case tkLt:
		p.consume()
		right, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		return &compareNode{left, right, "<"}, nil
	case tkLe:
		p.consume()
		right, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		return &compareNode{left, right, "<="}, nil
	case tkGt:
		p.consume()
		right, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		return &compareNode{left, right, ">"}, nil
	case tkGe:
		p.consume()
		right, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		return &compareNode{left, right, ">="}, nil
	case tkIdent:
		upper := strings.ToUpper(t.val)
		if upper == "BETWEEN" {
			p.consume()
			lo, err := p.parseOperand()
			if err != nil {
				return nil, err
			}
			if err := p.expectKwd("AND"); err != nil {
				return nil, err
			}
			hi, err := p.parseOperand()
			if err != nil {
				return nil, err
			}
			return &betweenNode{left, lo, hi}, nil
		}
		if upper == "IN" {
			p.consume()
			if p.peek().kind != tkLParen {
				return nil, fmt.Errorf("expected '(' after IN")
			}
			p.consume()
			list, err := p.parseOperandList()
			if err != nil {
				return nil, err
			}
			if p.peek().kind != tkRParen {
				return nil, fmt.Errorf("expected ')' closing IN list")
			}
			p.consume()
			return &inNode{left, list}, nil
		}
	}

	return nil, fmt.Errorf("expected comparison operator, got %q", t.val)
}

func (p *filterParser) parseOperand() (operandNode, error) {
	t := p.peek()

	// size() function → numeric operand
	if t.kind == tkIdent && strings.EqualFold(t.val, "size") {
		p.consume()
		if p.peek().kind != tkLParen {
			return nil, fmt.Errorf("expected '(' after size")
		}
		p.consume()
		inner, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		if p.peek().kind != tkRParen {
			return nil, fmt.Errorf("expected ')' closing size()")
		}
		p.consume()
		return &sizeOperandNode{inner}, nil
	}

	// Expression value reference
	if t.kind == tkExprVal {
		p.consume()
		return &valOperandNode{t.val}, nil
	}

	// Path (starts with identifier or expression name)
	if t.kind == tkIdent || t.kind == tkExprName {
		return p.parsePath()
	}

	return nil, fmt.Errorf("expected operand, got %q", t.val)
}

func (p *filterParser) parsePath() (operandNode, error) {
	t := p.consume()
	parts := []PathPart{{Name: t.val, Index: noIndex}}

	for {
		next := p.peek()
		if next.kind == tkDot {
			p.consume()
			name := p.consume()
			if name.kind != tkIdent && name.kind != tkExprName {
				return nil, fmt.Errorf("expected attribute name after '.'")
			}
			parts = append(parts, PathPart{Name: name.val, Index: noIndex})
		} else if next.kind == tkLBracket {
			p.consume()
			num := p.consume()
			if num.kind != tkNumber {
				return nil, fmt.Errorf("expected integer index in '[]'")
			}
			n, _ := strconv.Atoi(num.val)
			parts = append(parts, PathPart{Name: "", Index: n})
			if p.peek().kind != tkRBracket {
				return nil, fmt.Errorf("expected ']' after list index")
			}
			p.consume()
		} else {
			break
		}
	}
	return &pathOperandNode{parts}, nil
}

func (p *filterParser) parseOperandList() ([]operandNode, error) {
	var list []operandNode
	for {
		op, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		list = append(list, op)
		if p.peek().kind != tkComma {
			break
		}
		p.consume()
	}
	return list, nil
}

// ─── Update expression ────────────────────────────────────────────────────────

// applyUpdateExpression applies a DynamoDB UpdateExpression to item in place.
// Returns *ExpressionError for invalid expressions.
func applyUpdateExpression(item map[string]any, expr string, names map[string]string, values map[string]any) error {
	clauses := parseUpdateClauses(expr)

	if setExpr := clauses["SET"]; setExpr != "" {
		for _, assignment := range splitComma(setExpr) {
			assignment = strings.TrimSpace(assignment)
			eqIdx := strings.Index(assignment, "=")
			if eqIdx < 0 {
				continue
			}
			lhsRef := strings.TrimSpace(assignment[:eqIdx])
			rhsExpr := strings.TrimSpace(assignment[eqIdx+1:])

			lhsParts := ResolveParts(ParsePathParts(lhsRef), names)
			val, err := evalSetValue(item, rhsExpr, names, values)
			if err != nil {
				return err
			}
			if val == nil {
				continue
			}
			if err := SetPath(item, lhsParts, val); err != nil {
				return &ExpressionError{Message: err.Error()}
			}
		}
	}

	if removeExpr := clauses["REMOVE"]; removeExpr != "" {
		for _, attrRef := range splitComma(removeExpr) {
			parts := ResolveParts(ParsePathParts(strings.TrimSpace(attrRef)), names)
			RemovePath(item, parts)
		}
	}

	if addExpr := clauses["ADD"]; addExpr != "" {
		for _, assignment := range splitComma(addExpr) {
			assignment = strings.TrimSpace(assignment)
			spIdx := strings.Index(assignment, " ")
			if spIdx < 0 {
				continue
			}
			pathRef := strings.TrimSpace(assignment[:spIdx])
			valRef := strings.TrimSpace(assignment[spIdx+1:])

			pathParts := ResolveParts(ParsePathParts(pathRef), names)
			val := resolveExprValue(valRef, values)
			existing, _ := ResolvePath(item, pathParts)
			newVal := applyAddOp(existing, val)
			if err := SetPath(item, pathParts, newVal); err != nil {
				return &ExpressionError{Message: err.Error()}
			}
		}
	}

	if deleteExpr := clauses["DELETE"]; deleteExpr != "" {
		for _, assignment := range splitComma(deleteExpr) {
			assignment = strings.TrimSpace(assignment)
			spIdx := strings.Index(assignment, " ")
			if spIdx < 0 {
				continue
			}
			pathRef := strings.TrimSpace(assignment[:spIdx])
			valRef := strings.TrimSpace(assignment[spIdx+1:])

			pathParts := ResolveParts(ParsePathParts(pathRef), names)
			val := resolveExprValue(valRef, values)
			existing, ok := ResolvePath(item, pathParts)
			if !ok {
				continue
			}
			newSet, empty := deleteFromSet(existing, val)
			if empty {
				RemovePath(item, pathParts)
			} else if newSet != nil {
				_ = SetPath(item, pathParts, newSet)
			}
		}
	}
	return nil
}

// parseUpdateClauses splits an UpdateExpression into its clause bodies.
func parseUpdateClauses(expr string) map[string]string {
	result := make(map[string]string)
	padded := strings.ToUpper(expr) + " "

	type kp struct {
		kw  string
		pos int
	}
	var found []kp
	for _, kw := range []string{"SET", "REMOVE", "ADD", "DELETE"} {
		target := kw + " "
		start := 0
		for {
			i := strings.Index(padded[start:], target)
			if i < 0 {
				break
			}
			abs := start + i
			if abs == 0 || padded[abs-1] == ' ' {
				found = append(found, kp{kw, abs})
			}
			start = abs + 1
		}
	}
	sort.Slice(found, func(i, j int) bool { return found[i].pos < found[j].pos })

	for i, f := range found {
		contentStart := f.pos + len(f.kw) + 1
		contentEnd := len(expr)
		if i+1 < len(found) {
			contentEnd = found[i+1].pos
		}
		if contentStart > contentEnd {
			contentStart = contentEnd
		}
		result[f.kw] = strings.TrimSpace(expr[contentStart:contentEnd])
	}
	return result
}

// evalSetValue evaluates the RHS of a SET assignment.
func evalSetValue(item map[string]any, expr string, names map[string]string, values map[string]any) (any, error) {
	expr = strings.TrimSpace(expr)
	lower := strings.ToLower(expr)

	// if_not_exists(path, value)  — supports nested calls like if_not_exists(a, if_not_exists(b, :v))
	if strings.HasPrefix(lower, "if_not_exists(") {
		// Find the matching closing paren for the outer call.
		inner, ok := extractFunctionBody(expr, len("if_not_exists"))
		if !ok {
			return nil, &ExpressionError{Message: "malformed if_not_exists expression"}
		}
		parts := splitComma(inner)
		if len(parts) != 2 {
			return nil, &ExpressionError{Message: "if_not_exists requires exactly 2 arguments"}
		}
		pathRef := strings.TrimSpace(parts[0])
		valRef := strings.TrimSpace(parts[1])
		pathParts := ResolveParts(ParsePathParts(pathRef), names)
		v, exists := ResolvePath(item, pathParts)
		if exists {
			return v, nil // return current value (set is a no-op)
		}
		// The fallback value may itself be an if_not_exists call.
		return evalSetValue(item, valRef, names, values)
	}

	// list_append(list1, list2)
	if strings.HasPrefix(lower, "list_append(") {
		inner, ok := extractFunctionBody(expr, len("list_append"))
		if !ok {
			return nil, &ExpressionError{Message: "malformed list_append expression"}
		}
		parts := splitComma(inner)
		if len(parts) != 2 {
			return nil, &ExpressionError{Message: "list_append requires exactly 2 arguments"}
		}
		left := evalOperandForSet(item, strings.TrimSpace(parts[0]), names, values)
		right := evalOperandForSet(item, strings.TrimSpace(parts[1]), names, values)
		return appendLists(left, right), nil
	}

	// Arithmetic with + or - at top-level (outside any parentheses/brackets).
	// Scan for the LAST occurrence at depth 0 to correctly handle expressions like
	// "attribute_exists(Foo) + :n" or "a + b - c".
	if idx := findTopLevelArithOp(expr); idx >= 0 {
		op := string(expr[idx])
		leftRef := strings.TrimSpace(expr[:idx])
		rightRef := strings.TrimSpace(expr[idx+1:])
		left := evalOperandForSet(item, leftRef, names, values)
		right := evalOperandForSet(item, rightRef, names, values)
		ln, lOk := ParseNumeric(left)
		rn, rOk := ParseNumeric(right)
		if lOk && rOk {
			if op == "+" {
				return map[string]any{"N": fmt.Sprintf("%g", ln+rn)}, nil
			}
			return map[string]any{"N": fmt.Sprintf("%g", ln-rn)}, nil
		}
	}

	return evalOperandForSet(item, expr, names, values), nil
}

// extractFunctionBody returns the content inside the outermost parentheses of a
// function call "funcName(...)".  funcNameLen is the length of the function name.
func extractFunctionBody(expr string, funcNameLen int) (string, bool) {
	if len(expr) <= funcNameLen+1 {
		return "", false
	}
	if expr[funcNameLen] != '(' {
		return "", false
	}
	// Find matching closing paren.
	depth := 0
	for i := funcNameLen; i < len(expr); i++ {
		switch expr[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return expr[funcNameLen+1 : i], true
			}
		}
	}
	return "", false
}

// findTopLevelArithOp finds the index of a top-level '+' or '-' operator in expr
// (i.e., not inside parentheses or brackets).  Returns -1 if none found.
// We search left-to-right and return the FIRST top-level +/- so that
// "a + b - c" is evaluated left-to-right.
func findTopLevelArithOp(expr string) int {
	depth := 0
	for i := 0; i < len(expr); i++ {
		switch expr[i] {
		case '(', '[':
			depth++
		case ')', ']':
			depth--
		case '+', '-':
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// ValidateKeyConditionExpression validates that a KeyConditionExpression conforms
// to DynamoDB rules:
//   - The partition key must use the equality operator (=).
//   - The sort key may use =, <, <=, >, >=, BETWEEN, or begins_with.
//   - Any other operator is rejected with a ValidationException.
//
// pkAttr and skAttr are the table/index attribute names.
// An empty skAttr means the table has no sort key.
func ValidateKeyConditionExpression(expr string, pkAttr, skAttr string, names map[string]string, values map[string]any) error {
	if expr == "" {
		return &ExpressionError{Message: "KeyConditionExpression cannot be empty"}
	}

	conditions := splitAND(expr)
	for _, raw := range conditions {
		cond := strings.TrimSpace(raw)
		if cond == "" {
			continue
		}
		condUpper := strings.ToUpper(cond)

		// Determine which attribute this condition targets.
		attr := extractConditionAttr(cond, names)
		if attr == "" {
			// Can't determine attr — allow through (eval will catch errors).
			continue
		}

		if attr == pkAttr {
			// Partition key MUST use =
			if !isEqualityCondition(cond) {
				return &ExpressionError{Message: "Query key condition not supported: " + attr + " must use = operator for partition key"}
			}
			continue
		}

		if skAttr != "" && attr == skAttr {
			// Sort key may use =, <, <=, >, >=, BETWEEN, begins_with
			if !isValidSortKeyCondition(cond, condUpper) {
				return &ExpressionError{Message: "Query key condition not supported: " + attr + " uses an invalid operator for sort key"}
			}
			continue
		}

		// Neither PK nor SK — not allowed in KeyConditionExpression.
		return &ExpressionError{Message: "Query key condition not supported: " + attr + " is not a key attribute"}
	}
	return nil
}

// extractConditionAttr extracts the attribute name from the LHS of a simple
// condition like "pk = :v", "#pk = :v", "sk BETWEEN :lo AND :hi", "begins_with(sk, :pfx)".
func extractConditionAttr(cond string, names map[string]string) string {
	cond = strings.TrimSpace(cond)
	condUpper := strings.ToUpper(cond)

	// begins_with(attr, :val)
	if strings.HasPrefix(strings.ToLower(cond), "begins_with(") {
		inner, ok := extractFunctionBody(cond, len("begins_with"))
		if !ok {
			return ""
		}
		parts := splitComma(inner)
		if len(parts) < 1 {
			return ""
		}
		return resolveExprName(strings.TrimSpace(parts[0]), names)
	}

	// Check for BETWEEN: "attr BETWEEN :lo AND :hi"
	betweenIdx := strings.Index(condUpper, " BETWEEN ")
	if betweenIdx > 0 {
		return resolveExprName(strings.TrimSpace(cond[:betweenIdx]), names)
	}

	// Comparison operators — split on first occurrence.
	for _, op := range []string{" <> ", " <= ", " >= ", " < ", " > ", " = ", "<>", "<=", ">=", "<", ">", "="} {
		idx := strings.Index(cond, op)
		if idx > 0 {
			lhs := strings.TrimSpace(cond[:idx])
			// Make sure lhs is a simple path (no spaces).
			if !strings.Contains(lhs, " ") {
				return resolveExprName(lhs, names)
			}
		}
	}
	return ""
}

// isEqualityCondition returns true if cond is of the form "attr = :val".
func isEqualityCondition(cond string) bool {
	// Try both "attr = :val" (space) and "attr=:val" (no space).
	for _, op := range []string{" = ", "="} {
		idx := strings.Index(cond, op)
		if idx > 0 {
			// Make sure there's no other comparison-style operator present.
			rest := strings.TrimSpace(cond[idx+len(op):])
			if rest == "" {
				continue
			}
			upper := strings.ToUpper(cond)
			if !strings.Contains(upper, " <> ") &&
				!strings.Contains(upper, " < ") &&
				!strings.Contains(upper, " > ") &&
				!strings.Contains(upper, " BETWEEN ") &&
				!strings.Contains(upper, " IN ") {
				return true
			}
		}
	}
	return false
}

// isValidSortKeyCondition returns true if cond uses an operator valid for a sort key.
func isValidSortKeyCondition(cond, condUpper string) bool {
	// begins_with(sk, :val)
	if strings.HasPrefix(strings.ToLower(cond), "begins_with(") {
		return true
	}
	// BETWEEN :lo AND :hi
	if strings.Contains(condUpper, " BETWEEN ") {
		return true
	}
	// = < <= > >=
	for _, op := range []string{" <= ", " >= ", " < ", " > ", " = ", "<=", ">=", "<", ">", "="} {
		if strings.Contains(cond, op) {
			// Make sure it's not <> (inequality) which is not valid for SK.
			if strings.Contains(cond, "<>") {
				return false
			}
			return true
		}
	}
	return false
}

func evalOperandForSet(item map[string]any, ref string, names map[string]string, values map[string]any) any {
	ref = strings.TrimSpace(ref)
	if strings.HasPrefix(ref, ":") {
		return resolveExprValue(ref, values)
	}
	pathParts := ResolveParts(ParsePathParts(ref), names)
	v, _ := ResolvePath(item, pathParts)
	return v
}

func appendLists(a, b any) any {
	var result []any
	if am, ok := a.(map[string]any); ok {
		if al, ok := am["L"].([]any); ok {
			result = append(result, al...)
		}
	}
	if bm, ok := b.(map[string]any); ok {
		if bl, ok := bm["L"].([]any); ok {
			result = append(result, bl...)
		}
	}
	return map[string]any{"L": result}
}

// applyAddOp performs DynamoDB ADD: numeric add or set union.
func applyAddOp(existing, val any) any {
	em, eOk := existing.(map[string]any)
	vm, vOk := val.(map[string]any)
	if eOk && vOk {
		// Numeric add.
		if _, ok := em["N"]; ok {
			if _, ok := vm["N"]; ok {
				n1, _ := ParseNumeric(em)
				n2, _ := ParseNumeric(vm)
				return map[string]any{"N": fmt.Sprintf("%g", n1+n2)}
			}
		}
		// Set union (SS, NS, BS).
		for _, setType := range []string{"SS", "NS", "BS"} {
			if existingSet, ok := em[setType].([]any); ok {
				if addSet, ok := vm[setType].([]any); ok {
					return map[string]any{setType: unionSet(existingSet, addSet)}
				}
			}
		}
	}
	if existing == nil {
		return val
	}
	return val
}

func unionSet(a, b []any) []any {
	seen := make(map[string]bool)
	result := make([]any, 0, len(a)+len(b))
	for _, v := range a {
		key := fmt.Sprintf("%v", v)
		if !seen[key] {
			seen[key] = true
			result = append(result, v)
		}
	}
	for _, v := range b {
		key := fmt.Sprintf("%v", v)
		if !seen[key] {
			seen[key] = true
			result = append(result, v)
		}
	}
	return result
}

// deleteFromSet removes elements in val from the existing set attribute.
// Returns (newSet, empty). empty=true means the result is an empty set (remove the attribute).
func deleteFromSet(existing, val any) (any, bool) {
	em, ok1 := existing.(map[string]any)
	vm, ok2 := val.(map[string]any)
	if !ok1 || !ok2 {
		return existing, false
	}
	for _, setType := range []string{"SS", "NS", "BS"} {
		existingSet, ok3 := em[setType].([]any)
		if !ok3 {
			continue
		}
		delSet, ok4 := vm[setType].([]any)
		if !ok4 {
			continue
		}
		delKeys := make(map[string]bool, len(delSet))
		for _, v := range delSet {
			delKeys[fmt.Sprintf("%v", v)] = true
		}
		var result []any
		for _, v := range existingSet {
			if !delKeys[fmt.Sprintf("%v", v)] {
				result = append(result, v)
			}
		}
		if len(result) == 0 {
			return nil, true
		}
		return map[string]any{setType: result}, false
	}
	return existing, false
}
