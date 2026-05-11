package asl

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"unicode/utf8"
)

// EvalIntrinsic evaluates a States.* intrinsic function call.
// expr is the full intrinsic string, e.g. "States.Format('Hello, {}!', $.name)"
// input is the current state input document.
func EvalIntrinsic(expr string, input any) (any, error) {
	name, argsRaw, err := parseIntrinsicCall(expr)
	if err != nil {
		return nil, fmt.Errorf("intrinsic %q: %w", expr, err)
	}

	// Resolve argument values (JSONPath refs and nested intrinsics)
	args := make([]any, len(argsRaw))
	for i, arg := range argsRaw {
		v, err := resolveArg(arg, input)
		if err != nil {
			return nil, fmt.Errorf("intrinsic %q arg[%d]: %w", name, i, err)
		}
		args[i] = v
	}

	switch name {
	case "States.Format":
		return intrinsicFormat(args)
	case "States.StringToJson":
		return intrinsicStringToJson(args)
	case "States.JsonToString":
		return intrinsicJsonToString(args)
	case "States.Array":
		return args, nil
	case "States.ArrayPartition":
		return intrinsicArrayPartition(args)
	case "States.ArrayContains":
		return intrinsicArrayContains(args)
	case "States.ArrayRange":
		return intrinsicArrayRange(args)
	case "States.ArrayGetItem":
		return intrinsicArrayGetItem(args)
	case "States.ArrayLength":
		return intrinsicArrayLength(args)
	case "States.ArrayUnique":
		return intrinsicArrayUnique(args)
	case "States.Base64Encode":
		return intrinsicBase64Encode(args)
	case "States.Base64Decode":
		return intrinsicBase64Decode(args)
	case "States.Hash":
		return intrinsicHash(args)
	case "States.JsonMerge":
		return intrinsicJsonMerge(args)
	case "States.MathAdd":
		return intrinsicMathAdd(args)
	case "States.MathRandom":
		return intrinsicMathRandom(args)
	case "States.StringSplit":
		return intrinsicStringSplit(args)
	case "States.UUID":
		return intrinsicUUID()
	default:
		return nil, fmt.Errorf("unknown intrinsic function: %s", name)
	}
}

// parseIntrinsicCall splits "States.Func(arg1, arg2, ...)" into name + raw arg strings.
func parseIntrinsicCall(expr string) (name string, args []string, err error) {
	paren := strings.Index(expr, "(")
	if paren < 0 {
		return "", nil, fmt.Errorf("missing opening parenthesis")
	}
	if !strings.HasSuffix(expr, ")") {
		return "", nil, fmt.Errorf("missing closing parenthesis")
	}
	name = expr[:paren]
	argsStr := expr[paren+1 : len(expr)-1]
	args = splitArgs(argsStr)
	return name, args, nil
}

// splitArgs splits a comma-separated argument list respecting nested parens and quotes.
func splitArgs(s string) []string {
	var args []string
	depth := 0
	inStr := false
	strChar := byte(0)
	start := 0

	for i := 0; i < len(s); i++ {
		c := s[i]
		if inStr {
			if c == strChar && (i == 0 || s[i-1] != '\\') {
				inStr = false
			}
			continue
		}
		switch c {
		case '\'', '"':
			inStr = true
			strChar = c
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				args = append(args, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	if last := strings.TrimSpace(s[start:]); last != "" {
		args = append(args, last)
	}
	return args
}

// resolveArg resolves a single argument: JSONPath ref, literal, or nested intrinsic.
func resolveArg(arg string, input any) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, nil
	}
	// JSONPath reference
	if strings.HasPrefix(arg, "$.") || arg == "$" {
		return EvalPath(input, arg)
	}
	// Context object reference
	if strings.HasPrefix(arg, "$$.") {
		return nil, fmt.Errorf("context object ($$.) not supported in intrinsics without context")
	}
	// Nested intrinsic
	if strings.HasPrefix(arg, "States.") {
		return EvalIntrinsic(arg, input)
	}
	// String literal
	if (strings.HasPrefix(arg, "'") && strings.HasSuffix(arg, "'")) ||
		(strings.HasPrefix(arg, `"`) && strings.HasSuffix(arg, `"`)) {
		return arg[1 : len(arg)-1], nil
	}
	// JSON literal (number, bool, null, array, object)
	var v any
	if err := json.Unmarshal([]byte(arg), &v); err == nil {
		return v, nil
	}
	// Fall through: return as raw string
	return arg, nil
}

// ─── individual intrinsic implementations ────────────────────────────────────

func intrinsicFormat(args []any) (any, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("States.Format requires at least 1 argument")
	}
	tmpl, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("States.Format first arg must be string")
	}
	values := args[1:]
	var sb strings.Builder
	idx := 0
	i := 0
	runes := []rune(tmpl)
	for i < len(runes) {
		if runes[i] == '{' && i+1 < len(runes) && runes[i+1] == '}' {
			if idx >= len(values) {
				return nil, fmt.Errorf("States.Format: not enough values for placeholders")
			}
			sb.WriteString(fmt.Sprintf("%v", values[idx]))
			idx++
			i += 2
		} else if runes[i] == '\\' && i+1 < len(runes) && (runes[i+1] == '{' || runes[i+1] == '}') {
			sb.WriteRune(runes[i+1])
			i += 2
		} else {
			sb.WriteRune(runes[i])
			i++
		}
	}
	return sb.String(), nil
}

func intrinsicStringToJson(args []any) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("States.StringToJson requires 1 argument")
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("States.StringToJson argument must be string")
	}
	var v any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return nil, fmt.Errorf("States.StringToJson: invalid JSON: %w", err)
	}
	return v, nil
}

func intrinsicJsonToString(args []any) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("States.JsonToString requires 1 argument")
	}
	b, err := json.Marshal(args[0])
	if err != nil {
		return nil, fmt.Errorf("States.JsonToString: %w", err)
	}
	return string(b), nil
}

func intrinsicArrayPartition(args []any) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("States.ArrayPartition requires 2 arguments")
	}
	arr, ok := args[0].([]any)
	if !ok {
		return nil, fmt.Errorf("States.ArrayPartition first arg must be array")
	}
	size, err := toInt(args[1])
	if err != nil || size <= 0 {
		return nil, fmt.Errorf("States.ArrayPartition second arg must be positive integer")
	}
	var result []any
	for i := 0; i < len(arr); i += size {
		end := i + size
		if end > len(arr) {
			end = len(arr)
		}
		result = append(result, arr[i:end])
	}
	return result, nil
}

func intrinsicArrayContains(args []any) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("States.ArrayContains requires 2 arguments")
	}
	arr, ok := args[0].([]any)
	if !ok {
		return nil, fmt.Errorf("States.ArrayContains first arg must be array")
	}
	target := args[1]
	for _, v := range arr {
		if fmt.Sprintf("%v", v) == fmt.Sprintf("%v", target) {
			return true, nil
		}
	}
	return false, nil
}

func intrinsicArrayRange(args []any) (any, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("States.ArrayRange requires 3 arguments")
	}
	start, err1 := toInt(args[0])
	end, err2 := toInt(args[1])
	step, err3 := toInt(args[2])
	if err1 != nil || err2 != nil || err3 != nil || step == 0 {
		return nil, fmt.Errorf("States.ArrayRange: invalid arguments")
	}
	var result []any
	for i := start; (step > 0 && i <= end) || (step < 0 && i >= end); i += step {
		result = append(result, float64(i))
	}
	return result, nil
}

func intrinsicArrayGetItem(args []any) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("States.ArrayGetItem requires 2 arguments")
	}
	arr, ok := args[0].([]any)
	if !ok {
		return nil, fmt.Errorf("States.ArrayGetItem first arg must be array")
	}
	idx, err := toInt(args[1])
	if err != nil || idx < 0 || idx >= len(arr) {
		return nil, fmt.Errorf("States.ArrayGetItem: index out of bounds")
	}
	return arr[idx], nil
}

func intrinsicArrayLength(args []any) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("States.ArrayLength requires 1 argument")
	}
	arr, ok := args[0].([]any)
	if !ok {
		return nil, fmt.Errorf("States.ArrayLength argument must be array")
	}
	return float64(len(arr)), nil
}

func intrinsicArrayUnique(args []any) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("States.ArrayUnique requires 1 argument")
	}
	arr, ok := args[0].([]any)
	if !ok {
		return nil, fmt.Errorf("States.ArrayUnique argument must be array")
	}
	seen := make(map[string]bool)
	var result []any
	for _, v := range arr {
		k := fmt.Sprintf("%v", v)
		if !seen[k] {
			seen[k] = true
			result = append(result, v)
		}
	}
	return result, nil
}

func intrinsicBase64Encode(args []any) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("States.Base64Encode requires 1 argument")
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("States.Base64Encode argument must be string")
	}
	return encodeBase64([]byte(s)), nil
}

func intrinsicBase64Decode(args []any) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("States.Base64Decode requires 1 argument")
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("States.Base64Decode argument must be string")
	}
	b, err := decodeBase64(s)
	if err != nil {
		return nil, fmt.Errorf("States.Base64Decode: %w", err)
	}
	if !utf8.Valid(b) {
		return nil, fmt.Errorf("States.Base64Decode: result is not valid UTF-8")
	}
	return string(b), nil
}

func intrinsicHash(args []any) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("States.Hash requires 2 arguments")
	}
	data, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("States.Hash first argument must be string")
	}
	algo, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("States.Hash second argument must be string")
	}
	return computeHash(data, algo)
}

func intrinsicJsonMerge(args []any) (any, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("States.JsonMerge requires 3 arguments")
	}
	m1, ok1 := toMap(args[0])
	m2, ok2 := toMap(args[1])
	deep, _ := args[2].(bool)
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("States.JsonMerge first two arguments must be objects")
	}
	result := make(map[string]any, len(m1)+len(m2))
	for k, v := range m1 {
		result[k] = v
	}
	if deep {
		for k, v := range m2 {
			if existing, ok := result[k]; ok {
				em, eok := toMap(existing)
				nm, nok := toMap(v)
				if eok && nok {
					merged, err := intrinsicJsonMerge([]any{em, nm, true})
					if err != nil {
						return nil, err
					}
					result[k] = merged
					continue
				}
			}
			result[k] = v
		}
	} else {
		for k, v := range m2 {
			result[k] = v
		}
	}
	return result, nil
}

func intrinsicMathAdd(args []any) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("States.MathAdd requires 2 arguments")
	}
	a, err1 := toFloat64(args[0])
	b, err2 := toFloat64(args[1])
	if err1 != nil || err2 != nil {
		return nil, fmt.Errorf("States.MathAdd: arguments must be numeric")
	}
	return a + b, nil
}

func intrinsicMathRandom(args []any) (any, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("States.MathRandom requires 2 or 3 arguments")
	}
	lo, err1 := toInt(args[0])
	hi, err2 := toInt(args[1])
	if err1 != nil || err2 != nil || hi < lo {
		return nil, fmt.Errorf("States.MathRandom: invalid range")
	}
	n := rand.Intn(hi-lo+1) + lo
	return float64(n), nil
}

func intrinsicStringSplit(args []any) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("States.StringSplit requires 2 arguments")
	}
	s, ok1 := args[0].(string)
	sep, ok2 := args[1].(string)
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("States.StringSplit arguments must be strings")
	}
	parts := strings.Split(s, sep)
	result := make([]any, len(parts))
	for i, p := range parts {
		result[i] = p
	}
	return result, nil
}

func intrinsicUUID() (any, error) {
	return newUUID(), nil
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func toInt(v any) (int, error) {
	switch n := v.(type) {
	case float64:
		return int(n), nil
	case int:
		return n, nil
	case int64:
		return int(n), nil
	}
	return 0, fmt.Errorf("cannot convert %T to int", v)
}

func toFloat64(v any) (float64, error) {
	switch n := v.(type) {
	case float64:
		return n, nil
	case int:
		return float64(n), nil
	case int64:
		return float64(n), nil
	}
	return 0, fmt.Errorf("cannot convert %T to float64", v)
}
