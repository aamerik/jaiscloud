package firestore

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	firestorestore "jaiscloud/internal/gcp/store/firestore"
	"jaiscloud/internal/model"
)

// ─── wire request types (decoded from the JSON body) ──────────────────────────

type documentMaskWire struct {
	FieldPaths []string `json:"fieldPaths,omitempty"`
}

type preconditionWire struct {
	Exists     *bool  `json:"exists"`
	UpdateTime string `json:"updateTime,omitempty"`
}

type documentWire struct {
	Name       string                           `json:"name,omitempty"`
	Fields     map[string]*firestorestore.Value `json:"fields,omitempty"`
	CreateTime string                           `json:"createTime,omitempty"`
	UpdateTime string                           `json:"updateTime,omitempty"`
}

type fieldTransformWire struct {
	FieldPath             string                     `json:"fieldPath,omitempty"`
	Increment             *firestorestore.Value      `json:"increment,omitempty"`
	Maximum               *firestorestore.Value      `json:"maximum,omitempty"`
	Minimum               *firestorestore.Value      `json:"minimum,omitempty"`
	SetToServerValue      string                     `json:"setToServerValue,omitempty"`
	AppendMissingElements *firestorestore.ArrayValue `json:"appendMissingElements,omitempty"`
	RemoveAllFromArray    *firestorestore.ArrayValue `json:"removeAllFromArray,omitempty"`
}

type documentTransformWire struct {
	Document        string               `json:"document,omitempty"`
	FieldTransforms []fieldTransformWire `json:"fieldTransforms,omitempty"`
}

type writeWire struct {
	Update           *documentWire          `json:"update,omitempty"`
	Delete           string                 `json:"delete,omitempty"`
	Transform        *documentTransformWire `json:"transform,omitempty"`
	UpdateMask       *documentMaskWire      `json:"updateMask,omitempty"`
	UpdateTransforms []fieldTransformWire   `json:"updateTransforms,omitempty"`
	CurrentDocument  *preconditionWire      `json:"currentDocument,omitempty"`
}

type commitRequestWire struct {
	Transaction string       `json:"transaction,omitempty"`
	Writes      []*writeWire `json:"writes,omitempty"`
}

type batchWriteRequestWire struct {
	Writes []*writeWire `json:"writes,omitempty"`
}

type batchGetRequestWire struct {
	Documents   []string `json:"documents,omitempty"`
	Transaction string   `json:"transaction,omitempty"`
}

type rollbackRequestWire struct {
	Transaction string `json:"transaction,omitempty"`
}

type runQueryRequestWire struct {
	StructuredQuery *structuredQuery `json:"structuredQuery,omitempty"`
	Transaction     string           `json:"transaction,omitempty"`
}

// ─── error helpers ────────────────────────────────────────────────────────────

// newPreconditionErr returns a FAILED_PRECONDITION error at HTTP 400 (missing
// composite index / failed precondition).
func newPreconditionErr(msg string) error {
	return &model.ProviderError{Code: "FailedPrecondition", Message: msg, HTTPStatus: 400, Status: "FAILED_PRECONDITION"}
}

// newAbortedErr returns an ABORTED error at HTTP 409 (transaction contention).
func newAbortedErr(msg string) error {
	return &model.ProviderError{Code: "Aborted", Message: msg, HTTPStatus: 409, Status: "ABORTED"}
}

// mapCommitError maps store commit sentinels to Firestore RPC statuses.
func mapCommitError(err error) error {
	switch {
	case errors.Is(err, firestorestore.ErrAborted):
		return newAbortedErr("transaction was aborted due to concurrent modification")
	case errors.Is(err, firestorestore.ErrPreconditionFailed):
		return newPreconditionErr("precondition failed")
	default:
		return err
	}
}

// toPrecondition converts a wire precondition into a store precondition.
func toPrecondition(pw *preconditionWire) (*firestorestore.Precondition, error) {
	if pw == nil {
		return nil, nil
	}
	pre := &firestorestore.Precondition{}
	if pw.Exists != nil {
		pre.Exists = pw.Exists
	}
	if pw.UpdateTime != "" {
		t, err := time.Parse(time.RFC3339Nano, pw.UpdateTime)
		if err != nil {
			return nil, model.NewProviderError("InvalidArgument", "invalid updateTime precondition", 400)
		}
		pre.UpdateTime = &t
	}
	return pre, nil
}

// statusWire renders a google.rpc.Status-style object (code 0 = OK).
func statusWire(code int64, msg string) map[string]any {
	s := map[string]any{"code": code}
	if msg != "" {
		s["message"] = msg
	}
	return s
}

// statusWireForError maps a commit error to a per-write batchWrite status.
func statusWireForError(err error) map[string]any {
	switch {
	case errors.Is(err, firestorestore.ErrAborted):
		return statusWire(10, "transaction aborted")
	case errors.Is(err, firestorestore.ErrPreconditionFailed):
		return statusWire(9, "precondition failed")
	default:
		return statusWire(13, errMsg(err))
	}
}

func errMsg(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// ─── field transforms ─────────────────────────────────────────────────────────

// applyFieldTransforms applies each transform to a copy of fields, returning the
// updated fields and the per-transform result values.
func applyFieldTransforms(fields map[string]*firestorestore.Value, transforms []fieldTransformWire, serverTime time.Time) (map[string]*firestorestore.Value, []*firestorestore.Value, error) {
	out := make(map[string]*firestorestore.Value, len(fields)+len(transforms))
	for k, v := range fields {
		out[k] = v
	}
	results := make([]*firestorestore.Value, 0, len(transforms))
	for _, tf := range transforms {
		if tf.FieldPath == "" {
			return nil, nil, model.NewProviderError("InvalidArgument", "transform fieldPath is required", 400)
		}
		cur := fieldValue(&firestorestore.Document{Fields: out}, tf.FieldPath)
		var result *firestorestore.Value
		switch {
		case tf.Increment != nil:
			result = applyIncrement(cur, tf.Increment)
			out = setFieldPathValue(out, tf.FieldPath, result)
		case tf.Maximum != nil:
			result = applyExtremum(cur, tf.Maximum, true)
			out = setFieldPathValue(out, tf.FieldPath, result)
		case tf.Minimum != nil:
			result = applyExtremum(cur, tf.Minimum, false)
			out = setFieldPathValue(out, tf.FieldPath, result)
		case tf.SetToServerValue != "":
			v := firestorestore.TimestampVal(serverTime)
			out = setFieldPathValue(out, tf.FieldPath, v)
			result = v
		case tf.AppendMissingElements != nil:
			v, err := appendMissing(cur, tf.AppendMissingElements)
			if err != nil {
				return nil, nil, err
			}
			out = setFieldPathValue(out, tf.FieldPath, v)
			result = firestorestore.NullVal()
		case tf.RemoveAllFromArray != nil:
			v, err := removeAll(cur, tf.RemoveAllFromArray)
			if err != nil {
				return nil, nil, err
			}
			out = setFieldPathValue(out, tf.FieldPath, v)
			result = firestorestore.NullVal()
		default:
			return nil, nil, model.NewProviderError("InvalidArgument", "transform must specify an operation", 400)
		}
		results = append(results, result)
	}
	return out, results, nil
}

// setFieldPathValue sets the value at a (possibly dotted) field path.
func setFieldPathValue(fields map[string]*firestorestore.Value, path string, v *firestorestore.Value) map[string]*firestorestore.Value {
	setFieldPath(fields, strings.Split(path, "."), v)
	return fields
}

func applyIncrement(cur, inc *firestorestore.Value) *firestorestore.Value {
	if !isNumeric(cur) {
		return inc
	}
	ci, cint, cfloat := numberParts(cur)
	ii, iint, ifloat := numberParts(inc)
	if ci && ii {
		return firestorestore.IntVal(clampAdd(cint, iint))
	}
	cf := cfloat
	if ci {
		cf = float64(cint)
	}
	inf := ifloat
	if ii {
		inf = float64(iint)
	}
	return firestorestore.DoubleVal(cf + inf)
}

func applyExtremum(cur, v *firestorestore.Value, isMax bool) *firestorestore.Value {
	if !isNumeric(cur) {
		return v
	}
	c := compareNumbers(cur, v)
	if c == 0 {
		return cur
	}
	if (isMax && c > 0) || (!isMax && c < 0) {
		return cur
	}
	return v
}

func appendMissing(cur *firestorestore.Value, arr *firestorestore.ArrayValue) (*firestorestore.Value, error) {
	existing := []*firestorestore.Value{}
	if cur != nil && cur.ArrayValue != nil {
		existing = append(existing, cur.ArrayValue.Values...)
	}
	toAppend := []*firestorestore.Value{}
	if arr != nil {
		toAppend = arr.Values
	}
	for _, v := range toAppend {
		dup := false
		for _, e := range existing {
			if valuesEqual(e, v) {
				dup = true
				break
			}
		}
		if !dup {
			existing = append(existing, v)
		}
	}
	return firestorestore.ArrayVal(existing...), nil
}

func removeAll(cur *firestorestore.Value, arr *firestorestore.ArrayValue) (*firestorestore.Value, error) {
	var keep []*firestorestore.Value
	if cur != nil && cur.ArrayValue != nil {
		for _, e := range cur.ArrayValue.Values {
			remove := false
			if arr != nil {
				for _, r := range arr.Values {
					if valuesEqual(e, r) {
						remove = true
						break
					}
				}
			}
			if !remove {
				keep = append(keep, e)
			}
		}
	}
	return firestorestore.ArrayVal(keep...), nil
}

func isNumeric(v *firestorestore.Value) bool {
	return v != nil && (v.IntegerValue != nil || v.DoubleValue != nil)
}

func clampAdd(a, b int64) int64 {
	sum := a + b
	if (b > 0 && sum < a) || (b < 0 && sum > a) {
		if b > 0 {
			return 1<<63 - 1
		}
		return -1 << 63
	}
	return sum
}

// valueWire renders a Value as a wire-compatible map for writeResults.
func valueWire(v *firestorestore.Value) map[string]any {
	if v == nil {
		return map[string]any{}
	}
	b, _ := json.Marshal(v)
	var m map[string]any
	json.Unmarshal(b, &m)
	return m
}

// writeBatchSize returns the approximate wire size of the writes, enforcing the
// per-document limit in the process.
func writeBatchSize(writes []*writeWire) (int, error) {
	b, err := json.Marshal(writes)
	if err != nil {
		return 0, model.NewProviderError("InvalidArgument", "malformed writes", 400)
	}
	return len(b), nil
}
