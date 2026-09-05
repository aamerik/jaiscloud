package firestore

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	firestorestore "jaiscloud/internal/gcp/store/firestore"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
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

// ─── transaction state (provider-scoped) ─────────────────────────────────────

// readSet records the documents read within a transaction for optimistic
// concurrency re-validation at commit.
type readSet struct {
	reads map[string]firestorestore.ReadRef
}

// decodeBody round-trips nr.Params["body"] into a typed struct.
func decodeBody(nr *model.NormalizedRequest, v any) error {
	body, _ := nr.Params["body"]
	if body == nil {
		body = map[string]any{}
	}
	b, err := json.Marshal(body)
	if err != nil {
		return model.NewProviderError("InvalidArgument", "malformed request body", 400)
	}
	if err := json.Unmarshal(b, v); err != nil {
		return model.NewProviderError("InvalidArgument", "malformed request body", 400)
	}
	return nil
}

// recordRead registers a document read in a transaction's read-set. Missing
// documents are recorded with Exists=false so a concurrent create aborts.
func (p *Provider) recordRead(txnID, name string, doc firestorestore.Document, exists bool) {
	if txnID == "" {
		return
	}
	p.txnMu.Lock()
	defer p.txnMu.Unlock()
	rs := p.readSets[txnID]
	if rs == nil {
		return
	}
	if rs.reads == nil {
		rs.reads = make(map[string]firestorestore.ReadRef)
	}
	rs.reads[name] = firestorestore.ReadRef{Name: name, Exists: exists, UpdateTime: doc.UpdateTime}
}

// readSetFor returns the read-set entries for a transaction, or nil.
func (p *Provider) readSetFor(txnID string) []firestorestore.ReadRef {
	if txnID == "" {
		return nil
	}
	p.txnMu.Lock()
	defer p.txnMu.Unlock()
	rs := p.readSets[txnID]
	if rs == nil || len(rs.reads) == 0 {
		return nil
	}
	out := make([]firestorestore.ReadRef, 0, len(rs.reads))
	for _, r := range rs.reads {
		out = append(out, r)
	}
	return out
}

// clearReadSet removes a transaction's read-set (after commit/rollback).
func (p *Provider) clearReadSet(txnID string) {
	p.txnMu.Lock()
	defer p.txnMu.Unlock()
	delete(p.readSets, txnID)
}

// newTxnID returns an opaque base64 transaction ID.
func newTxnID() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "txn-" + base64.RawURLEncoding.EncodeToString([]byte(time.Now().String()))
	}
	return base64.RawURLEncoding.EncodeToString(b)
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

// ─── handlers ─────────────────────────────────────────────────────────────────

// BeginTransaction implements documents.beginTransaction.
func (p *Provider) BeginTransaction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	txnID := newTxnID()
	p.txnMu.Lock()
	if p.readSets == nil {
		p.readSets = make(map[string]*readSet)
	}
	p.readSets[txnID] = &readSet{reads: map[string]firestorestore.ReadRef{}}
	p.txnMu.Unlock()
	return provider.OK(map[string]any{"transaction": txnID}), nil
}

// Rollback implements documents.rollback.
func (p *Provider) Rollback(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	var req rollbackRequestWire
	if err := decodeBody(nr, &req); err != nil {
		return nil, err
	}
	p.clearReadSet(req.Transaction)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

// Commit implements documents.commit: an atomic batch of writes with optimistic
// preconditions and transaction read-set validation.
func (p *Provider) Commit(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	var req commitRequestWire
	if err := decodeBody(nr, &req); err != nil {
		return nil, err
	}
	if len(req.Writes) > maxWriteBatchSize {
		return nil, model.NewProviderError("InvalidArgument",
			"a commit may contain at most "+strconv.Itoa(maxWriteBatchSize)+" writes", 400)
	}
	if size, err := writeBatchSize(req.Writes); err != nil {
		return nil, err
	} else if size > maxTxnBytes {
		return nil, model.NewProviderError("InvalidArgument", "transaction exceeds the maximum size of 10 MiB", 400)
	}

	commitTime := clock.Now()
	writes, results, err := p.buildWrites(ctx, nr, req.Writes, commitTime)
	if err != nil {
		return nil, err
	}
	reads := p.readSetFor(req.Transaction)
	if err := p.store.Commit(ctx, reads, writes); err != nil {
		return nil, mapCommitError(err)
	}
	p.clearReadSet(req.Transaction)

	wr := make([]any, 0, len(results))
	for _, r := range results {
		wr = append(wr, r)
	}
	return provider.OK(map[string]any{
		"commitTime":   commitTime.Format(time.RFC3339Nano),
		"writeResults": wr,
	}), nil
}

// buildWrites translates wire writes into store writes + write-results,
// resolving masks and transforms against the current document state.
func (p *Provider) buildWrites(ctx context.Context, nr *model.NormalizedRequest, wire []*writeWire, now time.Time) ([]firestorestore.Write, []map[string]any, error) {
	writes := make([]firestorestore.Write, 0, len(wire))
	results := make([]map[string]any, 0, len(wire))
	for _, w := range wire {
		pre, err := toPrecondition(w.CurrentDocument)
		if err != nil {
			return nil, nil, err
		}

		switch {
		case w.Delete != "":
			writes = append(writes, firestorestore.Write{Name: w.Delete, Precondition: pre})
			results = append(results, map[string]any{}) // no updateTime after delete

		case w.Transform != nil:
			doc, res, err := p.buildTransform(ctx, w.Transform, pre, now)
			if err != nil {
				return nil, nil, err
			}
			writes = append(writes, firestorestore.Write{Name: doc.Name, Document: &doc, Precondition: pre})
			results = append(results, res)

		case w.Update != nil:
			doc, res, err := p.buildUpdate(ctx, w.Update, w.UpdateMask, w.UpdateTransforms, pre, now)
			if err != nil {
				return nil, nil, err
			}
			writes = append(writes, firestorestore.Write{Name: doc.Name, Document: &doc, Precondition: pre})
			results = append(results, res)

		default:
			return nil, nil, model.NewProviderError("InvalidArgument", "write must specify update, delete, or transform", 400)
		}
	}
	return writes, results, nil
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

// buildUpdate resolves an update (with optional mask + transforms) against the
// current document.
func (p *Provider) buildUpdate(ctx context.Context, dw *documentWire, mask *documentMaskWire, transforms []fieldTransformWire, pre *firestorestore.Precondition, now time.Time) (firestorestore.Document, map[string]any, error) {
	if dw.Name == "" {
		return firestorestore.Document{}, nil, model.NewProviderError("InvalidArgument", "update document name is required", 400)
	}
	if err := validateFields(dw.Fields); err != nil {
		return firestorestore.Document{}, nil, err
	}
	if err := checkSize(dw.Fields); err != nil {
		return firestorestore.Document{}, nil, err
	}

	existing, err := p.store.GetDocument(ctx, dw.Name)
	exists := err == nil
	if err != nil && !errors.Is(err, firestorestore.ErrDocumentNotFound) {
		return firestorestore.Document{}, nil, err
	}

	var maskPaths []string
	if mask != nil {
		maskPaths = mask.FieldPaths
	}
	base := map[string]*firestorestore.Value{}
	if exists {
		base = existing.Fields
	}
	merged := applyMask(dw.Fields, maskPaths, base)

	// Apply post-update transforms to the merged fields.
	var transformResults []*firestorestore.Value
	if len(transforms) > 0 {
		var res []*firestorestore.Value
		merged, res, err = applyFieldTransforms(merged, transforms, now)
		if err != nil {
			return firestorestore.Document{}, nil, err
		}
		transformResults = res
	}
	if err := checkSize(merged); err != nil {
		return firestorestore.Document{}, nil, err
	}

	createTime := now
	if exists {
		createTime = existing.CreateTime
	}
	doc := firestorestore.Document{Name: dw.Name, Fields: merged, CreateTime: createTime, UpdateTime: now}
	res := map[string]any{"updateTime": now.Format(time.RFC3339Nano)}
	if len(transformResults) > 0 {
		tr := make([]any, 0, len(transformResults))
		for _, r := range transformResults {
			tr = append(tr, valueWire(r))
		}
		res["transformResults"] = tr
	}
	return doc, res, nil
}

// buildTransform resolves a document transform against the current document.
func (p *Provider) buildTransform(ctx context.Context, tw *documentTransformWire, pre *firestorestore.Precondition, now time.Time) (firestorestore.Document, map[string]any, error) {
	if tw.Document == "" {
		return firestorestore.Document{}, nil, model.NewProviderError("InvalidArgument", "transform document name is required", 400)
	}
	existing, err := p.store.GetDocument(ctx, tw.Document)
	exists := err == nil
	if err != nil && !errors.Is(err, firestorestore.ErrDocumentNotFound) {
		return firestorestore.Document{}, nil, err
	}

	base := map[string]*firestorestore.Value{}
	createTime := now
	if exists {
		base = existing.Fields
		createTime = existing.CreateTime
	}
	fields, transformResults, err := applyFieldTransforms(base, tw.FieldTransforms, now)
	if err != nil {
		return firestorestore.Document{}, nil, err
	}
	if err := checkSize(fields); err != nil {
		return firestorestore.Document{}, nil, err
	}

	doc := firestorestore.Document{Name: tw.Document, Fields: fields, CreateTime: createTime, UpdateTime: now}
	tr := make([]any, 0, len(transformResults))
	for _, r := range transformResults {
		tr = append(tr, valueWire(r))
	}
	return doc, map[string]any{"updateTime": now.Format(time.RFC3339Nano), "transformResults": tr}, nil
}

// BatchWrite implements documents.batchWrite: non-atomic, per-write results.
func (p *Provider) BatchWrite(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	var req batchWriteRequestWire
	if err := decodeBody(nr, &req); err != nil {
		return nil, err
	}
	if len(req.Writes) > maxWriteBatchSize {
		return nil, model.NewProviderError("InvalidArgument",
			"a batchWrite may contain at most "+strconv.Itoa(maxWriteBatchSize)+" writes", 400)
	}
	now := clock.Now()
	statuses := make([]any, 0, len(req.Writes))
	writeResults := make([]any, 0, len(req.Writes))
	for _, w := range req.Writes {
		writes, results, err := p.buildWrites(ctx, nr, []*writeWire{w}, now)
		if err != nil {
			statuses = append(statuses, statusWire(3, errMsg(err)))
			writeResults = append(writeResults, map[string]any{})
			continue
		}
		if err := p.store.Commit(ctx, nil, writes); err != nil {
			statuses = append(statuses, statusWireForError(err))
			writeResults = append(writeResults, map[string]any{})
			continue
		}
		statuses = append(statuses, statusWire(0, ""))
		writeResults = append(writeResults, results[0])
	}
	return provider.OK(map[string]any{
		"status":       statuses,
		"writeResults": writeResults,
	}), nil
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

// BatchGet implements documents.batchGet. The response body is newline-delimited
// JSON (server-streaming): one BatchGetDocumentsResponse object per line, each
// either {"found":{...},"readTime":"..."} or {"missing":"<name>","readTime":"..."},
// with no trailing done line (BatchGetDocumentsResponse has no done field).
func (p *Provider) BatchGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	var req batchGetRequestWire
	if err := decodeBody(nr, &req); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	readTime := clock.Now()
	lines := make([]string, 0, len(req.Documents))
	for _, name := range req.Documents {
		if seen[name] {
			continue
		}
		seen[name] = true
		var item map[string]any
		doc, err := p.store.GetDocument(ctx, name)
		if err != nil {
			if errors.Is(err, firestorestore.ErrDocumentNotFound) {
				p.recordRead(req.Transaction, name, firestorestore.Document{}, false)
				item = map[string]any{"missing": name, "readTime": readTime.Format(time.RFC3339Nano)}
			} else {
				return nil, err
			}
		} else {
			p.recordRead(req.Transaction, name, doc, true)
			item = map[string]any{"found": documentMap(doc), "readTime": readTime.Format(time.RFC3339Nano)}
		}
		b, err := json.Marshal(item)
		if err != nil {
			return nil, err
		}
		lines = append(lines, string(b))
	}
	return provider.OK(map[string]any{wire.RawJSONKey: json.RawMessage(strings.Join(lines, "\n"))}), nil
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
