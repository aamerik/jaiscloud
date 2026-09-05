// Package firestore implements the Cloud Firestore provider (document CRUD) on
// top of the dedicated FirestoreStore. This is the DynamoDB analogue: documents
// are schemaless, and collections are implicit path segments (no CreateTable/DDL).
//
// Slice 1 implements documents.get / createDocument / patch / delete over the
// REST/JSON surface. runQuery (StructuredQuery), commit/transactions, and
// composite-index enforcement are TODO(slice-2).
package firestore

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"jaiscloud/internal/clock"
	firestorestore "jaiscloud/internal/gcp/store/firestore"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const (
	// maxDocumentSize is the Firestore per-document limit (1 MiB).
	maxDocumentSize = 1 << 20 // 1,048,576 bytes
	// maxFieldNameBytes is the per-field-name byte limit (document.proto).
	maxFieldNameBytes = 1500
	// maxStringBytes is the per-stringValue/bytesValue byte limit (1 MiB − 89).
	maxStringBytes = (1 << 20) - 89 // 1,048,487 bytes
	// autoIDLength is the length of a server-generated document ID.
	autoIDLength = 20
)

// autoIDAlphabet is the alphabet Firestore uses for auto-generated document IDs.
const autoIDAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

// reservedNameRe matches reserved field names / document IDs (^__.*__$).
var reservedNameRe = regexp.MustCompile(`^__.*__$`)

// Provider implements the Firestore documents REST surface.
type Provider struct {
	store firestorestore.FirestoreStore
	// resources holds the composite-index registry (control plane), scoped by
	// project (AccountID) and encoded as "firestore_index" ResourceEntries.
	resources store.ResourceStore

	// txnMu guards readSets: the in-memory transaction read-set registry
	// (transactions are ephemeral and never persisted).
	txnMu    sync.Mutex
	readSets map[string]*readSet
}

// New returns a Firestore provider backed by the given document store and
// control-plane resource store (for composite indexes).
func New(s firestorestore.FirestoreStore, resources store.ResourceStore) *Provider {
	return &Provider{
		store:     s,
		resources: resources,
		readSets:  make(map[string]*readSet),
	}
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Firestore.GetDocument":       p.DocumentsGet,
		"Firestore.CreateDocument":    p.CreateDocument,
		"Firestore.PatchDocument":     p.DocumentsPatch,
		"Firestore.DeleteDocument":    p.DocumentsDelete,
		"Firestore.ListDocuments":     p.ListDocuments,
		"Firestore.ListCollectionIds": p.ListCollectionIds,
		"Firestore.RunQuery":          p.RunQuery,
		"Firestore.Commit":            p.Commit,
		"Firestore.BatchWrite":        p.BatchWrite,
		"Firestore.BatchGet":          p.BatchGet,
		"Firestore.BeginTransaction":  p.BeginTransaction,
		"Firestore.Rollback":          p.Rollback,
		"Firestore.CreateIndex":       p.CreateIndex,
		"Firestore.ListIndexes":       p.ListIndexes,
		"Firestore.GetIndex":          p.GetIndex,
		"Firestore.DeleteIndex":       p.DeleteIndex,
	}
}

// Reset clears transaction read-sets (document state is reset via the store's
// own Resetter; index state via the resource store's).
func (p *Provider) Reset(_ context.Context) {
	p.txnMu.Lock()
	p.readSets = make(map[string]*readSet)
	p.txnMu.Unlock()
}

// ─── helpers ──────────────────────────────────────────────────────────────────

// relativeName returns the database id and relative document path encoded in
// nr.Params["name"] ("databases/{db}/documents/{path}").
func relativeName(nr *model.NormalizedRequest) (database, path string, err error) {
	name, _ := nr.Params["name"].(string)
	parts := strings.Split(name, "/")
	if len(parts) < 3 || parts[0] != "databases" || parts[2] != "documents" {
		return "", "", model.NewProviderError("InvalidArgument", "invalid document path", 400)
	}
	return parts[1], strings.Join(parts[3:], "/"), nil
}

// fullName builds the full document resource name from the request.
func fullName(nr *model.NormalizedRequest, database, path string) string {
	return nr.ResourceID("firestore-document", "databases/"+database+"/documents/"+path)
}

// mapStoreError maps store sentinel errors to Firestore GCP error envelopes.
//
// TODO(slice-2): surface precondition failures as HTTP 400 FAILED_PRECONDITION
// and transaction contention as HTTP 409 ABORTED. Those require a per-error
// RPC-status override on the shared JSONCodec envelope (the envelope currently
// derives `status` from the HTTP code).
func mapStoreError(err error) error {
	switch {
	case errors.Is(err, firestorestore.ErrDocumentNotFound):
		return model.NewProviderError("NotFound", "document not found", 404)
	case errors.Is(err, firestorestore.ErrDocumentExists):
		return model.NewProviderError("AlreadyExists", "document already exists", 409)
	case errors.Is(err, firestorestore.ErrInvalidArgument):
		return model.NewProviderError("InvalidArgument", "invalid document", 400)
	default:
		return err
	}
}

// extractFields extracts the document fields from a decoded request body.
func extractFields(body map[string]any) (map[string]*firestorestore.Value, error) {
	if body == nil {
		return map[string]*firestorestore.Value{}, nil
	}
	raw, ok := body["fields"]
	if !ok || raw == nil {
		return map[string]*firestorestore.Value{}, nil
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return nil, model.NewProviderError("InvalidArgument", "malformed fields", 400)
	}
	var fields map[string]*firestorestore.Value
	if err := json.Unmarshal(b, &fields); err != nil {
		return nil, model.NewProviderError("InvalidArgument", "malformed field value", 400)
	}
	return fields, nil
}

// validatePath rejects reserved collection/document IDs (^__.*__$) in a
// relative document path.
func validatePath(path string) error {
	if path == "" {
		return nil
	}
	for _, seg := range strings.Split(path, "/") {
		if reservedNameRe.MatchString(seg) {
			return model.NewProviderError("InvalidArgument", "collection or document id "+seg+" is reserved", 400)
		}
	}
	return nil
}

// validateDocumentID enforces Firestore document-id constraints: no '/' (the
// hierarchy separator) and no reserved ^__.*__$ prefix/suffix.
func validateDocumentID(id string) error {
	if strings.Contains(id, "/") {
		return model.NewProviderError("InvalidArgument", "document id must not contain '/'", 400)
	}
	if reservedNameRe.MatchString(id) {
		return model.NewProviderError("InvalidArgument", "document id "+id+" is reserved", 400)
	}
	return nil
}

// validateFields rejects reserved field names (^__.*__$) anywhere in the field
// tree.
func validateFields(fields map[string]*firestorestore.Value) error {
	for k, v := range fields {
		if reservedNameRe.MatchString(k) {
			return model.NewProviderError("InvalidArgument", "field name "+k+" is reserved", 400)
		}
		if err := validateValue(v); err != nil {
			return err
		}
	}
	return nil
}

func validateValue(v *firestorestore.Value) error {
	if v == nil {
		return nil
	}
	if v.MapValue != nil {
		for k, cv := range v.MapValue.Fields {
			if reservedNameRe.MatchString(k) {
				return model.NewProviderError("InvalidArgument", "field name "+k+" is reserved", 400)
			}
			if err := validateValue(cv); err != nil {
				return err
			}
		}
	}
	if v.ArrayValue != nil {
		for _, cv := range v.ArrayValue.Values {
			if err := validateValue(cv); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkSize enforces the 1 MiB per-document size limit by serializing the fields,
// and the per-field limits (field-name length, string/bytes value caps) via a
// recursive value-tree walk.
func checkSize(fields map[string]*firestorestore.Value) error {
	if err := checkFieldLimits(fields); err != nil {
		return err
	}
	b, err := json.Marshal(fields)
	if err != nil {
		return model.NewProviderError("InvalidArgument", "malformed document fields", 400)
	}
	if len(b) > maxDocumentSize {
		return model.NewProviderError("InvalidArgument", "document exceeds maximum size of 1 MiB", 400)
	}
	return nil
}

// checkFieldLimits walks the Value tree (top-level fields, nested mapValue.fields,
// arrayValue.values) enforcing the per-field limits: a field name must be
// non-empty and ≤ 1,500 UTF-8 bytes; a stringValue/bytesValue must be
// ≤ 1 MiB − 89 bytes.
func checkFieldLimits(fields map[string]*firestorestore.Value) error {
	for k, v := range fields {
		if err := checkFieldName(k); err != nil {
			return err
		}
		if err := checkValueLimits(v); err != nil {
			return err
		}
	}
	return nil
}

func checkFieldName(k string) error {
	if k == "" {
		return model.NewProviderError("InvalidArgument", "field name must not be empty", 400)
	}
	if len(k) > maxFieldNameBytes {
		return model.NewProviderError("InvalidArgument", "field name exceeds maximum size of 1500 bytes", 400)
	}
	return nil
}

func checkValueLimits(v *firestorestore.Value) error {
	if v == nil {
		return nil
	}
	if v.StringValue != nil && len(*v.StringValue) > maxStringBytes {
		return model.NewProviderError("InvalidArgument", "string value exceeds maximum size of 1048487 bytes", 400)
	}
	if v.BytesValue != nil && len(v.BytesValue) > maxStringBytes {
		return model.NewProviderError("InvalidArgument", "bytes value exceeds maximum size of 1048487 bytes", 400)
	}
	if v.MapValue != nil {
		for k, cv := range v.MapValue.Fields {
			if err := checkFieldName(k); err != nil {
				return err
			}
			if err := checkValueLimits(cv); err != nil {
				return err
			}
		}
	}
	if v.ArrayValue != nil {
		for _, cv := range v.ArrayValue.Values {
			if err := checkValueLimits(cv); err != nil {
				return err
			}
		}
	}
	return nil
}

// randomID generates a Firestore-style auto-ID of n characters.
func randomID(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return strings.Repeat("0", n)
	}
	for i := range b {
		b[i] = autoIDAlphabet[int(b[i])%len(autoIDAlphabet)]
	}
	return string(b)
}

// queryFieldPaths returns the field paths for a repeated query parameter (e.g.
// "mask.fieldPaths", "updateMask.fieldPaths"), honouring repeated and
// comma-separated values.
func queryFieldPaths(nr *model.NormalizedRequest, key string) []string {
	var paths []string
	add := func(v string) {
		for _, part := range strings.Split(v, ",") {
			if part = strings.TrimSpace(part); part != "" {
				paths = append(paths, part)
			}
		}
	}
	if nr.Raw != nil {
		for _, v := range nr.Raw.URL.Query()[key] {
			add(v)
		}
	}
	if len(paths) == 0 {
		if v, _ := nr.Params[key].(string); v != "" {
			add(v)
		}
	}
	return paths
}

// updateMaskPaths returns the updateMask field paths, honouring repeated and
// comma-separated updateMask.fieldPaths query parameters.
func updateMaskPaths(nr *model.NormalizedRequest) []string {
	return queryFieldPaths(nr, "updateMask.fieldPaths")
}

// maskFieldPaths returns the mask field paths (mask.fieldPaths query parameter).
func maskFieldPaths(nr *model.NormalizedRequest) []string {
	return queryFieldPaths(nr, "mask.fieldPaths")
}

// preconditionFromQuery parses the currentDocument precondition query parameters
// (currentDocument.exists / currentDocument.updateTime) used by patch/delete.
// It returns nil when no precondition is set.
func preconditionFromQuery(nr *model.NormalizedRequest) (*firestorestore.Precondition, error) {
	pre := &firestorestore.Precondition{}
	if s, _ := nr.Params["currentDocument.exists"].(string); s != "" {
		v, err := strconv.ParseBool(s)
		if err != nil {
			return nil, model.NewProviderError("InvalidArgument", "invalid currentDocument.exists precondition", 400)
		}
		pre.Exists = &v
	}
	if s, _ := nr.Params["currentDocument.updateTime"].(string); s != "" {
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return nil, model.NewProviderError("InvalidArgument", "invalid currentDocument.updateTime precondition", 400)
		}
		pre.UpdateTime = &t
	}
	if pre.Exists == nil && pre.UpdateTime == nil {
		return nil, nil
	}
	return pre, nil
}

// checkPrecondition validates a document precondition against the current state
// (exists and updateTime). It returns a FAILED_PRECONDITION error on mismatch.
func checkPrecondition(exists bool, updateTime time.Time, pre *firestorestore.Precondition) error {
	if pre == nil {
		return nil
	}
	if pre.Exists != nil {
		if *pre.Exists != exists {
			return newPreconditionErr("precondition failed: document existence does not match")
		}
		return nil
	}
	if pre.UpdateTime != nil {
		if !exists || !updateTime.Equal(*pre.UpdateTime) {
			return newPreconditionErr("precondition failed: updateTime does not match")
		}
	}
	return nil
}

// maskFields projects a document's fields down to the given field paths. A path
// not present in the document is omitted; "__name__" is always implicit and
// skipped (the name is carried on the wire independently).
func maskFields(doc firestorestore.Document, paths []string) map[string]*firestorestore.Value {
	out := map[string]*firestorestore.Value{}
	for _, fp := range paths {
		if fp == "__name__" {
			continue
		}
		if v := fieldValue(&doc, fp); v != nil {
			setFieldPath(out, strings.Split(fp, "."), v)
		}
	}
	return out
}

// applyMask merges body fields into a document's fields per Firestore patch
// semantics:
//   - no mask: the body fields fully replace the document's fields.
//   - mask present: only the field paths in the mask are updated; a mask path
//     not present in the body is deleted; fields outside the mask are unchanged.
//
// base is the existing fields (nil when the document is being created).
func applyMask(fields map[string]*firestorestore.Value, mask []string, base map[string]*firestorestore.Value) map[string]*firestorestore.Value {
	if len(mask) == 0 {
		out := make(map[string]*firestorestore.Value, len(fields))
		for k, v := range fields {
			out[k] = v
		}
		return out
	}
	out := make(map[string]*firestorestore.Value, len(base)+len(mask))
	for k, v := range base {
		out[k] = v
	}
	for _, fp := range mask {
		parts := strings.Split(fp, ".")
		if len(parts) == 1 {
			if v, ok := fields[parts[0]]; ok {
				out[parts[0]] = v
			} else {
				delete(out, parts[0])
			}
			continue
		}
		// Nested path: extract the leaf value from the body subtree.
		leaf := valueAt(fields[parts[0]], parts[1:])
		setFieldPath(out, parts, leaf)
	}
	return out
}

// valueAt returns the value at the dotted field path within a mapValue, or nil.
func valueAt(v *firestorestore.Value, path []string) *firestorestore.Value {
	cur := v
	for _, p := range path {
		if cur == nil || cur.MapValue == nil {
			return nil
		}
		cur = cur.MapValue.Fields[p]
	}
	return cur
}

// setFieldPath sets (or deletes, when val is nil) the field at the dotted path,
// creating intermediate mapValues as needed.
func setFieldPath(fields map[string]*firestorestore.Value, path []string, val *firestorestore.Value) {
	if len(path) == 1 {
		if val == nil {
			delete(fields, path[0])
		} else {
			fields[path[0]] = val
		}
		return
	}
	cur, ok := fields[path[0]]
	if !ok || cur == nil || cur.MapValue == nil {
		if val == nil {
			return // nothing to delete
		}
		cur = firestorestore.MapVal(map[string]*firestorestore.Value{})
		fields[path[0]] = cur
	}
	setFieldPath(cur.MapValue.Fields, path[1:], val)
}

// documentMap serialises a store Document to its REST wire form.
func documentMap(d firestorestore.Document) map[string]any {
	b, _ := json.Marshal(d)
	var m map[string]any
	json.Unmarshal(b, &m)
	return m
}

// ─── documents methods ────────────────────────────────────────────────────────

// DocumentsGet implements documents.get.
func (p *Provider) DocumentsGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	database, path, err := relativeName(nr)
	if err != nil {
		return nil, err
	}
	name := fullName(nr, database, path)
	doc, err := p.store.GetDocument(ctx, name)
	if err != nil {
		if txnID, _ := nr.Params["transaction"].(string); txnID != "" && errors.Is(err, firestorestore.ErrDocumentNotFound) {
			p.recordRead(txnID, name, firestorestore.Document{}, false)
		}
		return nil, mapStoreError(err)
	}
	if txnID, _ := nr.Params["transaction"].(string); txnID != "" {
		p.recordRead(txnID, name, doc, true)
	}
	if mask := maskFieldPaths(nr); len(mask) > 0 {
		doc.Fields = maskFields(doc, mask)
	}
	return provider.OK(documentMap(doc)), nil
}

// CreateDocument implements documents.createDocument. The document id comes
// from ?documentId=; when omitted the server generates a 20-character auto-ID
// and returns it in the response name.
func (p *Provider) CreateDocument(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	database, path, err := relativeName(nr)
	if err != nil {
		return nil, err
	}
	if err := validatePath(path); err != nil {
		return nil, err
	}

	docID, _ := nr.Params["documentId"].(string)
	if docID == "" {
		docID = randomID(autoIDLength)
	} else if err := validateDocumentID(docID); err != nil {
		return nil, err
	}

	name := fullName(nr, database, path+"/"+docID)

	body, _ := nr.Params["body"].(map[string]any)
	fields, err := extractFields(body)
	if err != nil {
		return nil, err
	}
	if err := validateFields(fields); err != nil {
		return nil, err
	}
	if err := checkSize(fields); err != nil {
		return nil, err
	}

	now := clock.Now()
	doc := firestorestore.Document{
		Name:       name,
		Fields:     fields,
		CreateTime: now,
		UpdateTime: now,
	}
	if err := p.store.CreateDocument(ctx, doc); err != nil {
		return nil, mapStoreError(err)
	}
	return provider.OK(documentMap(doc)), nil
}

// DocumentsPatch implements documents.patch. Patch upserts: a missing document
// is created. The updateMask (updateMask.fieldPaths) restricts which fields are
// updated; without it the body fields fully replace the document's fields.
func (p *Provider) DocumentsPatch(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	database, path, err := relativeName(nr)
	if err != nil {
		return nil, err
	}
	if err := validatePath(path); err != nil {
		return nil, err
	}
	name := fullName(nr, database, path)

	body, _ := nr.Params["body"].(map[string]any)
	fields, err := extractFields(body)
	if err != nil {
		return nil, err
	}
	if err := validateFields(fields); err != nil {
		return nil, err
	}

	mask := maskFieldPaths(nr)
	if len(mask) == 0 {
		mask = updateMaskPaths(nr)
	}
	pre, err := preconditionFromQuery(nr)
	if err != nil {
		return nil, err
	}
	now := clock.Now()

	existing, err := p.store.GetDocument(ctx, name)
	exists := err == nil
	if err != nil && !errors.Is(err, firestorestore.ErrDocumentNotFound) {
		return nil, err
	}

	// Optimistic precondition check (currentDocument.exists / updateTime).
	if exists {
		if err := checkPrecondition(true, existing.UpdateTime, pre); err != nil {
			return nil, err
		}
	} else if err := checkPrecondition(false, time.Time{}, pre); err != nil {
		return nil, err
	}

	var base map[string]*firestorestore.Value
	createTime := now
	if exists {
		base = existing.Fields
		createTime = existing.CreateTime
	}
	merged := applyMask(fields, mask, base)
	if err := checkSize(merged); err != nil {
		return nil, err
	}
	doc := firestorestore.Document{
		Name:       name,
		Fields:     merged,
		CreateTime: createTime,
		UpdateTime: now,
	}
	if exists {
		if err := p.store.UpdateDocument(ctx, doc); err != nil {
			return nil, mapStoreError(err)
		}
	} else {
		if err := p.store.CreateDocument(ctx, doc); err != nil {
			return nil, mapStoreError(err)
		}
	}
	return provider.OK(documentMap(doc)), nil
}

// DocumentsDelete implements documents.delete. Idempotent, unless a
// currentDocument precondition is set (then it is validated first).
func (p *Provider) DocumentsDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	database, path, err := relativeName(nr)
	if err != nil {
		return nil, err
	}
	name := fullName(nr, database, path)

	pre, err := preconditionFromQuery(nr)
	if err != nil {
		return nil, err
	}
	if pre != nil {
		existing, err := p.store.GetDocument(ctx, name)
		exists := err == nil
		if err != nil && !errors.Is(err, firestorestore.ErrDocumentNotFound) {
			return nil, err
		}
		var updateTime time.Time
		if exists {
			updateTime = existing.UpdateTime
		}
		if err := checkPrecondition(exists, updateTime, pre); err != nil {
			return nil, err
		}
	}

	if err := p.store.DeleteDocument(ctx, name); err != nil {
		return nil, mapStoreError(err)
	}
	return provider.OK(map[string]any{}), nil
}
