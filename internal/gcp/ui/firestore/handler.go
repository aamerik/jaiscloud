package firestoreui

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"jaiscloud/internal/config"
	"jaiscloud/internal/gcp/ui/uihelper"
)

// defaultDatabase is the Firestore default database id.
const defaultDatabase = "(default)"

const documentsPrefix = "databases/" + defaultDatabase + "/documents"

// Handler serves Firestore UI API requests by calling the Firestore provider.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// account resolves the project for a request, falling back to the configured
// project when the inject-config middleware has not populated the context.
func (h *Handler) account(r *http.Request) string {
	if a := uihelper.AccountFrom(r); a != "" {
		return a
	}
	return h.cfg.AccountID
}

// ─── mapping helpers ─────────────────────────────────────────────────────────

func str(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}

func lastSegment(name string) string {
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		return name[i+1:]
	}
	return name
}

// segment returns the decoded, single-segment value of a URL path parameter.
// A decoded '/' (from a %2F escape) is rejected: collection and document IDs
// are single path segments.
func segmentParam(r *http.Request, key string) (string, bool) {
	v := uihelper.PathParam(r, key)
	if v == "" || strings.Contains(v, "/") {
		return "", false
	}
	return v, true
}

// documentFromMap converts a provider REST document map into the UI shape. The
// raw Firestore value encoding is passed through unchanged.
func documentFromMap(m map[string]any, collection string) Document {
	full := str(m, "name")
	fields, _ := m["fields"].(map[string]any)
	if fields == nil {
		fields = map[string]any{}
	}
	return Document{
		ID:         lastSegment(full),
		Name:       full,
		Collection: collection,
		Fields:     fields,
		CreateTime: str(m, "createTime"),
		UpdateTime: str(m, "updateTime"),
	}
}

// decodeBody decodes exactly one JSON object from the request body, rejecting
// trailing data. Numbers are left as json.Number for pass-through fidelity.
func decodeBody(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return errors.New("unexpected trailing data in request body")
	}
	return nil
}

func (h *Handler) collectionPath(collection string) string {
	return documentsPrefix + "/" + collection
}

func (h *Handler) documentPath(collection, document string) string {
	return h.collectionPath(collection) + "/" + document
}

// ─── Collections ─────────────────────────────────────────────────────────────

// GET /collections
func (h *Handler) ListCollections(w http.ResponseWriter, r *http.Request) {
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "firestore", "Firestore.ListCollectionIds", "global", account)
	nr.Params["name"] = documentsPrefix
	pageParams(r, nr.Params)

	resp, err := h.provider.ListCollectionIds(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	raw := uihelper.AsSlice(resp.Data["collectionIds"])
	collections := make([]Collection, 0, len(raw))
	for _, item := range raw {
		if id, ok := item.(string); ok {
			collections = append(collections, Collection{ID: id})
		}
	}
	next, _ := resp.Data["nextPageToken"].(string)
	uihelper.WriteJSON(w, ListCollectionsResponse{Collections: collections, Total: len(collections), NextPageToken: next})
}

// ─── Documents ───────────────────────────────────────────────────────────────

// GET /collections/{collection}/documents
func (h *Handler) ListDocuments(w http.ResponseWriter, r *http.Request) {
	collection, ok := segmentParam(r, "collection")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid collection id", http.StatusBadRequest)
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "firestore", "Firestore.ListDocuments", "global", account)
	nr.Params["name"] = h.collectionPath(collection)
	pageParams(r, nr.Params)

	resp, err := h.provider.ListDocuments(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	raw := uihelper.AsSlice(resp.Data["documents"])
	docs := make([]Document, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			docs = append(docs, documentFromMap(m, collection))
		}
	}
	next, _ := resp.Data["nextPageToken"].(string)
	uihelper.WriteJSON(w, ListDocumentsResponse{Documents: docs, Total: len(docs), NextPageToken: next})
}

// POST /collections/{collection}/documents  body: { documentId?, fields }
func (h *Handler) CreateDocument(w http.ResponseWriter, r *http.Request) {
	var req CreateDocumentRequest
	if err := decodeBody(r, &req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	collection, ok := segmentParam(r, "collection")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid collection id", http.StatusBadRequest)
		return
	}

	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "firestore", "Firestore.CreateDocument", "global", account)
	nr.Params["name"] = h.collectionPath(collection)
	if req.DocumentID != "" {
		if strings.Contains(req.DocumentID, "/") {
			uihelper.UIError(w, "BadRequest", "document id must not contain '/'", http.StatusBadRequest)
			return
		}
		nr.Params["documentId"] = req.DocumentID
	}
	nr.Params["body"] = map[string]any{"fields": req.Fields}

	resp, err := h.provider.CreateDocument(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, documentFromMap(resp.Data, collection))
}

// GET /collections/{collection}/documents/{document}
func (h *Handler) GetDocument(w http.ResponseWriter, r *http.Request) {
	collection, ok := segmentParam(r, "collection")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid collection id", http.StatusBadRequest)
		return
	}
	document, ok := segmentParam(r, "document")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid document id", http.StatusBadRequest)
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "firestore", "Firestore.GetDocument", "global", account)
	nr.Params["name"] = h.documentPath(collection, document)

	resp, err := h.provider.DocumentsGet(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, documentFromMap(resp.Data, collection))
}

// PATCH /collections/{collection}/documents/{document}  body: { fields, updateTime? }
func (h *Handler) UpdateDocument(w http.ResponseWriter, r *http.Request) {
	var req UpdateDocumentRequest
	if err := decodeBody(r, &req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Fields == nil {
		uihelper.UIError(w, "BadRequest", "fields is required", http.StatusBadRequest)
		return
	}
	collection, ok := segmentParam(r, "collection")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid collection id", http.StatusBadRequest)
		return
	}
	document, ok := segmentParam(r, "document")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid document id", http.StatusBadRequest)
		return
	}

	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "firestore", "Firestore.PatchDocument", "global", account)
	nr.Params["name"] = h.documentPath(collection, document)
	// No updateMask: the console editor holds the whole document, so the body
	// fields fully replace the stored fields (provider applyMask semantics).
	nr.Params["body"] = map[string]any{"fields": req.Fields}
	// Optimistic concurrency: fail rather than overwrite a concurrently changed
	// (or deleted) document when the caller passes the observed updateTime.
	if req.UpdateTime != "" {
		nr.Params["currentDocument.updateTime"] = req.UpdateTime
	}

	resp, err := h.provider.DocumentsPatch(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, documentFromMap(resp.Data, collection))
}

// DELETE /collections/{collection}/documents/{document}
func (h *Handler) DeleteDocument(w http.ResponseWriter, r *http.Request) {
	collection, ok := segmentParam(r, "collection")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid collection id", http.StatusBadRequest)
		return
	}
	document, ok := segmentParam(r, "document")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid document id", http.StatusBadRequest)
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "firestore", "Firestore.DeleteDocument", "global", account)
	nr.Params["name"] = h.documentPath(collection, document)

	if _, err := h.provider.DocumentsDelete(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// pageParams copies pageSize/pageToken query parameters onto the request params.
func pageParams(r *http.Request, nr map[string]any) {
	if v := r.URL.Query().Get("pageSize"); v != "" {
		nr["pageSize"] = v
	}
	if v := r.URL.Query().Get("pageToken"); v != "" {
		nr["pageToken"] = v
	}
}
