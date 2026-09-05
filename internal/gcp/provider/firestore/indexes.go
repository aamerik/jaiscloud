package firestore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"

	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const rtIndex = "firestore_index"

// indexDef is the persisted GoogleFirestoreAdminV1Index shape (minimal: name,
// queryScope, fields). Composite indexes only; single-field indexes are
// implicit and never registered.
type indexDef struct {
	Name       string       `json:"name,omitempty"`
	QueryScope string       `json:"queryScope,omitempty"`
	Fields     []indexField `json:"fields,omitempty"`
}

type indexField struct {
	FieldPath   string `json:"fieldPath,omitempty"`
	Order       string `json:"order,omitempty"`
	ArrayConfig string `json:"arrayConfig,omitempty"`
}

// indexPath parses an index resource name (relative, after the project):
// "databases/{db}/collectionGroups/{cg}/indexes" (5 segments) or
// "databases/{db}/collectionGroups/{cg}/indexes/{id}" (6 segments).
func indexPath(name string) (database, cg, id string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) < 5 || parts[0] != "databases" || parts[2] != "collectionGroups" || parts[4] != "indexes" {
		return "", "", "", false
	}
	database, cg = parts[1], parts[3]
	if len(parts) >= 6 {
		id = parts[5]
	}
	return database, cg, id, true
}

// fullIndexName builds the full index resource name.
func fullIndexName(nr *model.NormalizedRequest, database, cg, id string) string {
	return "projects/" + nr.AccountID + "/databases/" + database + "/collectionGroups/" + cg + "/indexes/" + id
}

// indexRelName builds the resource-store ID (relative index name).
func indexRelName(database, cg, id string) string {
	return "databases/" + database + "/collectionGroups/" + cg + "/indexes/" + id
}

// hasIndex reports whether a composite index covering the ordered required
// field paths exists for the database.
func (p *Provider) hasIndex(ctx context.Context, project, database string, required []string) (bool, error) {
	if p.resources == nil {
		return false, nil
	}
	entries, err := p.resources.List(ctx, project, store.GlobalRegion, rtIndex, "databases/"+database+"/")
	if err != nil {
		return false, err
	}
	for _, e := range entries {
		var idx indexDef
		if json.Unmarshal(e.Data, &idx) != nil {
			continue
		}
		var paths []string
		for _, f := range idx.Fields {
			if f.FieldPath != "__name__" {
				paths = append(paths, f.FieldPath)
			}
		}
		if len(paths) < len(required) {
			continue
		}
		match := true
		for i, r := range required {
			if paths[i] != r {
				match = false
				break
			}
		}
		if match {
			return true, nil
		}
	}
	return false, nil
}

// CreateIndex implements projects.databases.collectionGroups.indexes.create.
func (p *Provider) CreateIndex(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["name"].(string)
	database, cg, _, ok := indexPath(name)
	if !ok {
		return nil, model.NewProviderError("InvalidArgument", "invalid index parent path", 400)
	}

	body, _ := nr.Params["body"].(map[string]any)
	b, _ := json.Marshal(body)
	var idx indexDef
	if err := json.Unmarshal(b, &idx); err != nil {
		return nil, model.NewProviderError("InvalidArgument", "malformed index definition", 400)
	}
	if len(idx.Fields) < 2 {
		return nil, model.NewProviderError("InvalidArgument", "a composite index requires at least 2 fields", 400)
	}

	id := randomHex(12)
	idx.Name = fullIndexName(nr, database, cg, id)
	if idx.QueryScope == "" {
		idx.QueryScope = "COLLECTION"
	}
	data, _ := json.Marshal(idx)

	if p.resources != nil {
		if err := p.resources.Create(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtIndex, ID: indexRelName(database, cg, id), Data: data}); err != nil {
			if errors.Is(err, store.ErrAlreadyExists) {
				return nil, model.NewProviderError("AlreadyExists", "index already exists", 409)
			}
			return nil, err
		}
	}
	// CreateIndex returns a google.longrunning.Operation wrapping the created
	// index (done=true), not the bare index body.
	opID := randomHex(12)
	opName := "projects/" + nr.AccountID + "/databases/" + database + "/operations/" + opID
	return provider.OK(map[string]any{
		"name":     opName,
		"done":     true,
		"response": indexMap(idx),
	}), nil
}

// ListIndexes implements projects.databases.collectionGroups.indexes.list.
func (p *Provider) ListIndexes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["name"].(string)
	database, cg, _, ok := indexPath(name)
	if !ok {
		return nil, model.NewProviderError("InvalidArgument", "invalid index parent path", 400)
	}
	prefix := "databases/" + database + "/collectionGroups/" + cg + "/indexes/"
	var idxs []indexDef
	if p.resources != nil {
		entries, err := p.resources.List(ctx, nr.AccountID, store.GlobalRegion, rtIndex, prefix)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			var idx indexDef
			if json.Unmarshal(e.Data, &idx) == nil {
				idxs = append(idxs, idx)
			}
		}
	}
	// Optional `filter` query parameter: basic substring match on fieldPath.
	if f, _ := nr.Params["filter"].(string); f != "" {
		kept := idxs[:0]
		for _, idx := range idxs {
			if indexMatchesFilter(idx, f) {
				kept = append(kept, idx)
			}
		}
		idxs = kept
	}

	page, nextToken := paging.Page(idxs, func(d indexDef) string { return d.Name }, nr.Params)
	items := make([]any, 0, len(page))
	for _, idx := range page {
		items = append(items, indexMap(idx))
	}
	resp := map[string]any{"indexes": items}
	if nextToken != "" {
		resp["nextPageToken"] = nextToken
	}
	return provider.OK(resp), nil
}

// GetIndex implements projects.databases.collectionGroups.indexes.get.
func (p *Provider) GetIndex(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["name"].(string)
	database, cg, id, ok := indexPath(name)
	if !ok || id == "" {
		return nil, model.NewProviderError("InvalidArgument", "invalid index path", 400)
	}
	rel := indexRelName(database, cg, id)
	if p.resources == nil {
		return nil, model.NewProviderError("NotFound", "index not found", 404)
	}
	e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtIndex, rel)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "index not found", 404)
		}
		return nil, err
	}
	var idx indexDef
	_ = json.Unmarshal(e.Data, &idx)
	return provider.OK(indexMap(idx)), nil
}

// DeleteIndex implements projects.databases.collectionGroups.indexes.delete.
func (p *Provider) DeleteIndex(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["name"].(string)
	database, cg, id, ok := indexPath(name)
	if !ok || id == "" {
		return nil, model.NewProviderError("InvalidArgument", "invalid index path", 400)
	}
	if p.resources == nil {
		return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
	}
	if err := p.resources.Delete(ctx, nr.AccountID, store.GlobalRegion, rtIndex, indexRelName(database, cg, id)); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "index not found", 404)
		}
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

// indexMap renders an indexDef to its REST wire form.
func indexMap(idx indexDef) map[string]any {
	fields := make([]any, 0, len(idx.Fields))
	for _, f := range idx.Fields {
		m := map[string]any{"fieldPath": f.FieldPath}
		if f.Order != "" {
			m["order"] = f.Order
		}
		if f.ArrayConfig != "" {
			m["arrayConfig"] = f.ArrayConfig
		}
		fields = append(fields, m)
	}
	return map[string]any{
		"name":       idx.Name,
		"queryScope": idx.QueryScope,
		"state":      "READY",
		"fields":     fields,
	}
}

// indexMatchesFilter reports whether any index field path contains the filter
// substring (the ListIndexes `filter` parameter).
func indexMatchesFilter(idx indexDef, filter string) bool {
	for _, f := range idx.Fields {
		if strings.Contains(f.FieldPath, filter) {
			return true
		}
	}
	return false
}

// randomHex returns a random hex string of the given length.
func randomHex(n int) string {
	b := make([]byte, (n+1)/2)
	if _, err := rand.Read(b); err != nil {
		return strings.Repeat("0", n)
	}
	return hex.EncodeToString(b)[:n]
}
