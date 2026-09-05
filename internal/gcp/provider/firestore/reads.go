package firestore

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	firestorestore "jaiscloud/internal/gcp/store/firestore"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// ListDocuments implements documents.list / documents.listDocuments (they share
// one wire path: GET on a collection). Returns the direct children of the
// collection, paginated by pageToken/pageSize.
func (p *Provider) ListDocuments(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	database, path, err := relativeName(nr)
	if err != nil {
		return nil, err
	}
	project := nr.AccountID
	docs, err := p.store.ListDocuments(ctx, project, database)
	if err != nil {
		return nil, err
	}
	coll := fullName(nr, database, path)
	children := make([]firestorestore.Document, 0)
	for _, d := range docs {
		if d.ParentPath == coll {
			children = append(children, d)
		}
	}
	sort.Slice(children, func(i, j int) bool { return children[i].Name < children[j].Name })

	page, nextToken := paging.Page(children, func(d firestorestore.Document) string { return d.Name }, nr.Params)

	if txnID, _ := nr.Params["transaction"].(string); txnID != "" {
		for _, d := range page {
			p.recordRead(txnID, d.Name, d, true)
		}
	}

	mask := maskFieldPaths(nr)
	items := make([]any, 0, len(page))
	for _, d := range page {
		if len(mask) > 0 {
			d.Fields = maskFields(d, mask)
		}
		items = append(items, documentMap(d))
	}
	resp := map[string]any{"documents": items}
	if nextToken != "" {
		resp["nextPageToken"] = nextToken
	}
	return provider.OK(resp), nil
}

// ListCollectionIds implements documents.listCollectionIds: returns the distinct
// subcollection IDs of a document.
func (p *Provider) ListCollectionIds(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	database, path, err := relativeName(nr)
	if err != nil {
		return nil, err
	}
	project := nr.AccountID
	parent := fullName(nr, database, path)

	docs, err := p.store.ListDocuments(ctx, project, database)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var ids []string
	for _, d := range docs {
		if parentDocOfCollection(d.ParentPath) != parent {
			continue
		}
		if d.CollectionID == "" || seen[d.CollectionID] {
			continue
		}
		seen[d.CollectionID] = true
		ids = append(ids, d.CollectionID)
	}
	sort.Strings(ids)

	page, nextToken := paging.Page(ids, func(s string) string { return s }, nr.Params)
	resp := map[string]any{"collectionIds": page}
	if nextToken != "" {
		resp["nextPageToken"] = nextToken
	}
	return provider.OK(resp), nil
}

// parentDocOfCollection returns the document path that owns a collection path
// (strip the trailing collection-id segment).
func parentDocOfCollection(collPath string) string {
	i := strings.LastIndex(collPath, "/")
	if i < 0 {
		return ""
	}
	return collPath[:i]
}

// RunQuery implements documents.runQuery over a StructuredQuery.
func (p *Provider) RunQuery(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	var req runQueryRequestWire
	if err := decodeBody(nr, &req); err != nil {
		return nil, err
	}
	if req.StructuredQuery == nil {
		return nil, model.NewProviderError("InvalidArgument", "runQuery requires a structuredQuery", 400)
	}

	database, path, err := relativeName(nr)
	if err != nil {
		return nil, err
	}
	project := nr.AccountID
	// The runQuery parent is the resource path before :runQuery
	// (".../documents" or ".../documents/{doc path}"); from[].collectionId is
	// relative to it.
	parent := "projects/" + project + "/databases/" + database + "/documents"
	if path != "" {
		parent += "/" + path
	}

	// Composite-index enforcement (strict): reject queries that require a
	// missing composite index before executing.
	required, err := analyzeQuery(req.StructuredQuery)
	if err != nil {
		return nil, err
	}
	if len(required) > 0 {
		ok, err := p.hasIndex(ctx, project, database, required)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, newPreconditionErr("the query requires a composite index on " +
				strings.Join(required, ", ") + " which is not defined")
		}
	}

	docs, err := p.store.ListDocuments(ctx, project, database)
	if err != nil {
		return nil, err
	}
	docPtrs := make([]*firestorestore.Document, len(docs))
	for i := range docs {
		docPtrs[i] = &docs[i]
	}
	results, err := executeQuery(docPtrs, req.StructuredQuery, parent)
	if err != nil {
		return nil, err
	}

	// Server-streaming responses are newline-delimited JSON: one RunQueryResponse
	// object per line, terminated by a {"done":true} line.
	readTime := clock.Now()
	lines := make([]string, 0, len(results)+1)
	for _, d := range results {
		if req.Transaction != "" {
			p.recordRead(req.Transaction, d.Name, *d, true)
		}
		item := map[string]any{
			"document": documentMap(*d),
			"readTime": readTime.Format(time.RFC3339Nano),
		}
		b, err := json.Marshal(item)
		if err != nil {
			return nil, err
		}
		lines = append(lines, string(b))
	}
	final := map[string]any{"done": true}
	if req.StructuredQuery.Offset > 0 {
		final["skippedResults"] = req.StructuredQuery.Offset
	}
	b, err := json.Marshal(final)
	if err != nil {
		return nil, err
	}
	lines = append(lines, string(b))
	return provider.OK(map[string]any{wire.RawJSONKey: json.RawMessage(strings.Join(lines, "\n"))}), nil
}
