// Package storage implements the Google Cloud Storage provider (buckets and
// objects) on top of the shared ResourceStore (metadata) and BlobStore (bytes).
package storage

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// Resource types used in the ResourceStore.
const (
	rtBucket    = "gcs_bucket"
	rtObject    = "gcs_object"
	rtBucketIAM = "gcs_bucket_iam"
	rtACL       = "gcs_acl"
)

// blobsNamespace is the BlobStore namespace ("bucket") used for GCS object bytes.
const blobsNamespace = "gcs"

// maxUploadSessions caps concurrent resumable-upload sessions (DoS guard).
const maxUploadSessions = 1000

// Provider implements the GCS JSON + media API.
type Provider struct {
	resources store.ResourceStore
	blobs     blobfs.BlobStore

	mu      sync.Mutex
	uploads map[string]*uploadSession // resumable upload sessions (in-memory)

	genMu sync.Mutex
	gen   int64 // monotonically-increasing object generation counter
}

// uploadSession holds the state of an in-progress resumable upload.
type uploadSession struct {
	Bucket      string
	Object      string
	ContentType string
	buf         []byte // accumulated bytes across chunks
}

// New returns a GCS provider backed by the given metadata and blob stores.
func New(resources store.ResourceStore, blobs blobfs.BlobStore) *Provider {
	p := &Provider{
		resources: resources,
		blobs:     blobs,
		uploads:   make(map[string]*uploadSession),
		gen:       clock.Now().UnixNano(),
	}
	p.seedGeneration(context.Background())
	return p
}

// seedGeneration bumps the generation counter past the highest stored
// generation so generations remain monotonic across restarts (--dsn).
func (p *Provider) seedGeneration(ctx context.Context) {
	entries, err := p.resources.List(ctx, "", "", rtObject, "")
	if err != nil {
		return
	}
	var maxGen int64
	for _, e := range entries {
		var o objectMeta
		if json.Unmarshal(e.Data, &o) != nil {
			continue
		}
		if g, err := strconv.ParseInt(o.Generation, 10, 64); err == nil && g > maxGen {
			maxGen = g
		}
	}
	p.genMu.Lock()
	if maxGen >= p.gen {
		p.gen = maxGen
	}
	p.genMu.Unlock()
}

// nextGen returns a unique, monotonically-increasing object generation.
func (p *Provider) nextGen() string {
	p.genMu.Lock()
	p.gen++
	g := p.gen
	p.genMu.Unlock()
	return strconv.FormatInt(g, 10)
}

// Reset clears in-progress resumable-upload sessions. Implements admin.Resetter
// so /_jaiscloud/reset does not leak upload state across test runs.
func (p *Provider) Reset(_ context.Context) {
	p.mu.Lock()
	p.uploads = make(map[string]*uploadSession)
	p.mu.Unlock()
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Storage.BucketsList":                 p.BucketsList,
		"Storage.BucketsInsert":               p.BucketsInsert,
		"Storage.BucketsGet":                  p.BucketsGet,
		"Storage.BucketsUpdate":               p.BucketsUpdate,
		"Storage.BucketsDelete":               p.BucketsDelete,
		"Storage.BucketsGetIamPolicy":         p.BucketsGetIamPolicy,
		"Storage.BucketsSetIamPolicy":         p.BucketsSetIamPolicy,
		"Storage.BucketACLList":               p.BucketACLList,
		"Storage.BucketACLInsert":             p.BucketACLInsert,
		"Storage.ObjectsList":                 p.ObjectsList,
		"Storage.ObjectsInsert":               p.ObjectsInsert,
		"Storage.ObjectsGet":                  p.ObjectsGet,
		"Storage.ObjectsGetMedia":             p.ObjectsGetMedia,
		"Storage.ObjectsUpdate":               p.ObjectsUpdate,
		"Storage.ObjectsDelete":               p.ObjectsDelete,
		"Storage.ObjectACLList":               p.ObjectACLList,
		"Storage.ObjectACLInsert":             p.ObjectACLInsert,
		"Storage.ObjectsInsertStartResumable": p.ObjectsInsertStartResumable,
		"Storage.ObjectsInsertResumable":      p.ObjectsInsertResumable,
	}
}

// ─── metadata types ───────────────────────────────────────────────────────────

type bucketMeta struct {
	Name         string `json:"name"`
	Location     string `json:"location,omitempty"`
	StorageClass string `json:"storageClass,omitempty"`
	TimeCreated  string `json:"timeCreated,omitempty"`
	Updated      string `json:"updated,omitempty"`
}

type objectMeta struct {
	Kind           string            `json:"kind"`
	ID             string            `json:"id,omitempty"`
	Name           string            `json:"name"`
	Bucket         string            `json:"bucket"`
	Size           string            `json:"size,omitempty"`
	ContentType    string            `json:"contentType,omitempty"`
	Md5Hash        string            `json:"md5Hash,omitempty"`
	Crc32c         string            `json:"crc32c,omitempty"`
	Etag           string            `json:"etag,omitempty"`
	SelfLink       string            `json:"selfLink,omitempty"`
	MediaLink      string            `json:"mediaLink,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	Generation     string            `json:"generation,omitempty"`
	Metageneration string            `json:"metageneration,omitempty"`
	StorageClass   string            `json:"storageClass,omitempty"`
	TimeCreated    string            `json:"timeCreated,omitempty"`
	Updated        string            `json:"updated,omitempty"`
}

// ─── buckets ──────────────────────────────────────────────────────────────────

func (p *Provider) BucketsList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, nr.AccountID, store.GlobalRegion, rtBucket, "")
	if err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].ID < entries[j].ID })

	page, nextToken := paginateEntries(entries, nr.Params)
	items := make([]any, 0, len(page))
	for _, e := range page {
		var b bucketMeta
		if json.Unmarshal(e.Data, &b) == nil {
			items = append(items, toBucketMap(b))
		}
	}
	resp := map[string]any{"kind": "storage#buckets", "items": items}
	if nextToken != "" {
		resp["nextPageToken"] = nextToken
	}
	return provider.OK(resp), nil
}

func (p *Provider) BucketsInsert(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body, _ := nr.Params["body"].(map[string]any)
	name, _ := body["name"].(string)
	if name == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing bucket name", 400)
	}
	b := bucketMeta{Name: name}
	if loc, _ := body["location"].(string); loc != "" {
		b.Location = loc
	} else {
		b.Location = "US"
	}
	if sc, _ := body["storageClass"].(string); sc != "" {
		b.StorageClass = sc
	} else {
		b.StorageClass = "STANDARD"
	}
	b.TimeCreated = clock.Now().Format(time.RFC3339Nano)
	b.Updated = b.TimeCreated

	data, _ := json.Marshal(b)
	if err := p.resources.Create(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtBucket, ID: name, Data: data}); err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil, model.NewProviderError("Conflict", "bucket already exists", 409)
		}
		return nil, err
	}
	return provider.OK(toBucketMap(b)), nil
}

func (p *Provider) BucketsGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["bucket"].(string)
	e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtBucket, name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "bucket not found", 404)
		}
		return nil, err
	}
	var b bucketMeta
	json.Unmarshal(e.Data, &b)
	return provider.OK(toBucketMap(b)), nil
}

func (p *Provider) BucketsUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["bucket"].(string)
	e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtBucket, name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "bucket not found", 404)
		}
		return nil, err
	}
	var b bucketMeta
	json.Unmarshal(e.Data, &b)
	// Preserve timeCreated; update only fields present in the request body.
	body, _ := nr.Params["body"].(map[string]any)
	if loc, _ := body["location"].(string); loc != "" {
		b.Location = loc
	}
	if sc, _ := body["storageClass"].(string); sc != "" {
		b.StorageClass = sc
	}
	b.Updated = clock.Now().Format(time.RFC3339Nano)
	data, _ := json.Marshal(b)
	if err := p.resources.Update(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtBucket, ID: name, Data: data}); err != nil {
		return nil, err
	}
	return provider.OK(toBucketMap(b)), nil
}

func (p *Provider) BucketsDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["bucket"].(string)
	// A non-empty bucket cannot be deleted (409), matching real GCS. The store's
	// List prefix is a substring match, so filter to the true "name/" prefix to
	// avoid counting objects from sibling buckets whose names contain "name/".
	objects, err := p.resources.List(ctx, nr.AccountID, store.GlobalRegion, rtObject, name+"/")
	if err != nil {
		return nil, err
	}
	for _, e := range objects {
		if strings.HasPrefix(e.ID, name+"/") {
			return nil, model.NewProviderError("bucketNotEmpty", "bucket is not empty", 409)
		}
	}
	if err := p.resources.Delete(ctx, nr.AccountID, store.GlobalRegion, rtBucket, name); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "bucket not found", 404)
		}
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

// ─── objects ──────────────────────────────────────────────────────────────────

func (p *Provider) ObjectsList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket, _ := nr.Params["bucket"].(string)
	if bucket == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing bucket", 400)
	}
	prefix := bucket + "/"
	entries, err := p.resources.List(ctx, nr.AccountID, store.GlobalRegion, rtObject, prefix)
	if err != nil {
		return nil, err
	}
	// The store's List prefix is a substring match; keep only true "bucket/"
	// prefixed IDs so sibling buckets (e.g. "abkt") don't leak in.
	filtered := entries[:0]
	for _, e := range entries {
		if strings.HasPrefix(e.ID, prefix) {
			filtered = append(filtered, e)
		}
	}
	entries = filtered
	sort.Slice(entries, func(i, j int) bool { return entries[i].ID < entries[j].ID })

	pfx, _ := nr.Params["prefix"].(string)
	delim, _ := nr.Params["delimiter"].(string)

	// With a delimiter, group objects into common prefixes (folders).
	if delim != "" {
		items := make([]any, 0)
		prefixes := make([]string, 0)
		seen := map[string]bool{}
		for _, e := range entries {
			name := strings.TrimPrefix(e.ID, prefix)
			if !strings.HasPrefix(name, pfx) {
				continue
			}
			rest := name[len(pfx):]
			if i := strings.Index(rest, delim); i >= 0 {
				common := pfx + rest[:i+len(delim)]
				if !seen[common] {
					seen[common] = true
					prefixes = append(prefixes, common)
				}
				continue
			}
			var o objectMeta
			if json.Unmarshal(e.Data, &o) == nil {
				o.Kind = "storage#object"
				items = append(items, toMap(o))
			}
		}
		sort.Strings(prefixes)
		resp := map[string]any{"kind": "storage#objects", "items": items}
		if len(prefixes) > 0 {
			resp["prefixes"] = prefixes
		}
		return provider.OK(resp), nil
	}

	// No delimiter: prefix filter + pagination.
	pfxFiltered := entries[:0]
	for _, e := range entries {
		if pfx == "" || strings.HasPrefix(strings.TrimPrefix(e.ID, prefix), pfx) {
			pfxFiltered = append(pfxFiltered, e)
		}
	}
	entries = pfxFiltered

	page, nextToken := paginateEntries(entries, nr.Params)
	items := make([]any, 0, len(page))
	for _, e := range page {
		var o objectMeta
		if json.Unmarshal(e.Data, &o) == nil {
			o.Kind = "storage#object"
			items = append(items, toMap(o))
		}
	}
	resp := map[string]any{"kind": "storage#objects", "items": items}
	if nextToken != "" {
		resp["nextPageToken"] = nextToken
	}
	return provider.OK(resp), nil
}

func (p *Provider) ObjectsInsert(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket, _ := nr.Params["bucket"].(string)
	if bucket == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing bucket", 400)
	}
	if _, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtBucket, bucket); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "bucket not found", 404)
		}
		return nil, err
	}

	object, _ := nr.Params["object"].(string)
	if body, ok := nr.Params["body"].(map[string]any); ok && object == "" {
		if n, _ := body["name"].(string); n != "" {
			object = n
		}
	}
	if object == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing object name", 400)
	}

	media, _ := nr.Params[wire.MediaKey].([]byte)
	contentType, _ := nr.Params[wire.ContentTypeKey].(string)
	if contentType == "" {
		if body, ok := nr.Params["body"].(map[string]any); ok {
			contentType, _ = body["contentType"].(string)
		}
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	now := clock.Now()
	id := bucket + "/" + object
	o := objectMeta{
		Kind:           "storage#object",
		Name:           object,
		Bucket:         bucket,
		Size:           fmt.Sprintf("%d", len(media)),
		ContentType:    contentType,
		Generation:     p.nextGen(),
		Metageneration: "1",
		StorageClass:   "STANDARD",
		TimeCreated:    now.Format(time.RFC3339Nano),
		Updated:        now.Format(time.RFC3339Nano),
	}
	if body, ok := nr.Params["body"].(map[string]any); ok {
		if md, ok := body["metadata"].(map[string]any); ok {
			o.Metadata = make(map[string]string, len(md))
			for k, v := range md {
				if s, ok := v.(string); ok {
					o.Metadata[k] = s
				}
			}
		}
	}
	o.ID = bucket + "/" + object + "/" + o.Generation
	o.Etag = "CAE="
	o.SelfLink = "https://www.googleapis.com/storage/v1/b/" + bucket + "/o/" + url.PathEscape(object)
	o.MediaLink = "https://www.googleapis.com/download/storage/v1/b/" + bucket + "/o/" + url.PathEscape(object) + "?alt=media"

	if stream, ok := nr.Params[wire.StreamKey].(io.Reader); ok {
		// Streaming upload: hash on the fly while writing to the blob store, so
		// large objects never buffer fully in memory.
		md5h := md5.New()
		crc32h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
		tee := io.TeeReader(io.TeeReader(stream, md5h), crc32h)
		n, err := p.blobs.PutStream(ctx, blobsNamespace, id, tee)
		if err != nil {
			return nil, err
		}
		o.Size = strconv.FormatInt(n, 10)
		o.Md5Hash = base64.StdEncoding.EncodeToString(md5h.Sum(nil))
		crcBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(crcBytes, crc32h.Sum32())
		o.Crc32c = base64.StdEncoding.EncodeToString(crcBytes)
	} else {
		// md5Hash is present even for zero-length objects; crc32c is CRC32C-Castagnoli.
		sum := md5.Sum(media)
		o.Md5Hash = base64.StdEncoding.EncodeToString(sum[:])
		crc := crc32.Checksum(media, crc32.MakeTable(crc32.Castagnoli))
		crcBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(crcBytes, crc)
		o.Crc32c = base64.StdEncoding.EncodeToString(crcBytes)
		if err := p.blobs.Put(ctx, blobsNamespace, id, media); err != nil {
			return nil, err
		}
	}

	data, _ := json.Marshal(o)
	if err := p.resources.Create(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtObject, ID: id, Data: data}); err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			_ = p.resources.Update(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtObject, ID: id, Data: data})
		} else {
			// Roll back the just-written blob so a failed metadata create does
			// not leave an orphaned object (metadata absent, blob present).
			_ = p.blobs.Delete(ctx, blobsNamespace, id)
			return nil, err
		}
	}
	return provider.OK(toMap(o)), nil
}

func (p *Provider) ObjectsGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.objectResponse(ctx, nr)
}

func (p *Provider) ObjectsGetMedia(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket, _ := nr.Params["bucket"].(string)
	object, _ := nr.Params["object"].(string)
	id := bucket + "/" + object
	// Metadata first: metadata gone → 404; metadata present + blob absent → 500.
	e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtObject, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "object not found", 404)
		}
		return nil, err
	}
	rc, err := p.blobs.GetStream(ctx, blobsNamespace, id, 0, -1)
	if err != nil {
		return nil, fmt.Errorf("gcs: object metadata present but blob missing: %w", err)
	}
	var o objectMeta
	json.Unmarshal(e.Data, &o)
	return &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{
			"_stream":           rc,
			wire.ContentTypeKey: o.ContentType,
		},
	}, nil
}

func (p *Provider) ObjectsUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket, _ := nr.Params["bucket"].(string)
	object, _ := nr.Params["object"].(string)
	id := bucket + "/" + object
	e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtObject, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "object not found", 404)
		}
		return nil, err
	}
	// Preserve size/md5Hash/generation/storageClass/timeCreated; update only
	// the fields present in the request and bump metageneration.
	var o objectMeta
	json.Unmarshal(e.Data, &o)
	if body, ok := nr.Params["body"].(map[string]any); ok {
		if ct, _ := body["contentType"].(string); ct != "" {
			o.ContentType = ct
		}
		if md, ok := body["metadata"].(map[string]any); ok {
			o.Metadata = make(map[string]string, len(md))
			for k, v := range md {
				if s, ok := v.(string); ok {
					o.Metadata[k] = s
				}
			}
		}
	}
	o.Kind = "storage#object"
	o.Metageneration = bumpMeta(o.Metageneration)
	o.Updated = clock.Now().Format(time.RFC3339Nano)
	data, _ := json.Marshal(o)
	if err := p.resources.Update(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtObject, ID: id, Data: data}); err != nil {
		return nil, err
	}
	return provider.OK(toMap(o)), nil
}

// bumpMeta increments a metageneration string ("N" → "N+1"; default "1").
func bumpMeta(m string) string {
	n, err := strconv.Atoi(m)
	if err != nil {
		return "1"
	}
	return strconv.Itoa(n + 1)
}

func (p *Provider) ObjectsDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket, _ := nr.Params["bucket"].(string)
	object, _ := nr.Params["object"].(string)
	id := bucket + "/" + object
	if err := p.resources.Delete(ctx, nr.AccountID, store.GlobalRegion, rtObject, id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "object not found", 404)
		}
		return nil, err
	}
	if err := p.blobs.Delete(ctx, blobsNamespace, id); err != nil {
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

// toMap converts an objectMeta struct into a map for JSON-encoding by the codec.
func toMap(o objectMeta) map[string]any {
	b, _ := json.Marshal(o)
	var m map[string]any
	json.Unmarshal(b, &m)
	return m
}

// toBucketMap converts a bucketMeta struct into a map for JSON-encoding.
func toBucketMap(b bucketMeta) map[string]any {
	return map[string]any{
		"kind":           "storage#bucket",
		"id":             b.Name,
		"name":           b.Name,
		"location":       b.Location,
		"locationType":   "multi-region",
		"storageClass":   b.StorageClass,
		"timeCreated":    b.TimeCreated,
		"updated":        b.Updated,
		"generation":     "0",
		"metageneration": "1",
		"projectNumber":  "0",
		"selfLink":       "https://www.googleapis.com/storage/v1/b/" + b.Name,
		"etag":           "CAE=",
		"versioning":     map[string]any{"enabled": false},
		"iamConfiguration": map[string]any{
			"uniformBucketLevelAccess": map[string]any{"enabled": false},
			"bucketPolicyOnly":         map[string]any{"enabled": false},
			"publicAccessPrevention":   "inherited",
		},
	}
}

// objectResponse returns the JSON metadata for an object, or 404.
func (p *Provider) objectResponse(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket, _ := nr.Params["bucket"].(string)
	object, _ := nr.Params["object"].(string)
	id := bucket + "/" + object
	e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtObject, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "object not found", 404)
		}
		return nil, err
	}
	var o objectMeta
	json.Unmarshal(e.Data, &o)
	o.Kind = "storage#object"
	return provider.OK(toMap(o)), nil
}

// ─── pagination helpers ────────────────────────────────────────────────────────

func encodeCursor(id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(id))
}

func decodeCursor(token string) string {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return ""
	}
	return string(b)
}

func maxResults(params map[string]any) int {
	if v, ok := params["maxResults"].(string); ok {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 1000
}

// paginateEntries applies the GCS pageToken cursor + maxResults to a sorted
// entry list. pageToken is an opaque base64 of the last-returned entry ID.
func paginateEntries(entries []store.ResourceEntry, params map[string]any) ([]store.ResourceEntry, string) {
	limit := maxResults(params)
	start := 0
	if tok, _ := params["pageToken"].(string); tok != "" {
		if cursor := decodeCursor(tok); cursor != "" {
			for start < len(entries) && entries[start].ID <= cursor {
				start++
			}
		}
	}
	end := start + limit
	if end > len(entries) {
		end = len(entries)
	}
	page := entries[start:end]
	next := ""
	if end < len(entries) {
		next = encodeCursor(entries[end-1].ID)
	}
	return page, next
}

// ─── IAM policy ───────────────────────────────────────────────────────────────

type iamBinding struct {
	Role    string   `json:"role"`
	Members []string `json:"members"`
}

type iamPolicy struct {
	Kind       string       `json:"kind"`
	ResourceID string       `json:"resourceId,omitempty"`
	Bindings   []iamBinding `json:"bindings"`
	Etag       string       `json:"etag"`
	Version    int          `json:"version"`
}

func defaultPolicy(bucket, project, resourceID string) iamPolicy {
	return iamPolicy{
		Kind:       "storage#policy",
		ResourceID: resourceID,
		Bindings: []iamBinding{
			{Role: "roles/storage.legacyBucketOwner", Members: []string{"projectEditor:" + project, "projectOwner:" + project}},
			{Role: "roles/storage.legacyBucketReader", Members: []string{"projectViewer:" + project}},
		},
		Etag:    "CAE=",
		Version: 1,
	}
}

func (p *Provider) BucketsGetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket, _ := nr.Params["bucket"].(string)
	pol := defaultPolicy(bucket, nr.AccountID, nr.ResourceID("gcs-bucket-policy", bucket))
	if e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtBucketIAM, bucket); err == nil {
		json.Unmarshal(e.Data, &pol)
	}
	return provider.OK(map[string]any{
		"kind":       pol.Kind,
		"resourceId": pol.ResourceID,
		"bindings":   pol.Bindings,
		"etag":       pol.Etag,
		"version":    pol.Version,
	}), nil
}

func (p *Provider) BucketsSetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket, _ := nr.Params["bucket"].(string)
	body, _ := nr.Params["body"].(map[string]any)
	pol := iamPolicy{Kind: "storage#policy", ResourceID: nr.ResourceID("gcs-bucket-policy", bucket), Etag: "CAE=", Version: 1}
	if bs, ok := body["bindings"].([]any); ok {
		for _, b := range bs {
			bm, _ := b.(map[string]any)
			role, _ := bm["role"].(string)
			ib := iamBinding{Role: role}
			if ms, ok := bm["members"].([]any); ok {
				for _, m := range ms {
					if s, ok := m.(string); ok {
						ib.Members = append(ib.Members, s)
					}
				}
			}
			pol.Bindings = append(pol.Bindings, ib)
		}
	}
	data, _ := json.Marshal(pol)
	_ = p.resources.Upsert(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtBucketIAM, ID: bucket, Data: data})
	return provider.OK(map[string]any{
		"kind":       pol.Kind,
		"resourceId": pol.ResourceID,
		"bindings":   pol.Bindings,
		"etag":       pol.Etag,
		"version":    pol.Version,
	}), nil
}

// ─── ACLs ─────────────────────────────────────────────────────────────────────

type aclEntry struct {
	Kind   string `json:"kind"`
	ID     string `json:"id"`
	Entity string `json:"entity"`
	Role   string `json:"role"`
	Bucket string `json:"bucket,omitempty"`
	Object string `json:"object,omitempty"`
	ETag   string `json:"etag"`
}

func defaultACL(bucket, object, kind string) []aclEntry {
	entries := []aclEntry{
		{Kind: kind, ID: bucket + "/owners", Entity: "project-owners-jaiscloud", Role: "OWNER", Bucket: bucket, ETag: "CAE="},
		{Kind: kind, ID: bucket + "/editors", Entity: "project-editors-jaiscloud", Role: "OWNER", Bucket: bucket, ETag: "CAE="},
		{Kind: kind, ID: bucket + "/viewers", Entity: "project-viewers-jaiscloud", Role: "READER", Bucket: bucket, ETag: "CAE="},
	}
	if object != "" {
		for i := range entries {
			entries[i].Object = object
		}
	}
	return entries
}

func (p *Provider) listACL(ctx context.Context, nr *model.NormalizedRequest, bucket, object, kind string) (*model.ProviderResponse, error) {
	aclID := bucket
	if object != "" {
		aclID = bucket + "/" + object
	}
	entries := defaultACL(bucket, object, kind)
	if e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtACL, aclID); err == nil {
		var stored []aclEntry
		if json.Unmarshal(e.Data, &stored) == nil {
			entries = stored
		}
	}
	return provider.OK(map[string]any{"kind": kind, "items": entries}), nil
}

func (p *Provider) insertACL(ctx context.Context, nr *model.NormalizedRequest, bucket, object, kind string) (*model.ProviderResponse, error) {
	body, _ := nr.Params["body"].(map[string]any)
	entity, _ := body["entity"].(string)
	role, _ := body["role"].(string)
	if entity == "" || role == "" {
		return nil, model.NewProviderError("InvalidRequest", "ACL insert requires entity and role", 400)
	}
	aclID := bucket
	if object != "" {
		aclID = bucket + "/" + object
	}
	entries := defaultACL(bucket, object, kind)
	if e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtACL, aclID); err == nil {
		var stored []aclEntry
		if json.Unmarshal(e.Data, &stored) == nil {
			entries = stored
		}
	}
	entry := aclEntry{Kind: kind, ID: aclID + "/" + entity, Entity: entity, Role: role, Bucket: bucket, Object: object, ETag: "CAE="}
	entries = append(entries, entry)
	data, _ := json.Marshal(entries)
	_ = p.resources.Upsert(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtACL, ID: aclID, Data: data})
	resp := map[string]any{
		"kind":   kind,
		"id":     entry.ID,
		"entity": entry.Entity,
		"role":   entry.Role,
		"bucket": entry.Bucket,
		"etag":   entry.ETag,
	}
	if entry.Object != "" {
		resp["object"] = entry.Object
	}
	return provider.OK(resp), nil
}

func (p *Provider) BucketACLList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket, _ := nr.Params["bucket"].(string)
	return p.listACL(ctx, nr, bucket, "", "storage#bucketAccessControls")
}

func (p *Provider) BucketACLInsert(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket, _ := nr.Params["bucket"].(string)
	return p.insertACL(ctx, nr, bucket, "", "storage#bucketAccessControl")
}

func (p *Provider) ObjectACLList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket, _ := nr.Params["bucket"].(string)
	object, _ := nr.Params["object"].(string)
	return p.listACL(ctx, nr, bucket, object, "storage#objectAccessControls")
}

func (p *Provider) ObjectACLInsert(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket, _ := nr.Params["bucket"].(string)
	object, _ := nr.Params["object"].(string)
	return p.insertACL(ctx, nr, bucket, object, "storage#objectAccessControl")
}

// ─── resumable uploads ────────────────────────────────────────────────────────

func (p *Provider) ObjectsInsertStartResumable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket, _ := nr.Params["bucket"].(string)
	object, _ := nr.Params["object"].(string)
	if object == "" {
		if body, ok := nr.Params["body"].(map[string]any); ok {
			object, _ = body["name"].(string)
		}
	}
	if bucket == "" || object == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing bucket or object name", 400)
	}
	ct, _ := nr.Params[wire.ContentTypeKey].(string)

	id := p.nextGen() // atomic + monotonic, collision-free under concurrency
	p.mu.Lock()
	if len(p.uploads) >= maxUploadSessions {
		p.mu.Unlock()
		return nil, model.NewProviderError("InvalidRequest", "too many active resumable uploads", 429)
	}
	p.uploads[id] = &uploadSession{Bucket: bucket, Object: object, ContentType: ct}
	p.mu.Unlock()

	loc := fmt.Sprintf("/upload/storage/v1/b/%s/o?uploadType=resumable&upload_id=%s", bucket, id)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{wire.LocationKey: loc}}, nil
}

func (p *Provider) ObjectsInsertResumable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	uploadID, _ := nr.Params["upload_id"].(string)
	media, _ := nr.Params[wire.MediaKey].([]byte)
	cr, _ := nr.Params["contentRange"].(string)

	p.mu.Lock()
	sess, ok := p.uploads[uploadID]
	if !ok {
		p.mu.Unlock()
		return nil, model.NewProviderError("NotFound", "unknown upload_id", 404)
	}
	if ct, _ := nr.Params[wire.ContentTypeKey].(string); ct != "" {
		sess.ContentType = ct
	}
	start, end, total, hasTotal, isStatus := parseContentRange(cr)
	complete := false
	if !isStatus {
		// Offset repair: only append a chunk that begins exactly where the
		// accumulated buffer ends; out-of-order/duplicate chunks are ignored.
		if int64(len(sess.buf)) == start {
			sess.buf = append(sess.buf, media...)
		}
		complete = hasTotal && end+1 >= total && int64(len(sess.buf)) >= total
	}
	if complete {
		delete(p.uploads, uploadID)
	}
	bucket := sess.Bucket
	object := sess.Object
	contentType := sess.ContentType
	all := sess.buf
	p.mu.Unlock()

	if !complete {
		rng := fmt.Sprintf("bytes=0-%d", len(all)-1)
		if len(all) == 0 {
			rng = "bytes=0-0"
		}
		return &model.ProviderResponse{HTTPStatus: 308, Data: map[string]any{wire.RangeKey: rng}}, nil
	}

	nr.Params["bucket"] = bucket
	nr.Params["object"] = object
	nr.Params[wire.MediaKey] = all
	nr.Params[wire.ContentTypeKey] = contentType
	return p.ObjectsInsert(ctx, nr)
}

// parseContentRange parses a "bytes <start>-<end>/<total>" header, or a status
// query "bytes */<total>". total may be "*" (unknown) in which case hasTotal is
// false. isStatus is true for the "bytes *" status query.
func parseContentRange(cr string) (start, end, total int64, hasTotal, isStatus bool) {
	rest := strings.TrimPrefix(cr, "bytes ")
	rangePart, totalPart, _ := strings.Cut(rest, "/")
	if rangePart == "*" {
		if totalPart != "" && totalPart != "*" {
			total, _ = strconv.ParseInt(totalPart, 10, 64)
			hasTotal = true
		}
		return 0, 0, total, hasTotal, true
	}
	se := strings.SplitN(rangePart, "-", 2)
	if len(se) == 2 {
		start, _ = strconv.ParseInt(se[0], 10, 64)
		end, _ = strconv.ParseInt(se[1], 10, 64)
	}
	if totalPart != "" && totalPart != "*" {
		total, _ = strconv.ParseInt(totalPart, 10, 64)
		hasTotal = true
	}
	return
}
