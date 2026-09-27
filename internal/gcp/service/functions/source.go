package functions

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"

	"jaiscloud/internal/gcp/store/gcs"
	"jaiscloud/internal/model"
)

// functionsSourceBucket is the blobfs namespace holding deployed function
// source archives (the GCP analogue of AWS Lambda's "lambda-code" namespace).
// Metadata (revision hash, size, blob key) lives on the function row; the bytes
// live here so memory and Postgres modes behave identically.
const functionsSourceBucket = "functions-source"

// SourceFetcher resolves the plaintext bytes of a GCS object so a function
// deployed with a v1 sourceArchiveUrl ("gs://bucket/object") or a v2
// buildConfig.source.storageSource can persist its archive. It is implemented
// by the GCS provider (which owns object encryption/decryption) and injected by
// main.go; a nil fetcher disables source resolution (metadata-only, the
// pre-FD1 behavior).
type SourceFetcher interface {
	FetchObjectBytes(ctx context.Context, bucket, object string) ([]byte, error)
}

// sourceBlobKey is the blobfs key of a function's source archive at revision
// rev (the sha256 hex). It is scoped by project/location/id exactly like the
// store row, so it cannot collide across locations.
func sourceBlobKey(project, location, id, rev string) string {
	return "functions/" + project + "/" + location + "/" + id + "/" + rev + ".zip"
}

// sourceRef returns the GCS bucket/object for an input's source reference,
// preferring the explicit v2 storageSource fields and falling back to a v1
// sourceArchiveUrl. It returns empty strings when the input carries no
// resolvable GCS reference.
func sourceRef(in FunctionInput) (bucket, object string) {
	if in.SourceBucket != "" && in.SourceObject != "" {
		return in.SourceBucket, in.SourceObject
	}
	return splitGSURL(in.SourceArchiveURL)
}

// splitGSURL parses "gs://bucket/object" into bucket and object. Unknown forms
// return empty strings.
func splitGSURL(ref string) (bucket, object string) {
	if !hasGSPrefix(ref) {
		return "", ""
	}
	rest := ref[len("gs://"):]
	i := indexByte(rest, '/')
	if i <= 0 || i >= len(rest)-1 {
		return "", ""
	}
	return rest[:i], rest[i+1:]
}

// StoreSource persists zip as the function's source archive and returns its
// sha256 revision hash, size, and blob key. It does not touch the function row;
// callers persist the returned metadata. Writing is content-addressed, so
// re-deploying identical bytes is a no-op at the blob layer.
func (s *Service) StoreSource(ctx context.Context, project, location, id string, zip []byte) (sha256hex string, size int64, blobKey string, err error) {
	if s.blobs == nil {
		return "", 0, "", invalidArgument("source storage is not configured")
	}
	sum := sha256.Sum256(zip)
	sha256hex = hex.EncodeToString(sum[:])
	size = int64(len(zip))
	blobKey = sourceBlobKey(project, location, id, sha256hex)
	if perr := s.blobs.Put(ctx, functionsSourceBucket, blobKey, zip); perr != nil {
		return "", 0, "", perr
	}
	return sha256hex, size, blobKey, nil
}

// resolveSource persists the source archive referenced by in when a fetcher is
// configured and the input carries a resolvable GCS reference. It is a no-op
// otherwise, so metadata-only creates keep working without GCS.
func (s *Service) resolveSource(ctx context.Context, project, location, id string, in FunctionInput) (sha256hex string, size int64, blobKey string, err error) {
	bucket, object := sourceRef(in)
	if s.sourceFetcher == nil || bucket == "" || object == "" {
		return "", 0, "", nil
	}
	zip, ferr := s.sourceFetcher.FetchObjectBytes(ctx, bucket, object)
	if ferr != nil {
		if errors.Is(ferr, gcs.ErrNoSuchObject) {
			return "", 0, "", model.NewProviderError("NotFound", "source archive gs://"+bucket+"/"+object+" not found", 404)
		}
		return "", 0, "", ferr
	}
	return s.StoreSource(ctx, project, location, id, zip)
}

// discardSource best-effort deletes a just-written archive, used to roll back
// when the accompanying metadata write fails (e.g. a concurrent create).
func (s *Service) discardSource(ctx context.Context, blobKey string) {
	if s.blobs != nil && blobKey != "" {
		_ = s.blobs.Delete(ctx, functionsSourceBucket, blobKey)
	}
}

// codeKeySeparator joins a function's location and id into the single opaque
// CodeKey string the shared Lambda executor's CodeLoader receives (the executor
// interface carries account + one name, not a location). It is "." rather than
// "/" so the key is safe both as a Docker container-name fragment and as the
// K8s pod/route identifier. Neither GCP locations nor function ids may contain
// ".".
const codeKeySeparator = "."

// CodeKey builds the executor code identifier for a function ("location.id").
func CodeKey(location, id string) string { return location + codeKeySeparator + id }

// splitCodeKey parses a CodeKey back into location and id.
func splitCodeKey(key string) (location, id string, ok bool) {
	i := strings.Index(key, codeKeySeparator)
	if i <= 0 || i >= len(key)-1 {
		return "", "", false
	}
	return key[:i], key[i+1:], true
}

// LoadCode implements lambdaexec.CodeLoader for Cloud Functions. funcName is
// the composite CodeKey ("location.id") set by CallFunction; the version
// argument is unused because a GCP function has one stored revision. A function
// with no persisted source yields (nil, nil), so the executor mounts nothing
// and mock echo / image behavior is unchanged.
func (s *Service) LoadCode(ctx context.Context, account, funcName, _ string) ([]byte, error) {
	location, id, ok := splitCodeKey(funcName)
	if !ok || s.blobs == nil {
		return nil, nil
	}
	f, err := s.functions.GetFunction(ctx, account, location, id)
	if err != nil || f.SourceBlobKey == "" {
		return nil, nil
	}
	return s.blobs.Get(ctx, functionsSourceBucket, f.SourceBlobKey)
}

// GetFunctionCodeZip implements admin.LambdaCodeFetcher, serving the code-mount
// init container in K8s mode from the same stored archive. functionName is the
// composite CodeKey; qualifier is ignored.
func (s *Service) GetFunctionCodeZip(ctx context.Context, account, functionName, _ string) ([]byte, error) {
	location, id, ok := splitCodeKey(functionName)
	if !ok {
		return nil, gcs.ErrNoSuchObject
	}
	f, err := s.functions.GetFunction(ctx, account, location, id)
	if err != nil || f.SourceBlobKey == "" {
		return nil, gcs.ErrNoSuchObject
	}
	return s.blobs.Get(ctx, functionsSourceBucket, f.SourceBlobKey)
}

// GetLayerCodeZip implements admin.LambdaCodeFetcher. Cloud Functions has no
// layer concept (GCP uses container images / Artifact Registry), so no layer is
// ever served.
func (s *Service) GetLayerCodeZip(context.Context, string, string, int64) ([]byte, error) {
	return nil, gcs.ErrNoSuchObject
}

// resetSources clears the functions-source blob namespace. The global blob
// resetter also clears it, but Service.Reset is used directly by unit tests.
func (s *Service) resetSources(ctx context.Context) {
	if s.blobs == nil {
		return
	}
	keys, err := s.blobs.List(ctx, functionsSourceBucket, "")
	if err != nil {
		return
	}
	for _, k := range keys {
		_ = s.blobs.Delete(ctx, functionsSourceBucket, k)
	}
}
