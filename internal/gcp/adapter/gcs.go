package gcp

import (
	"bytes"
	"encoding/json"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"

	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// GCSCodec decodes/encodes the Google Cloud Storage JSON + media wire API.
// Implements adapter.Codec.
type GCSCodec struct{}

func (c *GCSCodec) ServiceName() string { return "storage" }

// Decode routes the request to the correct storage handler based on path prefix.
func (c *GCSCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	path := r.URL.EscapedPath()
	switch {
	case strings.HasPrefix(path, "/upload/storage/v1/"):
		return c.decodeUpload(r, body, strings.TrimPrefix(path, "/upload/storage/v1/"))
	case strings.HasPrefix(path, "/download/storage/v1/"):
		return c.decodeDownload(r, body, strings.TrimPrefix(path, "/download/storage/v1/"))
	case strings.HasPrefix(path, "/storage/v1/"):
		return c.decodeStorage(r, body, strings.TrimPrefix(path, "/storage/v1/"))
	default:
		return nil, model.NewProviderError("InvalidRequest", "unsupported storage path", 404)
	}
}

// decodeDownload handles /download/storage/v1/... — always a media download, so
// object GETs are forced to ObjectsGetMedia regardless of the alt= param.
func (c *GCSCodec) decodeDownload(r *http.Request, body []byte, rest string) (*model.NormalizedRequest, error) {
	nr, err := c.decodeStorage(r, body, rest)
	if err != nil {
		return nil, err
	}
	if nr.Action == "ObjectsGet" {
		nr.Action = "ObjectsGetMedia"
	}
	return nr, nil
}

// decodeStorage handles the metadata/JSON API under /storage/v1/ and /download/storage/v1/.
func (c *GCSCodec) decodeStorage(r *http.Request, body []byte, rest string) (*model.NormalizedRequest, error) {
	seg := splitEscaped(rest)
	nr := &model.NormalizedRequest{Service: "storage", Params: map[string]any{}}
	queryToParams(r, nr.Params)

	switch {
	case len(seg) == 1 && seg[0] == "b":
		// /b — bucket list (GET) or insert (POST)
		if r.Method == http.MethodPost {
			nr.Action = "BucketsInsert"
		} else {
			nr.Action = "BucketsList"
		}
		m, err := parseJSON(body)
		if err != nil {
			return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
		}
		nr.Params["body"] = m
	case len(seg) == 2 && seg[0] == "b":
		// /b/{bucket}
		nr.Params["bucket"] = seg[1]
		switch r.Method {
		case http.MethodPost, http.MethodPut, http.MethodPatch:
			nr.Action = "BucketsUpdate"
			m, err := parseJSON(body)
			if err != nil {
				return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
			}
			nr.Params["body"] = m
		case http.MethodDelete:
			nr.Action = "BucketsDelete"
		default:
			nr.Action = "BucketsGet"
		}
	case len(seg) >= 3 && seg[0] == "b" && seg[2] == "iam":
		// /b/{bucket}/iam
		nr.Params["bucket"] = seg[1]
		if r.Method == http.MethodPut {
			nr.Action = "BucketsSetIamPolicy"
			m, err := parseJSON(body)
			if err != nil {
				return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
			}
			nr.Params["body"] = m
		} else {
			nr.Action = "BucketsGetIamPolicy"
		}
	case len(seg) == 3 && seg[0] == "b" && seg[2] == "acl":
		// /b/{bucket}/acl
		nr.Params["bucket"] = seg[1]
		if r.Method == http.MethodPost {
			nr.Action = "BucketACLInsert"
			m, err := parseJSON(body)
			if err != nil {
				return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
			}
			nr.Params["body"] = m
		} else {
			nr.Action = "BucketACLList"
		}
	case len(seg) >= 5 && seg[0] == "b" && seg[2] == "o" && seg[len(seg)-1] == "acl":
		// /b/{bucket}/o/{object...}/acl
		nr.Params["bucket"] = seg[1]
		nr.Params["object"] = strings.Join(seg[3:len(seg)-1], "/")
		if r.Method == http.MethodPost {
			nr.Action = "ObjectACLInsert"
			m, err := parseJSON(body)
			if err != nil {
				return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
			}
			nr.Params["body"] = m
		} else {
			nr.Action = "ObjectACLList"
		}
	case len(seg) >= 3 && seg[0] == "b" && seg[2] == "o":
		// /b/{bucket}/o[/{object}]
		nr.Params["bucket"] = seg[1]
		switch {
		case len(seg) == 3:
			// /b/{bucket}/o — object list (GET) or JSON-metadata insert (POST)
			if r.Method == http.MethodPost {
				nr.Action = "ObjectsInsert"
				m, err := parseJSON(body)
				if err != nil {
					return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
				}
				nr.Params["body"] = m
			} else {
				nr.Action = "ObjectsList"
			}
		default:
			// /b/{bucket}/o/{object...}
			nr.Params["object"] = strings.Join(seg[3:], "/")
			switch r.Method {
			case http.MethodDelete:
				nr.Action = "ObjectsDelete"
			case http.MethodPut, http.MethodPatch, http.MethodPost:
				nr.Action = "ObjectsUpdate"
				m, err := parseJSON(body)
				if err != nil {
					return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
				}
				nr.Params["body"] = m
			default:
				// alt=media → raw bytes; otherwise JSON metadata
				if nr.Params["alt"] == "media" {
					nr.Action = "ObjectsGetMedia"
				} else {
					nr.Action = "ObjectsGet"
				}
			}
		}
	default:
		return nil, model.NewProviderError("InvalidRequest", "unsupported storage path", 404)
	}
	return nr, nil
}

// decodeUpload handles the media API under /upload/storage/v1/.
func (c *GCSCodec) decodeUpload(r *http.Request, body []byte, rest string) (*model.NormalizedRequest, error) {
	seg := splitEscaped(rest)
	if !(len(seg) >= 3 && seg[0] == "b" && seg[2] == "o") {
		return nil, model.NewProviderError("InvalidRequest", "unsupported upload path", 404)
	}
	nr := &model.NormalizedRequest{Service: "storage", Params: map[string]any{}}
	queryToParams(r, nr.Params)
	nr.Params["bucket"] = seg[1]

	if len(seg) > 3 {
		nr.Params["object"] = strings.Join(seg[3:], "/")
	}
	if n := nr.Params["name"]; n != nil && n != "" {
		nr.Params["object"] = n
	}

	uploadType, _ := nr.Params["uploadType"].(string)
	switch uploadType {
	case "media":
		nr.Action = "ObjectsInsert"
		if body == nil {
			// Streaming upload — the gateway left r.Body unread.
			nr.Params[wire.StreamKey] = r.Body
		} else {
			nr.Params[wire.MediaKey] = body
		}
		if ct := r.Header.Get("Content-Type"); ct != "" {
			nr.Params[wire.ContentTypeKey] = ct
		}
	case "multipart":
		nr.Action = "ObjectsInsert"
		if err := parseMultipart(r, body, nr.Params); err != nil {
			return nil, err
		}
	case "resumable":
		if r.Method == http.MethodPut {
			nr.Action = "ObjectsInsertResumable"
			nr.Params[wire.MediaKey] = body
			if cr := r.Header.Get("Content-Range"); cr != "" {
				nr.Params["contentRange"] = cr
			}
		} else {
			nr.Action = "ObjectsInsertStartResumable"
			m, err := parseJSON(body)
			if err != nil {
				return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
			}
			nr.Params["body"] = m
			// The object content type is declared once at start via the
			// X-Upload-Content-Type header (the chunk's Content-Type on PUT is
			// the media type, not the object content type).
			if ct := r.Header.Get("X-Upload-Content-Type"); ct != "" {
				nr.Params[wire.ContentTypeKey] = ct
			}
		}
	default:
		// No uploadType: treat POST body as raw media (defensive default).
		nr.Action = "ObjectsInsert"
		nr.Params[wire.MediaKey] = body
		if ct := r.Header.Get("Content-Type"); ct != "" {
			nr.Params[wire.ContentTypeKey] = ct
		}
	}
	return nr, nil
}

// Encode serialises a provider response. Media responses (Data carries the
// wire.MediaKey) are written as raw bytes with the stored content type; everything
// else is JSON-encoded.
func (c *GCSCodec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	status := resp.HTTPStatus
	if status == 0 {
		status = http.StatusOK
	}
	headers := http.Header{}
	headers.Set("Content-Type", "application/json; charset=UTF-8")

	if loc, ok := resp.Data[wire.LocationKey].(string); ok && loc != "" {
		headers.Set("Location", loc)
		return status, headers, nil
	}

	if rng, ok := resp.Data[wire.RangeKey].(string); ok && rng != "" {
		headers.Set("Range", rng)
		return status, headers, nil
	}

	// Streaming download — the gateway will io.Copy the reader; return headers only.
	if _, ok := resp.Data["_stream"].(io.ReadCloser); ok {
		if ct, _ := resp.Data[wire.ContentTypeKey].(string); ct != "" {
			headers.Set("Content-Type", ct)
		}
		return status, headers, nil
	}

	if b, ok := resp.Data[wire.MediaKey].([]byte); ok {
		headers.Set("Content-Type", "application/octet-stream")
		if ct, _ := resp.Data[wire.ContentTypeKey].(string); ct != "" {
			headers.Set("Content-Type", ct)
		}
		return status, headers, b
	}

	out, err := json.Marshal(resp.Data)
	if err != nil {
		return http.StatusInternalServerError, headers, []byte(`{"error":{"code":500,"message":"encode failure","status":"INTERNAL"}}`)
	}
	return status, headers, out
}

// EncodeError serialises a ProviderError as a GCS error envelope. GCS (unlike
// other GCP REST APIs) returns {"error":{"errors":[{"domain","reason","message"}],
// "code","message"}} — no "status" field.
func (c *GCSCodec) EncodeError(nr *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	status := perr.HTTPStatus
	if status == 0 {
		status = http.StatusInternalServerError
	}
	headers := http.Header{}
	headers.Set("Content-Type", "application/json; charset=UTF-8")
	env := map[string]any{
		"error": map[string]any{
			"errors": []any{
				map[string]any{
					"domain":  "global",
					"reason":  gcpReason(perr.Code),
					"message": perr.Message,
				},
			},
			"code":    status,
			"message": perr.Message,
		},
	}
	out, _ := json.Marshal(env)
	return status, headers, out
}

// gcpReason maps a canonical ProviderError code to a GCS error reason string.
func gcpReason(code string) string {
	switch code {
	case "NotFound":
		return "notFound"
	case "AlreadyExists":
		return "alreadyExists"
	case "bucketNotEmpty":
		return "bucketNotEmpty"
	case "Conflict":
		return "conflict"
	case "InvalidRequest":
		return "invalid"
	case "UnsupportedOperation":
		return "unsupported"
	default:
		return "internalError"
	}
}

// gcpStatusString maps an HTTP status to the google.rpc.Code status string.
func gcpStatusString(code int) string {
	switch code {
	case 400:
		return "INVALID_ARGUMENT"
	case 401:
		return "UNAUTHENTICATED"
	case 403:
		return "PERMISSION_DENIED"
	case 404:
		return "NOT_FOUND"
	case 409:
		return "ALREADY_EXISTS"
	case 412:
		return "FAILED_PRECONDITION"
	case 429:
		return "RESOURCE_EXHAUSTED"
	case 499:
		return "CANCELLED"
	case 500:
		return "INTERNAL"
	case 501:
		return "UNIMPLEMENTED"
	case 503:
		return "UNAVAILABLE"
	default:
		return "UNKNOWN"
	}
}

// splitEscaped splits an escaped URL path on "/" and unescapes each segment, so
// %2F within a segment (a slash in an object name) survives as part of the name.
func splitEscaped(path string) []string {
	raw := strings.Split(strings.TrimPrefix(path, "/"), "/")
	out := make([]string, 0, len(raw))
	for _, s := range raw {
		if u, err := url.PathUnescape(s); err == nil {
			out = append(out, u)
		} else {
			out = append(out, s)
		}
	}
	return out
}

// queryToParams copies single-valued query parameters into params as strings.
func queryToParams(r *http.Request, params map[string]any) {
	for k, vs := range r.URL.Query() {
		if len(vs) > 0 {
			params[k] = vs[0]
		}
	}
}

// parseJSON decodes a JSON body into a map, or returns nil for empty/JSON bodies
// that are not objects (e.g. media). Never returns an error — decode failures
// are surfaced by the provider as validation errors.
func parseJSON(body []byte) (map[string]any, error) {
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, nil
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// parseMultipart decodes a multipart/related upload body (JSON metadata part
// followed by a media part) into params.
func parseMultipart(r *http.Request, body []byte, params map[string]any) error {
	ct := r.Header.Get("Content-Type")
	mediaType, p, err := mime.ParseMediaType(ct)
	if err != nil || !strings.HasPrefix(mediaType, "multipart/") {
		return model.NewProviderError("InvalidRequest", "expected multipart body", 400)
	}
	mr := multipart.NewReader(bytes.NewReader(body), p["boundary"])
	for partIdx := 0; ; partIdx++ {
		part, err := mr.NextPart()
		if err != nil {
			break // io.EOF ends the loop
		}
		var buf bytes.Buffer
		if _, err := buf.ReadFrom(part); err != nil {
			return model.NewProviderError("InvalidRequest", "malformed multipart body", 400)
		}
		data := buf.Bytes()
		if partIdx == 0 {
			m, err := parseJSON(data)
			if err != nil {
				return model.NewProviderError("InvalidRequest", "malformed JSON metadata", 400)
			}
			params["body"] = m
			if m != nil {
				if n, ok := m["name"].(string); ok && n != "" {
					params["object"] = n
				}
			}
		} else {
			params[wire.MediaKey] = data
			if cth := part.Header.Get("Content-Type"); cth != "" {
				params[wire.ContentTypeKey] = cth
			}
		}
	}
	if params[wire.MediaKey] == nil {
		return model.NewProviderError("InvalidRequest", "multipart body missing media part", 400)
	}
	return nil
}
