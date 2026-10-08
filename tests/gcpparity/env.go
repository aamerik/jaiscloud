//go:build gcp_parity

package gcpparity

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Env is the shared per-run context handed to every case: one REST HTTP client
// pointed at the live emulator, plus the resolved naming context. gRPC clients
// are built per call (each official client owns and closes its own connection),
// so no gRPC connection is shared here.
type Env struct {
	Cfg Config
	// Side and StepTag identify the twin resource a mutation-parity step is
	// currently driving: Side is "grpc"/"rest" and StepTag is a per-step token
	// (both empty everywhere else). Resource folds them into the ids it returns
	// so the two transports mutate distinct resources and two different parity
	// steps never reuse a name — important for services whose delete is a soft
	// delete (a re-create of the same name would 409). The harness strips Side
	// before diffing, leaving the tag+suffix identical on both sides (AUD3-12).
	Side    string
	StepTag string
	http    *http.Client
}

// NewEnv prepares an HTTP client for the REST listener. The caller must Close
// the Env.
func NewEnv(ctx context.Context, cfg Config) (*Env, error) {
	return &Env{
		Cfg:  cfg,
		http: &http.Client{Timeout: 30 * time.Second},
	}, nil
}

// Close releases idle HTTP connections.
func (e *Env) Close() {
	if e.http != nil {
		e.http.CloseIdleConnections()
	}
}

// GRPCClientOptions points an official Google client at the emulator's gRPC
// listener with an insecure, unauthenticated connection. Each client owns the
// connection it dials, so callers must Close their client.
func (e *Env) GRPCClientOptions() []option.ClientOption {
	return []option.ClientOption{
		option.WithEndpoint(e.Cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	}
}

// Resource returns a run-unique resource id. During a mutation-parity step
// (Side set) the step tag and the transport's twin token are folded in, so the
// gRPC and REST sides mutate distinct resources under the same logical name and
// two steps never collide; the harness strips the twin token before diffing the
// two responses.
func (e *Env) Resource(prefix string) string {
	if e.Side != "" {
		return prefix + "-" + e.StepTag + "-" + e.Side + "-" + e.Cfg.Suffix
	}
	return e.Cfg.ResourceName(prefix)
}

// restDo sends one REST request and returns the raw body plus status code. A
// non-2xx status is returned with its body; the case decides whether that is an
// error.
func (e *Env) restDo(ctx context.Context, method, path, body, contentType string) ([]byte, int, error) {
	var r io.Reader
	if body != "" {
		r = bytes.NewReader([]byte(body))
	}
	req, err := http.NewRequestWithContext(ctx, method, e.Cfg.REST+path, r)
	if err != nil {
		return nil, 0, fmt.Errorf("%s %s: %w", method, path, err)
	}
	if body != "" {
		if contentType == "" {
			contentType = "application/json"
		}
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := e.http.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("%s %s: read body: %w", method, path, err)
	}
	return data, resp.StatusCode, nil
}

// Rest calls restDo and requires a 2xx response.
func (e *Env) Rest(ctx context.Context, method, path, body string) (json.RawMessage, error) {
	data, status, err := e.restDo(ctx, method, path, body, "")
	if err != nil {
		return nil, err
	}
	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("%s %s: HTTP %d: %s", method, path, status, truncateBody(data))
	}
	return json.RawMessage(data), nil
}

// RestDelete sends a DELETE and requires a 2xx response; the (usually empty)
// body is discarded.
func (e *Env) RestDelete(ctx context.Context, path string) error {
	_, status, err := e.restDo(ctx, http.MethodDelete, path, "", "")
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("DELETE %s: HTTP %d", path, status)
	}
	return nil
}

// RestOperationResource sends a REST mutation whose response is a
// google.longrunning.Operation and returns the settled response resource (the
// Operation's "response" member). The emulator's default LRO mode completes
// operations synchronously, so the operation is already done and carries the
// resource inline; an in-flight operation is reported as an error rather than
// polled, because the parity harness never enables the opt-in async mode.
func (e *Env) RestOperationResource(ctx context.Context, method, path, body string) (json.RawMessage, error) {
	raw, err := e.Rest(ctx, method, path, body)
	if err != nil {
		return nil, err
	}
	var op struct {
		Name     string          `json:"name"`
		Done     bool            `json:"done"`
		Response json.RawMessage `json:"response"`
	}
	if err := json.Unmarshal(raw, &op); err != nil {
		return nil, fmt.Errorf("%s %s: decode operation: %w", method, path, err)
	}
	if !op.Done || len(op.Response) == 0 {
		return nil, fmt.Errorf("%s %s: operation %q is not done (the parity harness requires synchronous LRO mode)", method, path, op.Name)
	}
	return stripAnyType(op.Response), nil
}

// stripAnyType removes the google.protobuf.Any "@type" wrapper member from a
// long-running-operation response so the REST side (an Any-encoded resource)
// compares against the gRPC side (the unwrapped typed resource) on the resource
// fields alone. A non-object or @type-free body is returned unchanged.
func stripAnyType(raw json.RawMessage) json.RawMessage {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return raw
	}
	if _, ok := m["@type"]; !ok {
		return raw
	}
	delete(m, "@type")
	clean, err := json.Marshal(m)
	if err != nil {
		return raw
	}
	return clean
}

// GrpcBody marshals a proto response into the camelCase, protojson form the REST
// API would emit, so the two transports diff on logical fields.
func (e *Env) GrpcBody(m proto.Message) (json.RawMessage, error) {
	if m == nil {
		return nil, nil
	}
	b, err := protojson.MarshalOptions{UseProtoNames: false, EmitUnpopulated: false}.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("protojson marshal: %w", err)
	}
	return json.RawMessage(b), nil
}

func truncateBody(b []byte) string {
	const max = 160
	s := string(b)
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}
