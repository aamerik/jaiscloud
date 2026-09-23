// Package serviceusage implements the Service Usage v1 control plane
// (serviceusage.googleapis.com/v1): a project's enabled service APIs.
//
// It is an accept-and-succeed control plane, matching the emulator's posture:
// enabling a service flips it to ENABLED, disabling reverses it, and get/list
// echo that state. There is no real API gating or dependency resolution — a
// service works whether or not it was "enabled". State is stored in the shared
// ResourceStore (memory + PostgreSQL backends), so no provider-level Reset or
// Snapshotter is needed.
//
// Mutations return a google.longrunning.Operation with done:true (jaiscloud
// completes operations synchronously), so SDK/terraform operation futures
// resolve immediately.
package serviceusage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// Resource type in the shared ResourceStore. The entry id is the bare service
// name (e.g. "run.googleapis.com"); the account scope is the project.
const rtService = "gcp_serviceusage_service"

// Service states, per GoogleApiServiceusageV1Service.state.
const (
	stateEnabled  = "ENABLED"
	stateDisabled = "DISABLED"
)

// maxBatchEnable mirrors real GCP's per-request batchEnable cap.
const maxBatchEnable = 20

// Operation metadata @type for the Service Usage v1 control plane.
const operationMetadataType = "type.googleapis.com/google.api.serviceusage.v1.OperationMetadata"

// Provider handles Service Usage v1 services.
type Provider struct {
	resources store.ResourceStore
}

// New returns a Provider backed by the shared ResourceStore.
func New(resources store.ResourceStore) *Provider {
	return &Provider{resources: resources}
}

// Routes maps "ServiceUsage.<Action>" keys to their handlers.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"ServiceUsage.ServicesList":        p.ListServices,
		"ServiceUsage.ServicesGet":         p.GetService,
		"ServiceUsage.ServicesBatchEnable": p.BatchEnableServices,
		"ServiceUsage.ServicesEnable":      p.EnableService,
		"ServiceUsage.ServicesDisable":     p.DisableService,
	}
}

// ─── Wire types ───────────────────────────────────────────────────────────────

// Service is the GoogleApiServiceusageV1Service representation.
type Service struct {
	Name   string         `json:"name,omitempty"`
	Parent string         `json:"parent,omitempty"`
	Config *ServiceConfig `json:"config,omitempty"`
	State  string         `json:"state,omitempty"`
}

// ServiceConfig is the (heavily abbreviated) service configuration. Real GCP
// carries title/quota/auth/endpoints; the emulator tracks only the DNS name.
type ServiceConfig struct {
	Name string `json:"name,omitempty"`
}

// record is the stored per-service state.
type record struct {
	State string `json:"state"`
}

// ─── Operations ───────────────────────────────────────────────────────────────

func (p *Provider) ListServices(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := strParam(nr, "project")
	if project == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing project", 400)
	}
	wanted, err := parseFilter(strParam(nr, "filter"))
	if err != nil {
		return nil, err
	}
	entries, err := p.resources.List(ctx, nr.AccountID, store.GlobalRegion, rtService, "")
	if err != nil {
		return nil, err
	}
	services := make([]Service, 0, len(entries))
	for _, e := range entries {
		st := decodeRecord(e.Data).State
		if wanted != "" && st != wanted {
			continue
		}
		services = append(services, buildService(nr, project, e.ID, st))
	}
	page, next := paging.Page(services, func(s Service) string { return s.Name }, nr.Params)
	items := make([]any, 0, len(page))
	for _, s := range page {
		items = append(items, toMap(s))
	}
	resp := map[string]any{"services": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) GetService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := strParam(nr, "project")
	service := strParam(nr, "service")
	if project == "" || service == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing project or service", 400)
	}
	st, err := p.readState(ctx, nr.AccountID, service)
	if err != nil {
		return nil, err
	}
	return provider.OK(toMap(buildService(nr, project, service, st))), nil
}

func (p *Provider) EnableService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := strParam(nr, "project")
	service := strParam(nr, "service")
	if project == "" || service == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing project or service", 400)
	}
	svc, err := p.setServiceState(ctx, nr, project, service, stateEnabled)
	if err != nil {
		return nil, err
	}
	return provider.OK(p.operation(nr, "enable",
		[]string{svc.Name},
		typedResponse("type.googleapis.com/google.api.serviceusage.v1.EnableServiceResponse",
			map[string]any{"service": toMap(svc)}))), nil
}

func (p *Provider) DisableService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := strParam(nr, "project")
	service := strParam(nr, "service")
	if project == "" || service == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing project or service", 400)
	}
	st, err := p.readState(ctx, nr.AccountID, service)
	if err != nil {
		return nil, err
	}
	if st != stateEnabled {
		return nil, model.NewProviderError("FailedPrecondition",
			fmt.Sprintf("Service %s is not enabled for consumer projects/%s.", service, project), 400)
	}
	svc, err := p.setServiceState(ctx, nr, project, service, stateDisabled)
	if err != nil {
		return nil, err
	}
	return provider.OK(p.operation(nr, "disable",
		[]string{svc.Name},
		typedResponse("type.googleapis.com/google.api.serviceusage.v1.DisableServiceResponse",
			map[string]any{"service": toMap(svc)}))), nil
}

func (p *Provider) BatchEnableServices(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := strParam(nr, "project")
	if project == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing project", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	ids := stringSlice(body["serviceIds"])
	if len(ids) == 0 {
		return nil, model.NewProviderError("InvalidArgument", "serviceIds must not be empty", 400)
	}
	if len(ids) > maxBatchEnable {
		return nil, model.NewProviderError("InvalidArgument",
			fmt.Sprintf("A single request can enable a maximum of %d services at a time.", maxBatchEnable), 400)
	}
	services := make([]any, 0, len(ids))
	names := make([]string, 0, len(ids))
	for _, id := range ids {
		svc, err := p.setServiceState(ctx, nr, project, id, stateEnabled)
		if err != nil {
			return nil, err
		}
		services = append(services, toMap(svc))
		names = append(names, svc.Name)
	}
	return provider.OK(p.operation(nr, "batchEnable", names,
		typedResponse("type.googleapis.com/google.api.serviceusage.v1.BatchEnableServicesResponse",
			map[string]any{"services": services}))), nil
}

// setServiceState persists a service's state and returns its wire form. A
// storage failure is propagated (never swallowed) so an outage surfaces as a
// 5xx rather than a false done:true operation.
func (p *Provider) setServiceState(ctx context.Context, nr *model.NormalizedRequest, project, service, state string) (Service, error) {
	data, err := json.Marshal(record{State: state})
	if err != nil {
		return Service{}, err
	}
	if err := p.resources.Upsert(ctx, nr.AccountID, store.GlobalRegion,
		store.ResourceEntry{Type: rtService, ID: service, Data: data}); err != nil {
		return Service{}, err
	}
	return buildService(nr, project, service, state), nil
}

// readState returns the stored state, defaulting to DISABLED only when the
// service has never been enabled (real GCP lists every public API; the emulator
// has no catalog, so an unknown id resolves to DISABLED rather than NotFound).
// A non-NotFound storage error is propagated.
func (p *Provider) readState(ctx context.Context, account, service string) (string, error) {
	e, err := p.resources.Get(ctx, account, store.GlobalRegion, rtService, service)
	if errors.Is(err, store.ErrNotFound) {
		return stateDisabled, nil
	}
	if err != nil {
		return "", err
	}
	if st := decodeRecord(e.Data).State; st != "" {
		return st, nil
	}
	return stateDisabled, nil
}

// operation renders a completed google.longrunning.Operation. The emulator
// completes every mutation synchronously, so done is always true and the
// operation is never persisted (its name is not addressable).
func (p *Provider) operation(nr *model.NormalizedRequest, verb string, resourceNames []string, response map[string]any) map[string]any {
	op := map[string]any{
		"name": nr.ResourceID("serviceusage-operation", randomHex(12)),
		"metadata": map[string]any{
			"@type":         operationMetadataType,
			"resourceNames": resourceNames,
			"verb":          verb,
		},
		"done": true,
	}
	if response != nil {
		op["response"] = response
	}
	return op
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

// buildService renders the wire form of a service.
func buildService(nr *model.NormalizedRequest, project, service, state string) Service {
	return Service{
		Name:   nr.ResourceID("serviceusage-service", service),
		Parent: nr.ResourceID("serviceusage-parent", project),
		Config: &ServiceConfig{Name: service},
		State:  state,
	}
}

// parseFilter returns the wanted state ("ENABLED"/"DISABLED") or "" for no
// filter. Real GCP accepts only the two state filters; anything else is a 400.
func parseFilter(filter string) (string, error) {
	filter = strings.TrimSpace(filter)
	switch filter {
	case "":
		return "", nil
	case "state:ENABLED":
		return stateEnabled, nil
	case "state:DISABLED":
		return stateDisabled, nil
	default:
		return "", model.NewProviderError("InvalidArgument",
			fmt.Sprintf("Invalid filter %s. The allowed filter strings are state:ENABLED and state:DISABLED.", filter), 400)
	}
}

func decodeRecord(data json.RawMessage) record {
	var r record
	_ = json.Unmarshal(data, &r)
	return r
}

func typedResponse(typ string, fields map[string]any) map[string]any {
	out := map[string]any{"@type": typ}
	for k, v := range fields {
		out[k] = v
	}
	return out
}

func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

// stringSlice extracts a []string from a decoded JSON array ([]any of string).
func stringSlice(v any) []string {
	items, _ := v.([]any)
	out := make([]string, 0, len(items))
	for _, it := range items {
		if s, ok := it.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// toMap converts a typed wire struct to the map shape ProviderResponse expects.
func toMap(v any) map[string]any {
	b, err := json.Marshal(v)
	if err != nil {
		return map[string]any{}
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return map[string]any{}
	}
	return m
}

func randomHex(n int) string {
	b := make([]byte, (n+1)/2)
	if _, err := rand.Read(b); err != nil {
		return strings.Repeat("0", n)
	}
	return hex.EncodeToString(b)[:n]
}
