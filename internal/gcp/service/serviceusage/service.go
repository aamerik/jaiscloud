// Package serviceusage is the transport-neutral core of the Service Usage v1
// service (serviceusage.googleapis.com): a project's enabled service APIs.
//
// It deliberately has no dependency on protobuf or on NormalizedRequest: the
// gRPC transport (internal/gcp/transport/grpc/serviceusage) and the REST
// transport (internal/gcp/transport/rest/serviceusage) both transcode their wire
// format into this package's typed API and then call the SAME Service instance.
// That is the dual-protocol invariant: one core, one piece of state, so the
// transports cannot drift.
//
// It is an accept-and-succeed control plane, matching the emulator's posture:
// enabling a service flips it to ENABLED, disabling reverses it, and get/list
// echo that state. There is no real API gating or dependency resolution — a
// service works whether or not it was "enabled". State lives in the shared
// ResourceStore (memory + PostgreSQL backends), so no provider-level Reset or
// Snapshotter is needed.
//
// Mutations return a done google.longrunning.Operation (jaiscloud completes
// operations synchronously), so SDK/terraform operation futures resolve
// immediately.
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
	"jaiscloud/internal/store"
)

// rtService is the resource type in the shared ResourceStore. The entry id is
// the bare service name (e.g. "run.googleapis.com"); the account scope is the
// project.
const rtService = "gcp_serviceusage_service"

// maxBatchEnable mirrors real GCP's per-request batchEnable cap.
const maxBatchEnable = 20

// ListServices page-size contract: the default is 50 and the maximum is 200
// (serviceusage.googleapis.com Discovery, services.list pageSize).
const (
	defaultPageSize = 50
	maxPageSize     = 200
)

// State is a service's enablement state, per GoogleApiServiceusageV1Service.state.
type State string

const (
	// StateEnabled means the service is enabled for the consumer.
	StateEnabled State = "ENABLED"
	// StateDisabled means the service has been explicitly disabled or never enabled.
	StateDisabled State = "DISABLED"
)

// StateFilter narrows a ListAPIs result to one enablement state.
type StateFilter int

const (
	// FilterAll lists every tracked service.
	FilterAll StateFilter = iota
	// FilterEnabled lists only ENABLED services.
	FilterEnabled
	// FilterDisabled lists only DISABLED services.
	FilterDisabled
)

// API is the transport-neutral form of a GoogleApiServiceusageV1Service: the
// resource name, its consumer project, the configured DNS name, and the state.
type API struct {
	// Name is the full resource name: projects/{project}/services/{service}.
	Name string
	// Parent is the consumer project name: projects/{project}.
	Parent string
	// ConfigName is the service DNS name (e.g. "run.googleapis.com").
	ConfigName string
	// State is the enablement state.
	State State
}

// Operation is a completed google.longrunning.Operation. The emulator completes
// every mutation synchronously, so Done is always true and the operation is not
// persisted (its name is not addressable). The transport attaches the verb-
// appropriate response payload; the metadata carries the affected resource
// names, matching google.api.serviceusage.v1.OperationMetadata.
type Operation struct {
	// Name is the operation resource name: operations/{id}.
	Name string
	// ResourceNames are the full names of the services the operation touched.
	ResourceNames []string
	// Done is always true.
	Done bool
}

// Service is the transport-neutral Service Usage v1 service over the shared
// ResourceStore.
type Service struct {
	resources store.ResourceStore
}

// NewService returns a Service Usage core backed by the shared ResourceStore.
func NewService(resources store.ResourceStore) *Service {
	return &Service{resources: resources}
}

// ParseFilter maps a wire filter string ("state:ENABLED", "state:DISABLED", or
// empty) to a StateFilter. Real GCP accepts only the two state filters; anything
// else is an InvalidArgument.
func ParseFilter(filter string) (StateFilter, error) {
	switch strings.TrimSpace(filter) {
	case "":
		return FilterAll, nil
	case "state:ENABLED":
		return FilterEnabled, nil
	case "state:DISABLED":
		return FilterDisabled, nil
	default:
		return FilterAll, invalidArgument(fmt.Sprintf(
			"Invalid filter %s. The allowed filter strings are state:ENABLED and state:DISABLED.", filter))
	}
}

// ListAPIs returns a cursor page of the tracked services for the project that
// match the filter, plus the next-page token (empty when exhausted).
func (s *Service) ListAPIs(ctx context.Context, project string, filter StateFilter, pageSize int, pageToken string) ([]API, string, error) {
	if project == "" {
		return nil, "", invalidArgument("missing project")
	}
	entries, err := s.resources.List(ctx, project, store.GlobalRegion, rtService, "")
	if err != nil {
		return nil, "", err
	}
	apis := make([]API, 0, len(entries))
	for _, e := range entries {
		st := decodeRecord(e.Data).State
		if !filter.matches(st) {
			continue
		}
		apis = append(apis, buildAPI(project, e.ID, st))
	}
	// The API contract is: pageSize defaults to 50 and cannot exceed 200.
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	if pageSize > maxPageSize {
		pageSize = maxPageSize
	}
	params := map[string]any{"pageSize": pageSize}
	if pageToken != "" {
		params["pageToken"] = pageToken
	}
	page, next := paging.Page(apis, func(a API) string { return a.Name }, params)
	return page, next, nil
}

// GetAPI returns one service. An unknown service resolves to DISABLED rather
// than NotFound: real GCP lists every public API, and the emulator has no
// catalog, so a never-enabled id is simply disabled.
func (s *Service) GetAPI(ctx context.Context, project, service string) (API, error) {
	if project == "" || service == "" {
		return API{}, invalidArgument("missing project or service")
	}
	st, err := s.readState(ctx, project, service)
	if err != nil {
		return API{}, err
	}
	return buildAPI(project, service, st), nil
}

// EnableAPI flips a service to ENABLED and returns it with the done operation.
func (s *Service) EnableAPI(ctx context.Context, project, service string) (API, Operation, error) {
	if project == "" || service == "" {
		return API{}, Operation{}, invalidArgument("missing project or service")
	}
	api, err := s.setServiceState(ctx, project, service, StateEnabled)
	if err != nil {
		return API{}, Operation{}, err
	}
	return api, operation([]string{api.Name}), nil
}

// DisableAPI flips an ENABLED service to DISABLED and returns it with the done
// operation. Disabling a service that is not enabled is a FailedPrecondition,
// matching real GCP.
func (s *Service) DisableAPI(ctx context.Context, project, service string) (API, Operation, error) {
	if project == "" || service == "" {
		return API{}, Operation{}, invalidArgument("missing project or service")
	}
	st, err := s.readState(ctx, project, service)
	if err != nil {
		return API{}, Operation{}, err
	}
	if st != StateEnabled {
		return API{}, Operation{}, model.NewProviderError("FailedPrecondition",
			fmt.Sprintf("Service %s is not enabled for consumer projects/%s.", service, project), 400)
	}
	api, err := s.setServiceState(ctx, project, service, StateDisabled)
	if err != nil {
		return API{}, Operation{}, err
	}
	return api, operation([]string{api.Name}), nil
}

// BatchEnableAPIs enables up to maxBatchEnable services and returns them with
// the done operation. An empty or oversized batch is an InvalidArgument.
func (s *Service) BatchEnableAPIs(ctx context.Context, project string, serviceIDs []string) ([]API, Operation, error) {
	if project == "" {
		return nil, Operation{}, invalidArgument("missing project")
	}
	if len(serviceIDs) == 0 {
		return nil, Operation{}, invalidArgument("serviceIds must not be empty")
	}
	if len(serviceIDs) > maxBatchEnable {
		return nil, Operation{}, invalidArgument(fmt.Sprintf(
			"A single request can enable a maximum of %d services at a time.", maxBatchEnable))
	}
	apis := make([]API, 0, len(serviceIDs))
	names := make([]string, 0, len(serviceIDs))
	for _, id := range serviceIDs {
		// serviceIds are bare DNS identifiers (e.g. "run.googleapis.com"), not
		// resource names. A slash or an empty id is a malformed request; reject
		// it rather than persisting a bogus projects/{p}/services/... name.
		if id == "" || strings.Contains(id, "/") {
			return nil, Operation{}, invalidArgument(fmt.Sprintf("Invalid service id %q.", id))
		}
		api, err := s.setServiceState(ctx, project, id, StateEnabled)
		if err != nil {
			return nil, Operation{}, err
		}
		apis = append(apis, api)
		names = append(names, api.Name)
	}
	return apis, operation(names), nil
}

// setServiceState persists a service's state and returns its typed form. A
// storage failure is propagated (never swallowed) so an outage surfaces as a
// 5xx rather than a false done:true operation.
func (s *Service) setServiceState(ctx context.Context, project, service string, state State) (API, error) {
	data, err := json.Marshal(record{State: state})
	if err != nil {
		return API{}, err
	}
	if err := s.resources.Upsert(ctx, project, store.GlobalRegion,
		store.ResourceEntry{Type: rtService, ID: service, Data: data}); err != nil {
		return API{}, err
	}
	return buildAPI(project, service, state), nil
}

// readState returns the stored state, defaulting to DISABLED only when the
// service has never been enabled. A non-NotFound storage error is propagated.
func (s *Service) readState(ctx context.Context, project, service string) (State, error) {
	e, err := s.resources.Get(ctx, project, store.GlobalRegion, rtService, service)
	if errors.Is(err, store.ErrNotFound) {
		return StateDisabled, nil
	}
	if err != nil {
		return "", err
	}
	if st := decodeRecord(e.Data).State; st != "" {
		return st, nil
	}
	return StateDisabled, nil
}

// operation builds the completed operation descriptor for a mutation.
func operation(resourceNames []string) Operation {
	return Operation{
		Name:          OperationName(randomHex(12)),
		ResourceNames: resourceNames,
		Done:          true,
	}
}

// matches reports whether a state passes the filter.
func (f StateFilter) matches(st State) bool {
	switch f {
	case FilterEnabled:
		return st == StateEnabled
	case FilterDisabled:
		return st == StateDisabled
	default:
		return true
	}
}

// record is the stored per-service state.
type record struct {
	State State `json:"state"`
}

func decodeRecord(data json.RawMessage) record {
	var r record
	_ = json.Unmarshal(data, &r)
	return r
}

// invalidArgument builds the canonical InvalidArgument provider error both
// transports map onto their wire status.
func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

// randomHex returns n random hexadecimal characters (zero-filled on RNG
// failure, which never blocks a mutation).
func randomHex(n int) string {
	b := make([]byte, (n+1)/2)
	if _, err := rand.Read(b); err != nil {
		return strings.Repeat("0", n)
	}
	return hex.EncodeToString(b)[:n]
}
