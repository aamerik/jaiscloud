// Package metastore implements the Dataproc Metastore v1 control-plane
// provider (metastore.googleapis.com/v1): the Glue Data Catalog analogue — thin
// management-plane CRUD over services, backups, and metadata imports, plus the
// google.longrunning.Operation returned by Create/Update/Delete (Dataproc
// LRO shape, not managedkafka's flattened {done,response}).
//
// This is control-plane only: the emulator never stands up a Hive Thrift /
// Iceberg metadata endpoint (Phase 3). Deferred operations (ExportMetadata,
// RestoreService, QueryMetadata, MoveTableToDatabase,
// AlterMetadataResourceLocation) fail loud with Unimplemented rather than
// silently succeeding. The DataprocMetastore proto defines no getIamPolicy /
// setIamPolicy / testIamPermissions rpcs, so there is no IAM surface to defer.
package metastore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	metastorestore "jaiscloud/internal/gcp/store/metastore"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// Provider handles Dataproc Metastore v1 service/backup/import resources.
type Provider struct {
	store metastorestore.Store
}

// New returns a Provider backed by the given store.
func New(s metastorestore.Store) *Provider {
	return &Provider{store: s}
}

// Reset wipes the store.
func (p *Provider) Reset(ctx context.Context) { p.store.Reset(ctx) }

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Metastore.CreateService":                 p.CreateService,
		"Metastore.GetService":                    p.GetService,
		"Metastore.ListServices":                  p.ListServices,
		"Metastore.UpdateService":                 p.UpdateService,
		"Metastore.DeleteService":                 p.DeleteService,
		"Metastore.CreateBackup":                  p.CreateBackup,
		"Metastore.GetBackup":                     p.GetBackup,
		"Metastore.ListBackups":                   p.ListBackups,
		"Metastore.DeleteBackup":                  p.DeleteBackup,
		"Metastore.CreateMetadataImport":          p.CreateMetadataImport,
		"Metastore.GetMetadataImport":             p.GetMetadataImport,
		"Metastore.ListMetadataImports":           p.ListMetadataImports,
		"Metastore.UpdateMetadataImport":          p.UpdateMetadataImport,
		"Metastore.GetOperation":                  p.GetOperation,
		"Metastore.ExportMetadata":                p.unimplemented("ExportMetadata"),
		"Metastore.RestoreService":                p.unimplemented("RestoreService"),
		"Metastore.QueryMetadata":                 p.unimplemented("QueryMetadata"),
		"Metastore.MoveTableToDatabase":           p.unimplemented("MoveTableToDatabase"),
		"Metastore.AlterMetadataResourceLocation": p.unimplemented("AlterMetadataResourceLocation"),
	}
}

func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

func bodyMap(body map[string]any, key string) map[string]any {
	if body == nil {
		return nil
	}
	m, _ := body[key].(map[string]any)
	return m
}

func bodyStringMap(body map[string]any, key string) map[string]string {
	m := bodyMap(body, key)
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

func bodyString(body map[string]any, key string) string {
	if body == nil {
		return ""
	}
	s, _ := body[key].(string)
	return s
}

func mapErr(err error) error {
	switch {
	case errors.Is(err, metastorestore.ErrNoSuchService):
		return model.NewProviderError("NotFound", "service not found", 404)
	case errors.Is(err, metastorestore.ErrNoSuchBackup):
		return model.NewProviderError("NotFound", "backup not found", 404)
	case errors.Is(err, metastorestore.ErrNoSuchMetadataImport):
		return model.NewProviderError("NotFound", "metadata import not found", 404)
	case errors.Is(err, metastorestore.ErrNoSuchOperation):
		return model.NewProviderError("NotFound", "operation not found", 404)
	case errors.Is(err, metastorestore.ErrAlreadyExists):
		return model.NewProviderError("AlreadyExists", "resource already exists", 409)
	}
	return err
}

func randomHex(n int) string {
	b := make([]byte, (n+1)/2)
	if _, err := rand.Read(b); err != nil {
		return strings.Repeat("0", n)
	}
	return hex.EncodeToString(b)[:n]
}

func formatTimestamp(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// endpointURI synthesizes the output-only thrift endpoint (no real Thrift
// server exists — the URI is derived from id+location, mirroring the real
// metastore convention of thrift://{id}.{region}.metastore:9083).
func endpointURI(id, location string) string {
	return fmt.Sprintf("thrift://%s.%s.metastore.jaiscloud.local:9083", id, location)
}

// serviceMap renders a store Service as a metastore.v1.Service wire map. The
// stored request body is echoed verbatim, then name/state/times/endpointUri are
// overlaid (output-only) and tier defaults to DEVELOPER.
func (p *Provider) serviceMap(nr *model.NormalizedRequest, svc metastorestore.Service) map[string]any {
	var out map[string]any
	if len(svc.Config) > 0 {
		_ = json.Unmarshal(svc.Config, &out)
	}
	if out == nil {
		out = map[string]any{}
	}
	out["name"] = nr.ResourceID("metastore-service", svc.Location+"/"+svc.Name)
	out["createTime"] = formatTimestamp(svc.CreateTime)
	out["updateTime"] = formatTimestamp(svc.UpdateTime)
	out["state"] = svc.State
	out["endpointUri"] = endpointURI(svc.Name, svc.Location)
	out["uid"] = randomHex(32)
	if _, ok := out["tier"]; !ok {
		out["tier"] = "DEVELOPER"
	}
	if _, ok := out["port"]; !ok {
		out["port"] = 9083
	}
	if _, ok := out["releaseChannel"]; !ok {
		out["releaseChannel"] = "STABLE"
	}
	if svc.Labels != nil {
		out["labels"] = svc.Labels
	}
	return out
}

func (p *Provider) backupMap(nr *model.NormalizedRequest, b metastorestore.Backup) map[string]any {
	out := map[string]any{
		"name":       nr.ResourceID("metastore-backup", b.Location+"/"+b.ServiceName+"/"+b.Name),
		"createTime": formatTimestamp(b.CreateTime),
		"endTime":    formatTimestamp(b.EndTime),
		"state":      b.State,
	}
	if b.Description != "" {
		out["description"] = b.Description
	}
	return out
}

func (p *Provider) metadataImportMap(nr *model.NormalizedRequest, mi metastorestore.MetadataImport) map[string]any {
	out := map[string]any{
		"name":       nr.ResourceID("metastore-metadata-import", mi.Location+"/"+mi.ServiceName+"/"+mi.Name),
		"createTime": formatTimestamp(mi.CreateTime),
		"updateTime": formatTimestamp(mi.UpdateTime),
		"endTime":    formatTimestamp(mi.EndTime),
		"state":      mi.State,
	}
	if mi.Description != "" {
		out["description"] = mi.Description
	}
	var cfg map[string]any
	if len(mi.Config) > 0 {
		_ = json.Unmarshal(mi.Config, &cfg)
	}
	if dump, ok := cfg["databaseDump"].(map[string]any); ok {
		out["databaseDump"] = dump
	}
	return out
}

// operationMap renders a stored Operation as a google.longrunning.Operation.
func (p *Provider) operationMap(nr *model.NormalizedRequest, op metastorestore.Operation) map[string]any {
	name := nr.ResourceID("metastore-operation", op.Location+"/"+op.ID)
	var metadata any = map[string]any{}
	if op.Metadata != "" {
		_ = json.Unmarshal([]byte(op.Metadata), &metadata)
	}
	out := map[string]any{
		"name":     name,
		"metadata": metadata,
		"done":     op.Done,
	}
	if op.Done && op.Response != "" {
		var response any = map[string]any{}
		if json.Unmarshal([]byte(op.Response), &response) == nil {
			out["response"] = response
		}
	}
	return out
}

// storeOperation persists a done operation and returns its wire map.
func (p *Provider) storeOperation(ctx context.Context, nr *model.NormalizedRequest, location, verb, target string, metadata, response map[string]any) (map[string]any, error) {
	now := clock.Now().UTC()
	op := metastorestore.Operation{
		ID:         randomHex(12),
		ProjectID:  nr.AccountID,
		Location:   location,
		Done:       true,
		Verb:       verb,
		Target:     target,
		CreateTime: now,
		EndTime:    now,
	}
	if metadata != nil {
		metaJSON, _ := json.Marshal(metadata)
		op.Metadata = string(metaJSON)
	}
	if response != nil {
		respJSON, _ := json.Marshal(response)
		op.Response = string(respJSON)
	}
	if err := p.store.CreateOperation(ctx, nr.AccountID, location, op); err != nil {
		return nil, err
	}
	return p.operationMap(nr, op), nil
}

// operationMetadata renders the metastore.v1.OperationMetadata for an operation.
func operationMetadata(target, verb string, start time.Time) map[string]any {
	return map[string]any{
		"@type":      "type.googleapis.com/google.cloud.metastore.v1.OperationMetadata",
		"createTime": formatTimestamp(start),
		"endTime":    formatTimestamp(start),
		"target":     target,
		"verb":       verb,
		"apiVersion": "v1",
	}
}

func (p *Provider) pageServices(nr *model.NormalizedRequest, services []metastorestore.Service) map[string]any {
	page, next := paging.Page(services, func(s metastorestore.Service) string { return s.Name }, nr.Params)
	items := make([]any, 0, len(page))
	for _, s := range page {
		items = append(items, p.serviceMap(nr, s))
	}
	resp := map[string]any{"services": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return resp
}

func (p *Provider) pageBackups(nr *model.NormalizedRequest, backups []metastorestore.Backup) map[string]any {
	page, next := paging.Page(backups, func(b metastorestore.Backup) string { return b.Name }, nr.Params)
	items := make([]any, 0, len(page))
	for _, b := range page {
		items = append(items, p.backupMap(nr, b))
	}
	resp := map[string]any{"backups": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return resp
}

func (p *Provider) pageImports(nr *model.NormalizedRequest, imports []metastorestore.MetadataImport) map[string]any {
	page, next := paging.Page(imports, func(m metastorestore.MetadataImport) string { return m.Name }, nr.Params)
	items := make([]any, 0, len(page))
	for _, m := range page {
		items = append(items, p.metadataImportMap(nr, m))
	}
	resp := map[string]any{"metadataImports": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return resp
}

// --- Services ---

func (p *Provider) CreateService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	serviceID := strParam(nr, "serviceId")
	if location == "" || serviceID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or serviceId", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	now := clock.Now().UTC()
	svc := metastorestore.Service{
		Location:     location,
		Name:         serviceID,
		Labels:       bodyStringMap(body, "labels"),
		State:        "ACTIVE",
		StateHistory: []metastorestore.ServiceState{{State: "CREATING", StateStartTime: now}},
		CreateTime:   now,
		UpdateTime:   now,
	}
	if body != nil {
		if data, err := json.Marshal(body); err == nil {
			svc.Config = data
		}
	}
	if err := p.store.CreateService(ctx, nr.AccountID, location, svc); err != nil {
		return nil, mapErr(err)
	}
	target := nr.ResourceID("metastore-service", location+"/"+serviceID)
	op, err := p.storeOperation(ctx, nr, location, "create", target,
		operationMetadata(target, "create", now), p.serviceMap(nr, svc))
	if err != nil {
		return nil, err
	}
	return provider.OK(op), nil
}

func (p *Provider) GetService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	serviceID := strParam(nr, "serviceId")
	if location == "" || serviceID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or serviceId", 400)
	}
	svc, err := p.store.GetService(ctx, nr.AccountID, location, serviceID)
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(p.serviceMap(nr, svc)), nil
}

func (p *Provider) ListServices(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	if location == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location", 400)
	}
	services, err := p.store.ListServices(ctx, nr.AccountID, location)
	if err != nil {
		return nil, err
	}
	return provider.OK(p.pageServices(nr, services)), nil
}

func (p *Provider) UpdateService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	serviceID := strParam(nr, "serviceId")
	if location == "" || serviceID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or serviceId", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	mask := strParam(nr, "updateMask")

	// UpdateServiceAtomic reads, merges, and writes under one lock so a
	// concurrent update/delete can't land between our read and our write.
	updated, err := p.store.UpdateServiceAtomic(ctx, nr.AccountID, location, serviceID, func(current metastorestore.Service) (metastorestore.Service, error) {
		apply := func(field string) bool {
			return mask == "" || containsMaskField(mask, field)
		}
		if apply("labels") {
			if labels := bodyStringMap(body, "labels"); labels != nil {
				current.Labels = labels
			}
		}
		// Echo the remaining body fields into the stored config verbatim
		// (top-level keys overwrite), honoring the updateMask's named paths.
		if body != nil {
			var stored map[string]any
			if len(current.Config) > 0 {
				_ = json.Unmarshal(current.Config, &stored)
			}
			if stored == nil {
				stored = map[string]any{}
			}
			if mask == "" {
				for k, v := range body {
					stored[k] = v
				}
			} else {
				for _, field := range splitMask(mask) {
					root, ok := maskJSONRoot(field)
					if !ok {
						return metastorestore.Service{}, model.NewProviderError("InvalidArgument", "unsupported updateMask field "+field, 400)
					}
					if v, present := body[root]; present {
						stored[root] = v
					}
				}
			}
			if data, err := json.Marshal(stored); err == nil {
				current.Config = data
			}
		}
		current.UpdateTime = clock.Now().UTC()
		return current, nil
	})
	if err != nil {
		return nil, mapErr(err)
	}
	target := nr.ResourceID("metastore-service", location+"/"+serviceID)
	op, err := p.storeOperation(ctx, nr, location, "update", target,
		operationMetadata(target, "update", clock.Now().UTC()), p.serviceMap(nr, updated))
	if err != nil {
		return nil, err
	}
	return provider.OK(op), nil
}

func (p *Provider) DeleteService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	serviceID := strParam(nr, "serviceId")
	if location == "" || serviceID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or serviceId", 400)
	}
	if err := p.store.DeleteService(ctx, nr.AccountID, location, serviceID); err != nil {
		return nil, mapErr(err)
	}
	target := nr.ResourceID("metastore-service", location+"/"+serviceID)
	op, err := p.storeOperation(ctx, nr, location, "delete", target,
		operationMetadata(target, "delete", clock.Now().UTC()), map[string]any{})
	if err != nil {
		return nil, err
	}
	return provider.OK(op), nil
}

// --- Backups ---

func (p *Provider) CreateBackup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	serviceID := strParam(nr, "serviceId")
	backupID := strParam(nr, "backupId")
	if location == "" || serviceID == "" || backupID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location, serviceId, or backupId", 400)
	}
	if _, err := p.store.GetService(ctx, nr.AccountID, location, serviceID); err != nil {
		return nil, mapErr(err)
	}
	body, _ := nr.Params["body"].(map[string]any)
	now := clock.Now().UTC()
	b := metastorestore.Backup{
		Location:    location,
		ServiceName: serviceID,
		Name:        backupID,
		Description: bodyString(body, "description"),
		State:       "ACTIVE",
		CreateTime:  now,
		EndTime:     now,
	}
	if body != nil {
		if data, err := json.Marshal(body); err == nil {
			b.Config = data
		}
	}
	if err := p.store.CreateBackup(ctx, nr.AccountID, location, serviceID, b); err != nil {
		return nil, mapErr(err)
	}
	target := nr.ResourceID("metastore-backup", location+"/"+serviceID+"/"+backupID)
	op, err := p.storeOperation(ctx, nr, location, "create", target,
		operationMetadata(target, "create", now), p.backupMap(nr, b))
	if err != nil {
		return nil, err
	}
	return provider.OK(op), nil
}

func (p *Provider) GetBackup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	serviceID := strParam(nr, "serviceId")
	backupID := strParam(nr, "backupId")
	if location == "" || serviceID == "" || backupID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location, serviceId, or backupId", 400)
	}
	b, err := p.store.GetBackup(ctx, nr.AccountID, location, serviceID, backupID)
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(p.backupMap(nr, b)), nil
}

func (p *Provider) ListBackups(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	serviceID := strParam(nr, "serviceId")
	if location == "" || serviceID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or serviceId", 400)
	}
	backups, err := p.store.ListBackups(ctx, nr.AccountID, location, serviceID)
	if err != nil {
		return nil, err
	}
	return provider.OK(p.pageBackups(nr, backups)), nil
}

func (p *Provider) DeleteBackup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	serviceID := strParam(nr, "serviceId")
	backupID := strParam(nr, "backupId")
	if location == "" || serviceID == "" || backupID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location, serviceId, or backupId", 400)
	}
	if err := p.store.DeleteBackup(ctx, nr.AccountID, location, serviceID, backupID); err != nil {
		return nil, mapErr(err)
	}
	target := nr.ResourceID("metastore-backup", location+"/"+serviceID+"/"+backupID)
	op, err := p.storeOperation(ctx, nr, location, "delete", target,
		operationMetadata(target, "delete", clock.Now().UTC()), map[string]any{})
	if err != nil {
		return nil, err
	}
	return provider.OK(op), nil
}

// --- Metadata imports ---

func (p *Provider) CreateMetadataImport(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	serviceID := strParam(nr, "serviceId")
	importID := strParam(nr, "metadataImportId")
	if location == "" || serviceID == "" || importID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location, serviceId, or metadataImportId", 400)
	}
	if _, err := p.store.GetService(ctx, nr.AccountID, location, serviceID); err != nil {
		return nil, mapErr(err)
	}
	body, _ := nr.Params["body"].(map[string]any)
	now := clock.Now().UTC()
	mi := metastorestore.MetadataImport{
		Location:    location,
		ServiceName: serviceID,
		Name:        importID,
		Description: bodyString(body, "description"),
		State:       "SUCCEEDED",
		CreateTime:  now,
		UpdateTime:  now,
		EndTime:     now,
	}
	if body != nil {
		if data, err := json.Marshal(body); err == nil {
			mi.Config = data
		}
	}
	if err := p.store.CreateMetadataImport(ctx, nr.AccountID, location, serviceID, mi); err != nil {
		return nil, mapErr(err)
	}
	target := nr.ResourceID("metastore-metadata-import", location+"/"+serviceID+"/"+importID)
	op, err := p.storeOperation(ctx, nr, location, "create", target,
		operationMetadata(target, "create", now), p.metadataImportMap(nr, mi))
	if err != nil {
		return nil, err
	}
	return provider.OK(op), nil
}

func (p *Provider) GetMetadataImport(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	serviceID := strParam(nr, "serviceId")
	importID := strParam(nr, "metadataImportId")
	if location == "" || serviceID == "" || importID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location, serviceId, or metadataImportId", 400)
	}
	mi, err := p.store.GetMetadataImport(ctx, nr.AccountID, location, serviceID, importID)
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(p.metadataImportMap(nr, mi)), nil
}

func (p *Provider) ListMetadataImports(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	serviceID := strParam(nr, "serviceId")
	if location == "" || serviceID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or serviceId", 400)
	}
	imports, err := p.store.ListMetadataImports(ctx, nr.AccountID, location, serviceID)
	if err != nil {
		return nil, err
	}
	return provider.OK(p.pageImports(nr, imports)), nil
}

func (p *Provider) UpdateMetadataImport(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	serviceID := strParam(nr, "serviceId")
	importID := strParam(nr, "metadataImportId")
	if location == "" || serviceID == "" || importID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location, serviceId, or metadataImportId", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	mask := strParam(nr, "updateMask")

	updated, err := p.store.UpdateMetadataImportAtomic(ctx, nr.AccountID, location, serviceID, importID, func(current metastorestore.MetadataImport) (metastorestore.MetadataImport, error) {
		if mask == "" || containsMaskField(mask, "description") {
			if desc, ok := body["description"].(string); ok {
				current.Description = desc
			}
		}
		now := clock.Now().UTC()
		current.UpdateTime = now
		current.EndTime = now
		return current, nil
	})
	if err != nil {
		return nil, mapErr(err)
	}
	target := nr.ResourceID("metastore-metadata-import", location+"/"+serviceID+"/"+importID)
	op, err := p.storeOperation(ctx, nr, location, "update", target,
		operationMetadata(target, "update", clock.Now().UTC()), p.metadataImportMap(nr, updated))
	if err != nil {
		return nil, err
	}
	return provider.OK(op), nil
}

// --- Operations ---

func (p *Provider) GetOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	opID := strParam(nr, "operationId")
	if location == "" || opID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or operationId", 400)
	}
	op, err := p.store.GetOperation(ctx, nr.AccountID, location, opID)
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(p.operationMap(nr, op)), nil
}

// unimplemented returns a handler that fails loud with Unimplemented for the
// deferred control-plane operations.
func (p *Provider) unimplemented(name string) provider.HandlerFunc {
	return func(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return nil, model.NewProviderError("Unimplemented", name+" is not supported by the emulator", 501)
	}
}

// containsMaskField reports whether a comma-separated updateMask contains the
// given top-level field (or a sub-path of it).
func containsMaskField(mask, field string) bool {
	for _, part := range splitMask(mask) {
		if part == field || strings.HasPrefix(part, field+".") {
			return true
		}
	}
	return false
}

func splitMask(mask string) []string {
	var out []string
	for _, p := range strings.Split(mask, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// metastoreNestedBlocks maps the proto snake_case nested config block roots to
// their camelCase JSON wire keys. An updateMask deeper than top-level whose
// root is absent here (e.g. a misspelled snake_case field) fails loud with
// InvalidArgument rather than silently no-op-ing.
var metastoreNestedBlocks = map[string]string{
	"hive_metastore_config":        "hiveMetastoreConfig",
	"network_config":               "networkConfig",
	"encryption_config":            "encryptionConfig",
	"telemetry_config":             "telemetryConfig",
	"scaling_config":               "scalingConfig",
	"maintenance_window":           "maintenanceWindow",
	"metadata_management_activity": "metadataManagementActivity",
}

// maskJSONRoot translates an updateMask field path to the top-level JSON key in
// the stored body. Single-word top-level fields (labels, tier, network, port)
// and already-camelCase roots pass through unchanged; snake_case nested config
// blocks are translated via metastoreNestedBlocks. A snake_case root that is
// not a known nested block returns ok=false so the caller fails loud.
func maskJSONRoot(path string) (root string, ok bool) {
	root = path
	if i := strings.IndexByte(path, '.'); i >= 0 {
		root = path[:i]
	}
	if !strings.Contains(root, "_") {
		return root, true
	}
	camel, known := metastoreNestedBlocks[root]
	if !known {
		return "", false
	}
	return camel, true
}
