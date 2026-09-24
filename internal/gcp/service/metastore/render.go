package metastore

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	metastorestore "jaiscloud/internal/gcp/store/metastore"
)

// operationMetadataType is the @type of the Dataproc Metastore v1
// OperationMetadata.
const operationMetadataType = "type.googleapis.com/google.cloud.metastore.v1.OperationMetadata"

// Any type URLs for the resources a done operation can carry as its response.
const (
	serviceTypeURL        = "type.googleapis.com/google.cloud.metastore.v1.Service"
	backupTypeURL         = "type.googleapis.com/google.cloud.metastore.v1.Backup"
	metadataImportTypeURL = "type.googleapis.com/google.cloud.metastore.v1.MetadataImport"
)

// randomHex returns n random hexadecimal characters. It is used only for
// operation ids (ephemeral), never for a resource's stable output-only uid.
func randomHex(n int) string {
	b := make([]byte, (n+1)/2)
	if _, err := rand.Read(b); err != nil {
		return strings.Repeat("0", n)
	}
	return hex.EncodeToString(b)[:n]
}

// serviceUID derives the stable, output-only uid for a service from its
// resource name. The real uid is a stable, globally unique id for the
// resource's lifetime; deriving it from the name (rather than randomizing per
// render) keeps Create/Get/List and the REST/gRPC renders consistent, and keeps
// it stable across restarts.
func serviceUID(name string) string {
	sum := sha256.Sum256([]byte(name))
	return hex.EncodeToString(sum[:16])
}

// operationResponse wraps a rendered resource body as the Any JSON a
// google.longrunning.Operation response carries, adding the required @type
// discriminator. body is copied, never mutated.
func operationResponse(typeURL string, body map[string]any) map[string]any {
	out := make(map[string]any, len(body)+1)
	out["@type"] = typeURL
	for k, v := range body {
		out[k] = v
	}
	return out
}

// formatTimestamp renders a business timestamp as the RFC3339Nano string the
// Discovery JSON shape (and protojson) uses.
func formatTimestamp(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// endpointURI synthesizes the output-only thrift endpoint (no real per-service
// Thrift server exists — the single Hive Metastore catalog is global, so the
// URI is cosmetic and derived from id+location, mirroring the real metastore
// convention of thrift://{id}.{region}.metastore:9083).
func endpointURI(id, location string) string {
	return fmt.Sprintf("thrift://%s.%s.metastore.jaiscloud.local:9083", id, location)
}

// ServiceJSON renders a stored service as the metastore.v1.Service wire map.
// The stored request body is echoed verbatim, then name/state/times/endpointUri
// are overlaid (output-only) and tier/port/releaseChannel default.
//
// It is the single source of truth for the derived fields: the gRPC transport
// also drives its proto rendering from this map (via protojson), so the two
// transports cannot drift.
func ServiceJSON(svc metastorestore.Service, project string) map[string]any {
	var out map[string]any
	if len(svc.Config) > 0 {
		_ = json.Unmarshal(svc.Config, &out)
	}
	if out == nil {
		out = map[string]any{}
	}
	name := ServiceName(project, svc.Location, svc.Name)
	out["name"] = name
	out["createTime"] = formatTimestamp(svc.CreateTime)
	out["updateTime"] = formatTimestamp(svc.UpdateTime)
	out["state"] = svc.State
	out["endpointUri"] = endpointURI(svc.Name, svc.Location)
	out["uid"] = serviceUID(name)
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

// BackupJSON renders a stored backup as the Discovery backup shape.
func BackupJSON(b metastorestore.Backup, project string) map[string]any {
	out := map[string]any{
		"name":       BackupName(project, b.Location, b.ServiceName, b.Name),
		"createTime": formatTimestamp(b.CreateTime),
		"endTime":    formatTimestamp(b.EndTime),
		"state":      b.State,
	}
	if b.Description != "" {
		out["description"] = b.Description
	}
	return out
}

// MetadataImportJSON renders a stored metadata import as the Discovery shape.
func MetadataImportJSON(mi metastorestore.MetadataImport, project string) map[string]any {
	out := map[string]any{
		"name":       MetadataImportName(project, mi.Location, mi.ServiceName, mi.Name),
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

// OperationMetadataJSON renders the metastore.v1.OperationMetadata for an
// operation. The emulator completes every control-plane mutation
// synchronously, so createTime and endTime are the same instant.
func OperationMetadataJSON(target, verb string, start time.Time) map[string]any {
	return map[string]any{
		"@type":      operationMetadataType,
		"createTime": formatTimestamp(start),
		"endTime":    formatTimestamp(start),
		"target":     target,
		"verb":       verb,
		"apiVersion": "v1",
	}
}

// OperationJSON renders a stored Operation as a google.longrunning.Operation.
// Metadata/Response are the canonical wire JSON persisted alongside it.
func OperationJSON(op metastorestore.Operation, project string) map[string]any {
	name := OperationName(project, op.Location, op.ID)
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

// storeOperation persists a done google.longrunning.Operation and returns it.
// Marshalling metadata/response cannot fail for the plain maps this package
// builds, so a marshal error is ignored and surfaces as an empty JSON object on
// read-back.
func (s *Service) storeOperation(ctx context.Context, project, location, verb, target string, response map[string]any) (metastorestore.Operation, error) {
	now := clock.Now().UTC()
	metaJSON, _ := json.Marshal(OperationMetadataJSON(target, verb, now))
	respJSON, _ := json.Marshal(response)
	op := metastorestore.Operation{
		ID:         randomHex(12),
		ProjectID:  project,
		Location:   location,
		Done:       true,
		Metadata:   string(metaJSON),
		Response:   string(respJSON),
		Verb:       verb,
		Target:     target,
		CreateTime: now,
		EndTime:    now,
	}
	if err := s.store.CreateOperation(ctx, project, location, op); err != nil {
		return metastorestore.Operation{}, err
	}
	return op, nil
}
