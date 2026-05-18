// Package catalog — Glue Table Versions implementation.
package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/pagination"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// ─── Resource type ────────────────────────────────────────────────────────────

const rtTableVersion = "glue_table_version"

// ─── Types ────────────────────────────────────────────────────────────────────

type tableVersionEntry struct {
	DatabaseName string         `json:"DatabaseName"`
	TableName    string         `json:"TableName"`
	VersionId    string         `json:"VersionId"` // "1", "2", ...
	Table        map[string]any `json:"Table"`     // snapshot of the table at this version
	CreatedOn    time.Time      `json:"CreatedOn"`
}

// ─── ID helpers ───────────────────────────────────────────────────────────────

func tableVersionID(db, table, versionID string) string {
	return strings.ToLower(db) + "/" + strings.ToLower(table) + "/" + versionID
}

func tableVersionPrefix(db, table string) string {
	return strings.ToLower(db) + "/" + strings.ToLower(table) + "/"
}

// ─── Persistence helpers ──────────────────────────────────────────────────────

func (p *GlueProvider) saveTableVersion(ctx context.Context, account, region string, v tableVersionEntry) error {
	data, _ := json.Marshal(v)
	entry := store.ResourceEntry{Type: rtTableVersion, ID: tableVersionID(v.DatabaseName, v.TableName, v.VersionId), Data: data}
	return p.resources.Upsert(ctx, account, region, entry)
}

func (p *GlueProvider) loadTableVersion(ctx context.Context, account, region, db, table, versionID string) (tableVersionEntry, error) {
	e, err := p.resources.Get(ctx, account, region, rtTableVersion, tableVersionID(db, table, versionID))
	if err == store.ErrNotFound {
		return tableVersionEntry{}, &model.ProviderError{
			Code:       "NotFound",
			Message:    fmt.Sprintf("Table version %s not found for %s/%s", versionID, db, table),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	if err != nil {
		return tableVersionEntry{}, err
	}
	var v tableVersionEntry
	json.Unmarshal(e.Data, &v)
	return v, nil
}

// nextVersionID computes the next sequential version number for a table.
func (p *GlueProvider) nextVersionID(ctx context.Context, account, region, db, table string) string {
	entries, err := p.resources.List(ctx, account, region, rtTableVersion, tableVersionPrefix(db, table))
	if err != nil || len(entries) == 0 {
		return "1"
	}
	max := 0
	for _, e := range entries {
		var v tableVersionEntry
		if json.Unmarshal(e.Data, &v) == nil {
			if n, err := strconv.Atoi(v.VersionId); err == nil && n > max {
				max = n
			}
		}
	}
	return strconv.Itoa(max + 1)
}

// WriteTableVersion persists a snapshot of t as the next version.
// Called by UpdateTable before applying changes.
func (p *GlueProvider) WriteTableVersion(ctx context.Context, t glueTable) {
	// WriteTableVersion is called without account/region context — use empty strings
	// (global Glue catalog behavior; multi-account support can thread these later).
	versionID := p.nextVersionID(ctx, "", "", t.DatabaseName, t.Name)
	v := tableVersionEntry{
		DatabaseName: t.DatabaseName,
		TableName:    t.Name,
		VersionId:    versionID,
		Table:        tableToWire(t),
		CreatedOn:    time.Now(),
	}
	p.saveTableVersion(ctx, "", "", v) //nolint:errcheck
}

// WriteTableVersionWithAccount persists a snapshot using explicit account/region.
func (p *GlueProvider) WriteTableVersionWithAccount(ctx context.Context, account, region string, t glueTable) {
	versionID := p.nextVersionID(ctx, account, region, t.DatabaseName, t.Name)
	v := tableVersionEntry{
		DatabaseName: t.DatabaseName,
		TableName:    t.Name,
		VersionId:    versionID,
		Table:        tableToWire(t),
		CreatedOn:    time.Now(),
	}
	p.saveTableVersion(ctx, account, region, v) //nolint:errcheck
}

// ─── Table Version operations ─────────────────────────────────────────────────

func (p *GlueProvider) GetTableVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	db := strParam(nr.Params, "DatabaseName")
	table := strParam(nr.Params, "TableName")
	versionID := strParam(nr.Params, "VersionId")
	v, err := p.loadTableVersion(ctx, nr.AccountID, nr.Region, db, table, versionID)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"TableVersion": tableVersionToWire(v)}), nil
}

func (p *GlueProvider) GetTableVersions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	db := strParam(nr.Params, "DatabaseName")
	table := strParam(nr.Params, "TableName")
	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, rtTableVersion, tableVersionPrefix(db, table))
	if err != nil {
		return nil, err
	}
	versions := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var v tableVersionEntry
		if json.Unmarshal(e.Data, &v) == nil &&
			strings.EqualFold(v.DatabaseName, db) &&
			strings.EqualFold(v.TableName, table) {
			versions = append(versions, tableVersionToWire(v))
		}
	}
	// Sort ascending by numeric version id
	sort.Slice(versions, func(i, j int) bool {
		vi, _ := strconv.Atoi(fmt.Sprint(versions[i]["VersionId"]))
		vj, _ := strconv.Atoi(fmt.Sprint(versions[j]["VersionId"]))
		return vi < vj
	})
	maxResults := 100
	if v, ok := nr.Params["MaxResults"].(float64); ok && v > 0 {
		maxResults = int(v)
	}
	token, _ := nr.Params["NextToken"].(string)
	page, next, pgErr := pagination.Paginate(versions, maxResults, token, "GetTableVersions")
	if pgErr != nil {
		return nil, &model.ProviderError{Code: "InvalidInputException", Message: pgErr.Error(), HTTPStatus: http.StatusBadRequest}
	}
	data := map[string]any{"TableVersions": page}
	if next != "" {
		data["NextToken"] = next
	}
	return provider.OK(data), nil
}

func (p *GlueProvider) DeleteTableVersions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	db := strParam(nr.Params, "DatabaseName")
	table := strParam(nr.Params, "TableName")
	versionIDs := strSliceParam(nr.Params, "VersionIds")

	errors := []map[string]any{}
	for _, vid := range versionIDs {
		if err := p.resources.Delete(ctx, nr.AccountID, nr.Region, rtTableVersion, tableVersionID(db, table, vid)); err != nil {
			errors = append(errors, map[string]any{
				"TableName": table,
				"VersionId": vid,
				"ErrorDetail": map[string]any{
					"ErrorCode":    "EntityNotFoundException",
					"ErrorMessage": "Table version not found",
				},
			})
		}
	}
	return provider.OK(map[string]any{"Errors": errors}), nil
}

func (p *GlueProvider) BatchDeleteTableVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// Same as DeleteTableVersions — both take VersionIds list
	return p.DeleteTableVersions(ctx, nr)
}

// ─── Wire serialisation ───────────────────────────────────────────────────────

func tableVersionToWire(v tableVersionEntry) map[string]any {
	return map[string]any{
		"DatabaseName": v.DatabaseName,
		"Table":        v.Table,
		"VersionId":    v.VersionId,
	}
}
