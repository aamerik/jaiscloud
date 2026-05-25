package iam

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const rtPolicyVersion = "iam_policy_version"

type policyVersionData struct {
	PolicyArn        string    `json:"PolicyArn"`
	VersionId        string    `json:"VersionId"`
	Document         string    `json:"Document"`
	IsDefaultVersion bool      `json:"IsDefaultVersion"`
	CreateDate       time.Time `json:"CreateDate"`
}

func policyVersionKey(arn, versionId string) string {
	return arn + "|" + versionId
}

// nextVersionId increments the version counter for a policy ARN.
func (p *IAMProvider) nextVersionId(ctx context.Context, arn string) string {
	entries, _ := p.resources.List(ctx, "", "", rtPolicyVersion, "")
	max := 0
	prefix := arn + "|v"
	for _, e := range entries {
		if strings.HasPrefix(e.ID, prefix) {
			var n int
			fmt.Sscanf(e.ID[len(prefix):], "%d", &n)
			if n > max {
				max = n
			}
		}
	}
	return fmt.Sprintf("v%d", max+1)
}

func (p *IAMProvider) CreatePolicyVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "PolicyArn")
	doc := strParam(nr.Params, "PolicyDocument")
	if arn == "" || doc == "" {
		return nil, model.NewProviderError("ValidationError", "PolicyArn and PolicyDocument are required", http.StatusBadRequest)
	}
	setDefault := strParam(nr.Params, "SetAsDefault") == "true"

	// Enforce max 5 versions
	entries, _ := p.resources.List(ctx, "", "", rtPolicyVersion, "")
	count := 0
	for _, e := range entries {
		if strings.HasPrefix(e.ID, arn+"|") {
			count++
		}
	}
	if count >= 5 {
		return nil, model.NewProviderError("LimitExceeded", "Cannot create a new policy version: limit of 5 versions reached", http.StatusBadRequest)
	}

	versionId := p.nextVersionId(ctx, arn)
	now := clock.Now()

	if setDefault {
		// Clear existing default
		for _, e := range entries {
			if !strings.HasPrefix(e.ID, arn+"|") {
				continue
			}
			var v policyVersionData
			if json.Unmarshal(e.Data, &v) != nil || !v.IsDefaultVersion {
				continue
			}
			v.IsDefaultVersion = false
			data, _ := json.Marshal(v)
			_ = p.resources.Update(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtPolicyVersion, ID: e.ID, Data: data})
		}
	}

	v := policyVersionData{
		PolicyArn:        arn,
		VersionId:        versionId,
		Document:         doc,
		IsDefaultVersion: setDefault,
		CreateDate:       now,
	}
	data, _ := json.Marshal(v)
	_ = p.resources.Create(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtPolicyVersion, ID: policyVersionKey(arn, versionId), Data: data})

	return provider.OK(map[string]any{"PolicyVersion": policyVersionMap(v)}), nil
}

func (p *IAMProvider) GetPolicyVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "PolicyArn")
	versionId := strParam(nr.Params, "VersionId")
	var v policyVersionData
	if err := loadEntry(ctx, p.resources, nr.AccountID, rtPolicyVersion, policyVersionKey(arn, versionId), &v); err != nil {
		return nil, model.NewProviderError("NoSuchEntity", "Policy version not found", http.StatusNotFound)
	}
	return provider.OK(map[string]any{"PolicyVersion": policyVersionMap(v)}), nil
}

func (p *IAMProvider) DeletePolicyVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "PolicyArn")
	versionId := strParam(nr.Params, "VersionId")
	key := policyVersionKey(arn, versionId)
	var v policyVersionData
	if err := loadEntry(ctx, p.resources, nr.AccountID, rtPolicyVersion, key, &v); err != nil {
		return nil, model.NewProviderError("NoSuchEntity", "Policy version not found", http.StatusNotFound)
	}
	if v.IsDefaultVersion {
		return nil, model.NewProviderError("DeleteConflict", "Cannot delete the default version of a policy", http.StatusConflict)
	}
	_ = p.resources.Delete(ctx, nr.AccountID, store.GlobalRegion, rtPolicyVersion, key)
	return provider.OK(nil), nil
}

func (p *IAMProvider) ListPolicyVersions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "PolicyArn")
	entries, _ := p.resources.List(ctx, "", "", rtPolicyVersion, "")
	var versions []map[string]any
	for _, e := range entries {
		if !strings.HasPrefix(e.ID, arn+"|") {
			continue
		}
		var v policyVersionData
		if json.Unmarshal(e.Data, &v) == nil {
			versions = append(versions, policyVersionMap(v))
		}
	}
	if versions == nil {
		versions = []map[string]any{}
	}
	return provider.OK(map[string]any{"Versions": versions, "IsTruncated": false}), nil
}

func (p *IAMProvider) SetDefaultPolicyVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "PolicyArn")
	versionId := strParam(nr.Params, "VersionId")

	entries, _ := p.resources.List(ctx, "", "", rtPolicyVersion, "")
	found := false
	for _, e := range entries {
		if !strings.HasPrefix(e.ID, arn+"|") {
			continue
		}
		var v policyVersionData
		if json.Unmarshal(e.Data, &v) != nil {
			continue
		}
		shouldBeDefault := v.VersionId == versionId
		if shouldBeDefault {
			found = true
		}
		if v.IsDefaultVersion != shouldBeDefault {
			v.IsDefaultVersion = shouldBeDefault
			data, _ := json.Marshal(v)
			_ = p.resources.Update(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtPolicyVersion, ID: e.ID, Data: data})
		}
	}
	if !found {
		return nil, model.NewProviderError("NoSuchEntity", "Policy version not found", http.StatusNotFound)
	}
	return provider.OK(nil), nil
}

func policyVersionMap(v policyVersionData) map[string]any {
	return map[string]any{
		"VersionId":        v.VersionId,
		"Document":         v.Document,
		"IsDefaultVersion": v.IsDefaultVersion,
		"CreateDate":       v.CreateDate.UTC().Format(time.RFC3339),
	}
}
