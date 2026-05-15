package emroneks

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/pagination"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const rtJobTemplate = "emrcontainers_jobtemplate"

type jobTemplate struct {
	Id              string            `json:"id"`
	Name            string            `json:"name"`
	Arn             string            `json:"arn"`
	CreatedAt       time.Time         `json:"createdAt"`
	CreatedBy       string            `json:"createdBy"`
	Tags            map[string]string `json:"tags,omitempty"`
	JobTemplateData map[string]any    `json:"jobTemplateData"`
}

func jobTemplateStoreID(id string) string { return "jt/" + id }

// applyJobTemplateDefaults merges template defaults under request params.
// Keys present in request params are never overwritten (request takes precedence).
func applyJobTemplateDefaults(requestParams map[string]any, templateData map[string]any) map[string]any {
	merged := make(map[string]any, len(requestParams))
	for k, v := range requestParams {
		merged[k] = v
	}
	for k, v := range templateData {
		if _, exists := merged[k]; !exists {
			merged[k] = v
		}
	}
	return merged
}

func (p *EMRContainersProvider) saveJobTemplate(ctx context.Context, jt jobTemplate) error {
	data, _ := json.Marshal(jt)
	entry := store.ResourceEntry{Type: rtJobTemplate, ID: jobTemplateStoreID(jt.Id), Data: data}
	err := p.resources.Create(ctx, entry)
	if err == store.ErrAlreadyExists {
		return p.resources.Update(ctx, entry)
	}
	return err
}

func (p *EMRContainersProvider) loadJobTemplate(ctx context.Context, id string) (jobTemplate, error) {
	e, err := p.resources.Get(ctx, rtJobTemplate, jobTemplateStoreID(id))
	if err == store.ErrNotFound {
		return jobTemplate{}, &model.ProviderError{
			Code:       "ResourceNotFoundException",
			Message:    fmt.Sprintf("JobTemplate %s not found", id),
			HTTPStatus: http.StatusNotFound,
		}
	}
	if err != nil {
		return jobTemplate{}, err
	}
	var jt jobTemplate
	json.Unmarshal(e.Data, &jt)
	return jt, nil
}

// CreateJobTemplate creates a new job template.
func (p *EMRContainersProvider) CreateJobTemplate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "name")
	if name == "" {
		return nil, &model.ProviderError{Code: "ValidationException", Message: "name is required", HTTPStatus: http.StatusBadRequest}
	}
	jobTemplateData, _ := nr.Params["jobTemplateData"].(map[string]any)
	tags := parseTags(nr.Params)

	id := shortID()
	arn := nr.ResourceID("emr-job-template", id)
	jt := jobTemplate{
		Id:              id,
		Name:            name,
		Arn:             arn,
		CreatedAt:       time.Now().UTC(),
		Tags:            tags,
		JobTemplateData: jobTemplateData,
	}
	if err := p.saveJobTemplate(ctx, jt); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"id":   id,
		"name": name,
		"arn":  arn,
		"tags": tags,
	}), nil
}

// DescribeJobTemplate returns a single job template by ID.
func (p *EMRContainersProvider) DescribeJobTemplate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := pathID(nr, "templateId", "id")
	jt, err := p.loadJobTemplate(ctx, id)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"jobTemplate": map[string]any{
			"id":              jt.Id,
			"name":            jt.Name,
			"arn":             jt.Arn,
			"createdAt":       jt.CreatedAt.UTC().Format(time.RFC3339),
			"createdBy":       jt.CreatedBy,
			"tags":            jt.Tags,
			"jobTemplateData": jt.JobTemplateData,
		},
	}), nil
}

// DeleteJobTemplate deletes a job template by ID.
func (p *EMRContainersProvider) DeleteJobTemplate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := pathID(nr, "templateId", "id")
	if _, err := p.loadJobTemplate(ctx, id); err != nil {
		return nil, err
	}
	_ = p.resources.Delete(ctx, rtJobTemplate, jobTemplateStoreID(id))
	return provider.OK(map[string]any{"id": id}), nil
}

// ListJobTemplates returns a paginated list of job templates with optional name filter.
func (p *EMRContainersProvider) ListJobTemplates(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, rtJobTemplate, "jt/")
	if err != nil {
		return nil, err
	}

	nameFilter := strParam(nr.Params, "name")
	all := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var jt jobTemplate
		json.Unmarshal(e.Data, &jt)
		if nameFilter != "" && jt.Name != nameFilter {
			continue
		}
		all = append(all, map[string]any{
			"id":        jt.Id,
			"name":      jt.Name,
			"arn":       jt.Arn,
			"createdAt": jt.CreatedAt.UTC().Format(time.RFC3339),
			"tags":      jt.Tags,
		})
	}

	maxResults := 100
	if m, ok := nr.Params["maxResults"].(float64); ok && int(m) > 0 {
		maxResults = int(m)
	}
	token := strParam(nr.Params, "nextToken")

	page, nextToken, pErr := pagination.Paginate(all, maxResults, token, "ListJobTemplates")
	if pErr != nil {
		return nil, &model.ProviderError{Code: "ValidationException", Message: pErr.Error(), HTTPStatus: http.StatusBadRequest}
	}

	resp := map[string]any{"templates": page}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}
	return provider.OK(resp), nil
}
