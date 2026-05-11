// Package ecr implements the AWS ECR provider (Elastic Container Registry) in lite mode.
// All operations are metadata-only; no real image layer storage or Docker push/pull.
package ecr

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"regexp"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	ecrstore "jaiscloud/internal/store/aws/ecr"
)

var repoNameRe = regexp.MustCompile(`^(?:[a-z0-9]+(?:[._-][a-z0-9]+)*/)*[a-z0-9]+(?:[._-][a-z0-9]+)*$`)

type Provider struct {
	store *ecrstore.MemoryECRStore
}

func New(store *ecrstore.MemoryECRStore) *Provider {
	return &Provider{store: store}
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		// Repository CRUD
		"ECR.CreateRepository":   p.CreateRepository,
		"ECR.DeleteRepository":   p.DeleteRepository,
		"ECR.DescribeRepositories": p.DescribeRepositories,

		// Image operations
		"ECR.PutImage":         p.PutImage,
		"ECR.BatchGetImage":    p.BatchGetImage,
		"ECR.BatchDeleteImage": p.BatchDeleteImage,
		"ECR.ListImages":       p.ListImages,
		"ECR.DescribeImages":   p.DescribeImages,

		// Auth
		"ECR.GetAuthorizationToken": p.GetAuthorizationToken,

		// Lifecycle policy
		"ECR.PutLifecyclePolicy":    p.PutLifecyclePolicy,
		"ECR.GetLifecyclePolicy":    p.GetLifecyclePolicy,
		"ECR.DeleteLifecyclePolicy": p.DeleteLifecyclePolicy,
		"ECR.StartLifecyclePolicyPreview": p.StartLifecyclePolicyPreview,
		"ECR.GetLifecyclePolicyPreview":   p.GetLifecyclePolicyPreview,

		// Repository policy
		"ECR.PutRepositoryPolicy":    p.PutRepositoryPolicy,
		"ECR.GetRepositoryPolicy":    p.GetRepositoryPolicy,
		"ECR.DeleteRepositoryPolicy": p.DeleteRepositoryPolicy,

		// Tags
		"ECR.TagResource":        p.TagResource,
		"ECR.UntagResource":      p.UntagResource,
		"ECR.ListTagsForResource": p.ListTagsForResource,

		// Scanning stubs
		"ECR.StartImageScan":             p.StartImageScan,
		"ECR.DescribeImageScanFindings":  p.DescribeImageScanFindings,

		// Pull-through cache
		"ECR.CreatePullThroughCacheRule":   p.CreatePullThroughCacheRule,
		"ECR.DescribePullThroughCacheRules": p.DescribePullThroughCacheRules,
		"ECR.DeletePullThroughCacheRule":   p.DeletePullThroughCacheRule,

		// Registry-level
		"ECR.PutRegistryPolicy":         p.PutRegistryPolicy,
		"ECR.GetRegistryPolicy":         p.GetRegistryPolicy,
		"ECR.DeleteRegistryPolicy":      p.DeleteRegistryPolicy,
		"ECR.DescribeRegistry":          p.DescribeRegistry,
		"ECR.PutReplicationConfiguration": p.PutReplicationConfiguration,
		"ECR.DescribeRepositoriesForReplication": p.DescribeRepositoriesForReplication,
	}
}

func (p *Provider) Reset() { p.store.Reset() }

// ─── Repository CRUD ─────────────────────────────────────────────────────────

func (p *Provider) CreateRepository(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["repositoryName"].(string)
	if !validateRepoName(name) {
		return nil, &model.ProviderError{Code: "InvalidParameterException", Message: "Invalid parameter at 'repositoryName' failed to satisfy constraint: repository name must match pattern", HTTPStatus: 400}
	}

	mutability := "MUTABLE"
	if m, ok := nr.Params["imageTagMutability"].(string); ok && m != "" {
		mutability = m
	}

	encType := "AES256"
	kmsKey := ""
	if ec, ok := nr.Params["encryptionConfiguration"].(map[string]any); ok {
		if t, ok := ec["encryptionType"].(string); ok && t != "" {
			encType = t
		}
		if k, ok := ec["kmsKey"].(string); ok {
			kmsKey = k
		}
	}

	scanOnPush := false
	if isc, ok := nr.Params["imageScanningConfiguration"].(map[string]any); ok {
		scanOnPush, _ = isc["scanOnPush"].(bool)
	}

	arn := nr.ResourceID("ecr-repository", name)
	uri := fmt.Sprintf("%s.dkr.ecr.%s.amazonaws.com/%s", nr.AccountID, nr.Region, name)

	inputTags := parseTags(nr.Params["tags"])

	repo := &ecrstore.Repository{
		RegistryID:         nr.AccountID,
		Name:               name,
		ARN:                arn,
		URI:                uri,
		CreatedAt:          time.Now(),
		ImageTagMutability: mutability,
		ImageScanningConfig: ecrstore.ImageScanningConfig{ScanOnPush: scanOnPush},
		EncryptionConfig:    ecrstore.EncryptionConfig{EncryptionType: encType, KMSKey: kmsKey},
		Images:             make(map[string]*ecrstore.Image),
		Tags:               inputTags,
	}

	if err := p.store.CreateRepository(repo); err != nil {
		return nil, storeErrToProvider(err)
	}

	return provider.OK(map[string]any{
		"repository": repoToMap(repo),
	}), nil
}

func (p *Provider) DeleteRepository(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["repositoryName"].(string)
	force, _ := nr.Params["force"].(bool)

	repo, err := p.store.GetRepository(name)
	if err != nil {
		return nil, storeErrToProvider(err)
	}

	if err := p.store.DeleteRepository(name, force); err != nil {
		return nil, storeErrToProvider(err)
	}

	return provider.OK(map[string]any{
		"repository": repoToMap(repo),
	}), nil
}

func (p *Provider) DescribeRepositories(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	var names []string
	if ns, ok := nr.Params["repositoryNames"].([]any); ok {
		for _, n := range ns {
			if s, ok := n.(string); ok {
				names = append(names, s)
			}
		}
	}

	maxResults := 100
	if mr, ok := nr.Params["maxResults"].(float64); ok && mr > 0 {
		maxResults = int(mr)
	}
	nextToken, _ := nr.Params["nextToken"].(string)

	var repos []*ecrstore.Repository
	if len(names) > 0 {
		for _, name := range names {
			r, err := p.store.GetRepository(name)
			if err != nil {
				return nil, storeErrToProvider(err)
			}
			repos = append(repos, r)
		}
	} else {
		repos = p.store.ListRepositories("")
	}

	// Pagination
	start := 0
	if nextToken != "" {
		for i, r := range repos {
			if r.Name == nextToken {
				start = i
				break
			}
		}
	}
	end := start + maxResults
	var outNextToken *string
	if end < len(repos) {
		t := repos[end].Name
		outNextToken = &t
	} else {
		end = len(repos)
	}
	page := repos[start:end]

	repoMaps := make([]any, len(page))
	for i, r := range page {
		repoMaps[i] = repoToMap(r)
	}

	resp := map[string]any{"repositories": repoMaps}
	if outNextToken != nil {
		resp["nextToken"] = *outNextToken
	}
	return provider.OK(resp), nil
}

// ─── Image Operations ─────────────────────────────────────────────────────────

func (p *Provider) PutImage(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	repoName, _ := nr.Params["repositoryName"].(string)
	manifest, _ := nr.Params["imageManifest"].(string)
	mediaType, _ := nr.Params["imageManifestMediaType"].(string)
	imageTag, _ := nr.Params["imageTag"].(string)

	if manifest == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterException", Message: "imageManifest is required", HTTPStatus: 400}
	}
	if mediaType == "" {
		mediaType = "application/vnd.docker.distribution.manifest.v2+json"
	}

	digest := computeDigest(manifest)
	if supplied, ok := nr.Params["imageDigest"].(string); ok && supplied != "" && supplied != digest {
		return nil, &model.ProviderError{Code: "LayerInaccessibleException", Message: "The supplied image digest does not match the computed digest", HTTPStatus: 400}
	}

	var tags []string
	if imageTag != "" {
		tags = []string{imageTag}
	}

	img := &ecrstore.Image{
		Digest:            digest,
		Manifest:          manifest,
		ManifestMediaType: mediaType,
		Tags:              tags,
		PushedAt:          time.Now(),
		Size:              int64(len(manifest)),
	}

	if err := p.store.PutImage(repoName, img); err != nil {
		return nil, storeErrToProvider(err)
	}

	imageID := map[string]any{"imageDigest": digest}
	if imageTag != "" {
		imageID["imageTag"] = imageTag
	}

	return provider.OK(map[string]any{
		"image": map[string]any{
			"registryId":        nr.AccountID,
			"repositoryName":    repoName,
			"imageId":           imageID,
			"imageManifest":     manifest,
			"imageManifestMediaType": mediaType,
		},
	}), nil
}

func (p *Provider) BatchGetImage(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	repoName, _ := nr.Params["repositoryName"].(string)
	ids := parseImageIDs(nr.Params["imageIds"])

	found, notFound := p.store.BatchGetImages(repoName, ids)

	images := make([]any, len(found))
	for i, img := range found {
		imageID := map[string]any{"imageDigest": img.Digest}
		if len(img.Tags) > 0 {
			imageID["imageTag"] = img.Tags[0]
		}
		images[i] = map[string]any{
			"registryId":             nr.AccountID,
			"repositoryName":         repoName,
			"imageId":                imageID,
			"imageManifest":          img.Manifest,
			"imageManifestMediaType": img.ManifestMediaType,
		}
	}

	failures := make([]any, len(notFound))
	for i, id := range notFound {
		failures[i] = map[string]any{
			"imageId":       imageIDToMap(id),
			"failureCode":   "ImageNotFound",
			"failureReason": "Requested image not found",
		}
	}

	return provider.OK(map[string]any{
		"images":   images,
		"failures": failures,
	}), nil
}

func (p *Provider) BatchDeleteImage(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	repoName, _ := nr.Params["repositoryName"].(string)
	ids := parseImageIDs(nr.Params["imageIds"])

	deleted, failed := p.store.BatchDeleteImages(repoName, ids)

	deletedMaps := make([]any, len(deleted))
	for i, id := range deleted {
		deletedMaps[i] = imageIDToMap(id)
	}

	failedMaps := make([]any, len(failed))
	for i, f := range failed {
		failedMaps[i] = map[string]any{
			"imageId":       imageIDToMap(f.ImageID),
			"failureCode":   f.FailureCode,
			"failureReason": f.FailureReason,
		}
	}

	return provider.OK(map[string]any{
		"imageIds": deletedMaps,
		"failures": failedMaps,
	}), nil
}

func (p *Provider) ListImages(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	repoName, _ := nr.Params["repositoryName"].(string)

	tagStatus := "ANY"
	if filter, ok := nr.Params["filter"].(map[string]any); ok {
		if ts, ok := filter["tagStatus"].(string); ok && ts != "" {
			tagStatus = ts
		}
	}

	maxResults := 100
	if mr, ok := nr.Params["maxResults"].(float64); ok && mr > 0 {
		maxResults = int(mr)
	}

	ids := p.store.ListImages(repoName, tagStatus)

	// Simple token-based pagination using index
	nextToken, _ := nr.Params["nextToken"].(string)
	start := 0
	if nextToken != "" {
		for i, id := range ids {
			if fmt.Sprintf("%s:%s", id.ImageDigest, id.ImageTag) == nextToken {
				start = i
				break
			}
		}
	}
	end := start + maxResults
	var outNextToken *string
	if end < len(ids) {
		id := ids[end]
		t := fmt.Sprintf("%s:%s", id.ImageDigest, id.ImageTag)
		outNextToken = &t
	} else {
		end = len(ids)
	}
	page := ids[start:end]

	idMaps := make([]any, len(page))
	for i, id := range page {
		idMaps[i] = imageIDToMap(id)
	}

	resp := map[string]any{"imageIds": idMaps}
	if outNextToken != nil {
		resp["nextToken"] = *outNextToken
	}
	return provider.OK(resp), nil
}

func (p *Provider) DescribeImages(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	repoName, _ := nr.Params["repositoryName"].(string)
	ids := parseImageIDs(nr.Params["imageIds"])

	images := p.store.DescribeImages(repoName, ids)

	details := make([]any, len(images))
	for i, img := range images {
		d := map[string]any{
			"registryId":             nr.AccountID,
			"repositoryName":         repoName,
			"imageDigest":            img.Digest,
			"imageTags":              img.Tags,
			"imageSizeInBytes":       img.Size,
			"imagePushedAt":          img.PushedAt.Unix(),
			"imageManifestMediaType": img.ManifestMediaType,
			"imageScanStatus": map[string]any{
				"status":      "COMPLETE",
				"description": "",
			},
			"imageScanFindingsSummary": map[string]any{
				"imageScanCompletedAt":         time.Now().Unix(),
				"vulnerabilitySourceUpdatedAt": time.Now().Unix(),
				"findingSeverityCounts":        map[string]int{},
			},
		}
		if img.Tags == nil {
			d["imageTags"] = []string{}
		}
		details[i] = d
	}

	return provider.OK(map[string]any{"imageDetails": details}), nil
}

// ─── GetAuthorizationToken ────────────────────────────────────────────────────

func (p *Provider) GetAuthorizationToken(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	var registryIDs []string
	if ids, ok := nr.Params["registryIds"].([]any); ok {
		for _, id := range ids {
			if s, ok := id.(string); ok {
				registryIDs = append(registryIDs, s)
			}
		}
	}
	if len(registryIDs) == 0 {
		registryIDs = []string{nr.AccountID}
	}

	expiresAt := time.Now().Add(12 * time.Hour)
	authData := make([]any, len(registryIDs))
	for i, id := range registryIDs {
		authData[i] = map[string]any{
			"authorizationToken": generateAuthToken(),
			"expiresAt":          expiresAt.Unix(),
			"proxyEndpoint":      fmt.Sprintf("https://%s.dkr.ecr.%s.amazonaws.com", id, nr.Region),
		}
	}

	return provider.OK(map[string]any{"authorizationData": authData}), nil
}

// ─── Lifecycle Policy ─────────────────────────────────────────────────────────

func (p *Provider) PutLifecyclePolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	repoName, _ := nr.Params["repositoryName"].(string)
	policyText, _ := nr.Params["lifecyclePolicyText"].(string)

	if err := p.store.PutLifecyclePolicy(repoName, policyText); err != nil {
		return nil, storeErrToProvider(err)
	}

	return provider.OK(map[string]any{
		"registryId":          nr.AccountID,
		"repositoryName":      repoName,
		"lifecyclePolicyText": policyText,
		"lastEvaluatedAt":     time.Now().Unix(),
	}), nil
}

func (p *Provider) GetLifecyclePolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	repoName, _ := nr.Params["repositoryName"].(string)

	policy, err := p.store.GetLifecyclePolicy(repoName)
	if err != nil {
		return nil, storeErrToProvider(err)
	}

	return provider.OK(map[string]any{
		"registryId":          nr.AccountID,
		"repositoryName":      repoName,
		"lifecyclePolicyText": policy,
		"lastEvaluatedAt":     time.Now().Unix(),
	}), nil
}

func (p *Provider) DeleteLifecyclePolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	repoName, _ := nr.Params["repositoryName"].(string)

	policy, err := p.store.GetLifecyclePolicy(repoName)
	if err != nil {
		return nil, storeErrToProvider(err)
	}

	if err := p.store.DeleteLifecyclePolicy(repoName); err != nil {
		return nil, storeErrToProvider(err)
	}

	return provider.OK(map[string]any{
		"registryId":          nr.AccountID,
		"repositoryName":      repoName,
		"lifecyclePolicyText": policy,
		"lastEvaluatedAt":     time.Now().Unix(),
	}), nil
}

func (p *Provider) StartLifecyclePolicyPreview(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	repoName, _ := nr.Params["repositoryName"].(string)
	return provider.OK(map[string]any{
		"registryId":     nr.AccountID,
		"repositoryName": repoName,
		"status":         "COMPLETE",
	}), nil
}

func (p *Provider) GetLifecyclePolicyPreview(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	repoName, _ := nr.Params["repositoryName"].(string)
	return provider.OK(map[string]any{
		"registryId":     nr.AccountID,
		"repositoryName": repoName,
		"status":         "COMPLETE",
		"previewResults": []any{},
		"summary":        map[string]any{"expiringImageTotalCount": 0},
	}), nil
}

// ─── Repository Policy ────────────────────────────────────────────────────────

func (p *Provider) PutRepositoryPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	repoName, _ := nr.Params["repositoryName"].(string)
	policyText, _ := nr.Params["policyText"].(string)

	if err := p.store.PutRepositoryPolicy(repoName, policyText); err != nil {
		return nil, storeErrToProvider(err)
	}

	return provider.OK(map[string]any{
		"registryId":     nr.AccountID,
		"repositoryName": repoName,
		"policyText":     policyText,
	}), nil
}

func (p *Provider) GetRepositoryPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	repoName, _ := nr.Params["repositoryName"].(string)

	policy, err := p.store.GetRepositoryPolicy(repoName)
	if err != nil {
		return nil, storeErrToProvider(err)
	}

	return provider.OK(map[string]any{
		"registryId":     nr.AccountID,
		"repositoryName": repoName,
		"policyText":     policy,
	}), nil
}

func (p *Provider) DeleteRepositoryPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	repoName, _ := nr.Params["repositoryName"].(string)

	policy, err := p.store.GetRepositoryPolicy(repoName)
	if err != nil {
		return nil, storeErrToProvider(err)
	}

	if err := p.store.DeleteRepositoryPolicy(repoName); err != nil {
		return nil, storeErrToProvider(err)
	}

	return provider.OK(map[string]any{
		"registryId":     nr.AccountID,
		"repositoryName": repoName,
		"policyText":     policy,
	}), nil
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func (p *Provider) TagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn, _ := nr.Params["resourceArn"].(string)
	tags := parseTags(nr.Params["tags"])

	if err := p.store.AddTags(arn, tags); err != nil {
		return nil, storeErrToProvider(err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) UntagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn, _ := nr.Params["resourceArn"].(string)
	var keys []string
	if ks, ok := nr.Params["tagKeys"].([]any); ok {
		for _, k := range ks {
			if s, ok := k.(string); ok {
				keys = append(keys, s)
			}
		}
	}

	if err := p.store.RemoveTags(arn, keys); err != nil {
		return nil, storeErrToProvider(err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) ListTagsForResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn, _ := nr.Params["resourceArn"].(string)
	tags := p.store.GetTags(arn)

	tagList := make([]any, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]any{"Key": k, "Value": v})
	}
	return provider.OK(map[string]any{"tags": tagList}), nil
}

// ─── Image Scanning Stubs ─────────────────────────────────────────────────────

func (p *Provider) StartImageScan(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	repoName, _ := nr.Params["repositoryName"].(string)
	imageIDParam := parseImageID(nr.Params["imageId"])

	img, err := p.store.GetImage(repoName, imageIDParam)
	if err != nil {
		return nil, storeErrToProvider(err)
	}

	return provider.OK(map[string]any{
		"registryId":     nr.AccountID,
		"repositoryName": repoName,
		"imageId": map[string]any{
			"imageDigest": img.Digest,
			"imageTag":    firstTag(img.Tags),
		},
		"imageScanStatus": map[string]any{
			"status":      "COMPLETE",
			"description": "",
		},
	}), nil
}

func (p *Provider) DescribeImageScanFindings(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	repoName, _ := nr.Params["repositoryName"].(string)
	imageIDParam := parseImageID(nr.Params["imageId"])

	img, err := p.store.GetImage(repoName, imageIDParam)
	if err != nil {
		return nil, storeErrToProvider(err)
	}

	now := time.Now().Unix()
	return provider.OK(map[string]any{
		"registryId":     nr.AccountID,
		"repositoryName": repoName,
		"imageId": map[string]any{
			"imageDigest": img.Digest,
			"imageTag":    firstTag(img.Tags),
		},
		"imageScanStatus": map[string]any{
			"status":      "COMPLETE",
			"description": "",
		},
		"imageScanFindings": map[string]any{
			"imageScanCompletedAt":         now,
			"vulnerabilitySourceUpdatedAt": now,
			"findings":                    []any{},
			"findingSeverityCounts":        map[string]int{},
		},
	}), nil
}

// ─── Pull-Through Cache ───────────────────────────────────────────────────────

func (p *Provider) CreatePullThroughCacheRule(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	prefix, _ := nr.Params["ecrRepositoryPrefix"].(string)
	upstreamURL, _ := nr.Params["upstreamRegistryUrl"].(string)

	rule := &ecrstore.PullThroughCacheRule{
		EcrRepositoryPrefix: prefix,
		UpstreamRegistryURL: upstreamURL,
		CreatedAt:           time.Now(),
	}
	if err := p.store.CreatePullThroughCacheRule(rule); err != nil {
		return nil, storeErrToProvider(err)
	}

	return provider.OK(map[string]any{
		"ecrRepositoryPrefix": prefix,
		"upstreamRegistryUrl": upstreamURL,
		"registryId":          nr.AccountID,
		"createdAt":           rule.CreatedAt.Unix(),
	}), nil
}

func (p *Provider) DescribePullThroughCacheRules(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	rules := p.store.ListPullThroughCacheRules()
	out := make([]any, len(rules))
	for i, r := range rules {
		out[i] = map[string]any{
			"ecrRepositoryPrefix": r.EcrRepositoryPrefix,
			"upstreamRegistryUrl": r.UpstreamRegistryURL,
			"registryId":          nr.AccountID,
			"createdAt":           r.CreatedAt.Unix(),
		}
	}
	return provider.OK(map[string]any{"pullThroughCacheRules": out}), nil
}

func (p *Provider) DeletePullThroughCacheRule(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	prefix, _ := nr.Params["ecrRepositoryPrefix"].(string)
	if err := p.store.DeletePullThroughCacheRule(prefix); err != nil {
		return nil, storeErrToProvider(err)
	}
	return provider.OK(map[string]any{
		"ecrRepositoryPrefix": prefix,
		"registryId":          nr.AccountID,
	}), nil
}

// ─── Registry-Level ───────────────────────────────────────────────────────────

func (p *Provider) PutRegistryPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	policy, _ := nr.Params["policyText"].(string)
	p.store.PutRegistryPolicy(policy)
	return provider.OK(map[string]any{
		"registryId": nr.AccountID,
		"policyText": policy,
	}), nil
}

func (p *Provider) GetRegistryPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	policy, err := p.store.GetRegistryPolicy()
	if err != nil {
		return nil, storeErrToProvider(err)
	}
	return provider.OK(map[string]any{
		"registryId": nr.AccountID,
		"policyText": policy,
	}), nil
}

func (p *Provider) DeleteRegistryPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	p.store.DeleteRegistryPolicy()
	return provider.OK(map[string]any{"registryId": nr.AccountID}), nil
}

func (p *Provider) DescribeRegistry(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{
		"registryId": nr.AccountID,
		"replicationConfiguration": map[string]any{
			"rules": []any{},
		},
	}), nil
}

func (p *Provider) PutReplicationConfiguration(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// Store config for DescribeRegistry (no actual replication)
	return provider.OK(map[string]any{
		"replicationConfiguration": nr.Params["replicationConfiguration"],
	}), nil
}

func (p *Provider) DescribeRepositoriesForReplication(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{"repositories": []any{}}), nil
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func validateRepoName(name string) bool {
	if len(name) < 2 || len(name) > 256 {
		return false
	}
	return repoNameRe.MatchString(name)
}

func computeDigest(manifest string) string {
	h := sha256.Sum256([]byte(manifest))
	return "sha256:" + hex.EncodeToString(h[:])
}

func generateAuthToken() string {
	pwBytes := make([]byte, 32)
	_, _ = rand.Read(pwBytes)
	pw := base64.StdEncoding.EncodeToString(pwBytes)
	creds := fmt.Sprintf("AWS:%s", pw)
	return base64.StdEncoding.EncodeToString([]byte(creds))
}

func storeErrToProvider(err error) *model.ProviderError {
	if e := ecrstore.AsECRError(err); e != nil {
		return &model.ProviderError{Code: e.Code, Message: e.Message, HTTPStatus: e.Status}
	}
	return &model.ProviderError{Code: "InternalFailure", Message: err.Error(), HTTPStatus: 500}
}

func repoToMap(r *ecrstore.Repository) map[string]any {
	return map[string]any{
		"repositoryArn":          r.ARN,
		"registryId":             r.RegistryID,
		"repositoryName":         r.Name,
		"repositoryUri":          r.URI,
		"createdAt":              r.CreatedAt.Unix(),
		"imageTagMutability":     r.ImageTagMutability,
		"imageScanningConfiguration": map[string]any{
			"scanOnPush": r.ImageScanningConfig.ScanOnPush,
		},
		"encryptionConfiguration": map[string]any{
			"encryptionType": r.EncryptionConfig.EncryptionType,
			"kmsKey":         r.EncryptionConfig.KMSKey,
		},
	}
}

func imageIDToMap(id ecrstore.ImageIdentifier) map[string]any {
	m := make(map[string]any)
	if id.ImageDigest != "" {
		m["imageDigest"] = id.ImageDigest
	}
	if id.ImageTag != "" {
		m["imageTag"] = id.ImageTag
	}
	return m
}

func parseImageIDs(raw any) []ecrstore.ImageIdentifier {
	ids, ok := raw.([]any)
	if !ok {
		return nil
	}
	out := make([]ecrstore.ImageIdentifier, 0, len(ids))
	for _, item := range ids {
		out = append(out, parseImageID(item))
	}
	return out
}

func parseImageID(raw any) ecrstore.ImageIdentifier {
	m, ok := raw.(map[string]any)
	if !ok {
		return ecrstore.ImageIdentifier{}
	}
	id := ecrstore.ImageIdentifier{}
	if d, ok := m["imageDigest"].(string); ok {
		id.ImageDigest = d
	}
	if t, ok := m["imageTag"].(string); ok {
		id.ImageTag = t
	}
	return id
}

func parseTags(raw any) map[string]string {
	tags := make(map[string]string)
	items, ok := raw.([]any)
	if !ok {
		return tags
	}
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		k, _ := m["Key"].(string)
		v, _ := m["Value"].(string)
		if k != "" {
			tags[k] = v
		}
	}
	return tags
}

func firstTag(tags []string) string {
	if len(tags) > 0 {
		return tags[0]
	}
	return ""
}
