package object

import (
	"context"
	"strings"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// ─── Buckets ──────────────────────────────────────────────────────────────────

func (p *ObjectProvider) CreateBucket(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	if bucket == "" {
		return nil, model.NewProviderError("InvalidBucketName", "The specified bucket is not valid", 400)
	}
	// Determine requested region (LocationConstraint in body, or nr.Region)
	locationConstraint := strParam(nr.Params, "LocationConstraint")
	if locationConstraint == "" {
		locationConstraint = nr.Region
	}
	meta := map[string]any{
		"AccountID": nr.AccountID,
		"Region":    locationConstraint,
	}
	if err := p.meta.CreateBucket(ctx, bucket, meta); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			// S3 idempotency: load existing bucket metadata
			existing, getErr := p.meta.GetBucket(ctx, bucket)
			if getErr != nil {
				// Can't read existing — treat as success (idempotent)
				return provider.OK(map[string]any{"Location": "/" + bucket}), nil
			}
			existingOwner, _ := existing["AccountID"].(string)
			existingRegion, _ := existing["Region"].(string)
			if existingOwner != nr.AccountID {
				// Different account owns this bucket
				return nil, model.NewProviderError("BucketAlreadyExists", "The requested bucket name is not available", 409)
			}
			// Same account owns the bucket
			if existingRegion == "us-east-1" && locationConstraint == "us-east-1" {
				// Same account, us-east-1: idempotent success
				return provider.OK(map[string]any{"Location": "/" + bucket}), nil
			}
			if existingRegion != locationConstraint {
				// Same account but different region
				return nil, model.NewProviderError("BucketAlreadyOwnedByYou", "Your previous request to create the named bucket succeeded and you already own it", 409)
			}
			// Same account, same region: idempotent success
			return provider.OK(map[string]any{"Location": "/" + bucket}), nil
		}
		return nil, model.NewProviderError("InternalError", err.Error(), 500)
	}
	return provider.OK(map[string]any{"Location": "/" + bucket}), nil
}

func (p *ObjectProvider) DeleteBucket(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	if err := p.meta.DeleteBucket(ctx, bucket); err != nil {
		if strings.Contains(err.Error(), "NoSuchBucket") {
			return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
		}
		if strings.Contains(err.Error(), "BucketNotEmpty") {
			return nil, model.NewProviderError("BucketNotEmpty", "The bucket is not empty", 409)
		}
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

func (p *ObjectProvider) ListBuckets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	buckets, err := p.meta.ListBuckets(ctx, nr.AccountID)
	if err != nil {
		return nil, err
	}
	if buckets == nil {
		buckets = []map[string]any{}
	}
	return provider.OK(map[string]any{
		"Buckets": buckets,
		"Owner":   map[string]any{"ID": nr.AccountID, "DisplayName": nr.AccountID},
	}), nil
}

func (p *ObjectProvider) GetBucketLocation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	if _, err := p.meta.GetBucket(ctx, bucket); err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	return provider.OK(map[string]any{"LocationConstraint": nr.Region}), nil
}

func (p *ObjectProvider) HeadBucket(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	if _, err := p.meta.GetBucket(ctx, bucket); err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"_region": nr.Region}}, nil
}
