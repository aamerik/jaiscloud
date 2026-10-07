//go:build gcp_parity

package gcpparity

import (
	"context"
	"encoding/json"

	logging "cloud.google.com/go/logging/apiv2"
	"cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/api/iterator"
)

// loggingScenario compares the Cloud Logging Admin v2 log-bucket surface over
// REST and gRPC: create → get → list → update(description, over REST) → get →
// delete.
func loggingScenario() Scenario {
	id := "parity-bucket"
	locationParent := func(e *Env) string { return e.Cfg.ProjectPath() + "/locations/global" }
	name := func(e *Env) string { return locationParent(e) + "/buckets/" + e.Resource(id) }

	newClient := func(ctx context.Context, e *Env) (*logging.ConfigClient, error) {
		return logging.NewConfigClient(ctx, e.GRPCClientOptions()...)
	}

	return Scenario{Service: "logging", Steps: []Step{
		{
			Op: "CreateBucket",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := newClient(ctx, e)
				if err != nil {
					return err
				}
				defer c.Close()
				_, err = c.CreateBucket(ctx, &loggingpb.CreateBucketRequest{
					Parent:   locationParent(e),
					BucketId: e.Resource(id),
					Bucket:   &loggingpb.LogBucket{Description: "parity", RetentionDays: 30},
				})
				return err
			},
		},
		{
			Op: "GetBucket",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetBucket(ctx, &loggingpb.GetBucketRequest{Name: name(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v2/"+name(e), "")
			},
		},
		{
			Op:    "ListBuckets",
			Scope: true,
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				it := c.ListBuckets(ctx, &loggingpb.ListBucketsRequest{Parent: locationParent(e)})
				out := &loggingpb.ListBucketsResponse{}
				for {
					b, err := it.Next()
					if err == iterator.Done {
						break
					}
					if err != nil {
						return nil, err
					}
					out.Buckets = append(out.Buckets, b)
				}
				return out, nil
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v2/"+locationParent(e)+"/buckets", "")
			},
		},
		{
			Op: "UpdateBucket (REST)",
			Mutate: func(ctx context.Context, e *Env) error {
				_, err := e.Rest(ctx, "PATCH", "/v2/"+name(e)+"?updateMask=description", `{"description":"parity updated"}`)
				return err
			},
		},
		{
			Op: "GetBucketAfterUpdate",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetBucket(ctx, &loggingpb.GetBucketRequest{Name: name(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v2/"+name(e), "")
			},
		},
		{
			Op: "DeleteBucket",
			Mutate: func(ctx context.Context, e *Env) error {
				return e.RestDelete(ctx, "/v2/"+name(e))
			},
		},
	}}
}
