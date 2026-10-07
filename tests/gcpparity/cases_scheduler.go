//go:build gcp_parity

package gcpparity

import (
	"context"
	"encoding/json"
	"fmt"

	scheduler "cloud.google.com/go/scheduler/apiv1"
	"cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"google.golang.org/api/iterator"
)

// schedulerScenario compares the Cloud Scheduler job surface over REST and gRPC:
// create → get → list → update(description, over REST) → get → delete.
func schedulerScenario() Scenario {
	id := "parity-job"
	const location = "us-central1"
	parent := func(e *Env) string { return fmt.Sprintf("projects/%s/locations/%s", e.Cfg.Project, location) }
	jobName := func(e *Env) string { return parent(e) + "/jobs/" + e.Resource(id) }

	newClient := func(ctx context.Context, e *Env) (*scheduler.CloudSchedulerClient, error) {
		return scheduler.NewCloudSchedulerClient(ctx, e.GRPCClientOptions()...)
	}

	return Scenario{Service: "scheduler", Steps: []Step{
		{
			Op: "CreateJob",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := newClient(ctx, e)
				if err != nil {
					return err
				}
				defer c.Close()
				_, err = c.CreateJob(ctx, &schedulerpb.CreateJobRequest{
					Parent: parent(e),
					Job: &schedulerpb.Job{
						Name:     jobName(e),
						Schedule: "* * * * *",
						TimeZone: "UTC",
						Target: &schedulerpb.Job_HttpTarget{HttpTarget: &schedulerpb.HttpTarget{
							Uri:        "http://example.com/parity",
							HttpMethod: schedulerpb.HttpMethod_GET,
						}},
					},
				})
				return err
			},
		},
		{
			Op: "GetJob",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetJob(ctx, &schedulerpb.GetJobRequest{Name: jobName(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+jobName(e), "")
			},
		},
		{
			Op:    "ListJobs",
			Scope: true,
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				it := c.ListJobs(ctx, &schedulerpb.ListJobsRequest{Parent: parent(e)})
				out := &schedulerpb.ListJobsResponse{}
				for {
					j, err := it.Next()
					if err == iterator.Done {
						break
					}
					if err != nil {
						return nil, err
					}
					out.Jobs = append(out.Jobs, j)
				}
				return out, nil
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+parent(e)+"/jobs", "")
			},
		},
		{
			Op: "UpdateJob (REST)",
			Mutate: func(ctx context.Context, e *Env) error {
				_, err := e.Rest(ctx, "PATCH", "/v1/"+jobName(e)+"?updateMask=description", `{"description":"parity updated"}`)
				return err
			},
		},
		{
			Op: "GetJobAfterUpdate",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetJob(ctx, &schedulerpb.GetJobRequest{Name: jobName(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+jobName(e), "")
			},
		},
		{
			Op: "DeleteJob",
			Mutate: func(ctx context.Context, e *Env) error {
				return e.RestDelete(ctx, "/v1/"+jobName(e))
			},
		},
	}}
}
