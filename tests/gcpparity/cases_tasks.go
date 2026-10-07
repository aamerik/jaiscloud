//go:build gcp_parity

package gcpparity

import (
	"context"
	"encoding/json"
	"fmt"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"google.golang.org/api/iterator"
)

// tasksScenario compares the Cloud Tasks queue + task surface over REST and
// gRPC: create queue → get/list → create task → get/list → delete.
func tasksScenario() Scenario {
	queueID := "parity-queue"
	const location = "us-central1"
	parent := func(e *Env) string { return fmt.Sprintf("projects/%s/locations/%s", e.Cfg.Project, location) }
	queueName := func(e *Env) string { return parent(e) + "/queues/" + e.Resource(queueID) }

	newClient := func(ctx context.Context, e *Env) (*cloudtasks.Client, error) {
		return cloudtasks.NewClient(ctx, e.GRPCClientOptions()...)
	}
	listTasks := func(ctx context.Context, e *Env) (*cloudtaskspb.ListTasksResponse, error) {
		c, err := newClient(ctx, e)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		it := c.ListTasks(ctx, &cloudtaskspb.ListTasksRequest{Parent: queueName(e)})
		out := &cloudtaskspb.ListTasksResponse{}
		for {
			t, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return nil, err
			}
			out.Tasks = append(out.Tasks, t)
		}
		return out, nil
	}

	return Scenario{Service: "tasks", Steps: []Step{
		{
			Op: "CreateQueue",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := newClient(ctx, e)
				if err != nil {
					return err
				}
				defer c.Close()
				_, err = c.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
					Parent: parent(e),
					Queue:  &cloudtaskspb.Queue{Name: queueName(e)},
				})
				return err
			},
		},
		{
			Op: "GetQueue",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetQueue(ctx, &cloudtaskspb.GetQueueRequest{Name: queueName(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v2/"+queueName(e), "")
			},
		},
		{
			Op:    "ListQueues",
			Scope: true,
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				it := c.ListQueues(ctx, &cloudtaskspb.ListQueuesRequest{Parent: parent(e)})
				out := &cloudtaskspb.ListQueuesResponse{}
				for {
					q, err := it.Next()
					if err == iterator.Done {
						break
					}
					if err != nil {
						return nil, err
					}
					out.Queues = append(out.Queues, q)
				}
				return out, nil
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v2/"+parent(e)+"/queues", "")
			},
		},
		{
			Op: "CreateTask",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := newClient(ctx, e)
				if err != nil {
					return err
				}
				defer c.Close()
				_, err = c.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
					Parent: queueName(e),
					Task: &cloudtaskspb.Task{
						MessageType: &cloudtaskspb.Task_HttpRequest{HttpRequest: &cloudtaskspb.HttpRequest{
							HttpMethod: cloudtaskspb.HttpMethod_GET,
							Url:        "http://example.com/parity",
						}},
					},
				})
				return err
			},
		},
		{
			Op: "ListTasks",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				return listTasks(ctx, e)
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v2/"+queueName(e)+"/tasks", "")
			},
		},
		{
			Op: "UpdateQueue (REST)",
			Mutate: func(ctx context.Context, e *Env) error {
				_, err := e.Rest(ctx, "PATCH", "/v2/"+queueName(e)+"?updateMask=rateLimits",
					`{"rateLimits":{"maxDispatchesPerSecond":10}}`)
				return err
			},
		},
		{
			Op: "GetQueueAfterUpdate",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetQueue(ctx, &cloudtaskspb.GetQueueRequest{Name: queueName(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v2/"+queueName(e), "")
			},
		},
		{
			Op: "DeleteQueue",
			Mutate: func(ctx context.Context, e *Env) error {
				return e.RestDelete(ctx, "/v2/"+queueName(e))
			},
		},
	}}
}
