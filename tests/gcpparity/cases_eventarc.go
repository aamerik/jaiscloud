//go:build gcp_parity

package gcpparity

import (
	"context"
	"encoding/json"
	"fmt"

	eventarc "cloud.google.com/go/eventarc/apiv1"
	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"google.golang.org/api/iterator"
)

const eventarcLocation = "us-central1"

// eventarcScenario compares the Eventarc trigger surface over REST and gRPC:
// create → get → list → update(labels, over REST) → get → delete.
func eventarcScenario() Scenario {
	id := "parity-trigger"
	parent := func(e *Env) string { return fmt.Sprintf("projects/%s/locations/%s", e.Cfg.Project, eventarcLocation) }
	name := func(e *Env) string { return parent(e) + "/triggers/" + e.Resource(id) }

	trigger := func(e *Env) *eventarcpb.Trigger {
		return &eventarcpb.Trigger{
			Name: name(e),
			Destination: &eventarcpb.Destination{
				Descriptor_: &eventarcpb.Destination_HttpEndpoint{
					HttpEndpoint: &eventarcpb.HttpEndpoint{Uri: "https://example.com/parity"},
				},
			},
			EventFilters: []*eventarcpb.EventFilter{
				{Attribute: "type", Value: "google.cloud.storage.object.v1.finalized"},
				{Attribute: "bucket", Value: "parity-bucket"},
			},
		}
	}
	newClient := func(ctx context.Context, e *Env) (*eventarc.Client, error) {
		return eventarc.NewClient(ctx, e.GRPCClientOptions()...)
	}

	return Scenario{Service: "eventarc", Steps: []Step{
		{
			Op: "CreateTrigger",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := newClient(ctx, e)
				if err != nil {
					return err
				}
				defer c.Close()
				op, err := c.CreateTrigger(ctx, &eventarcpb.CreateTriggerRequest{Parent: parent(e), TriggerId: e.Resource(id), Trigger: trigger(e)})
				if err != nil {
					return err
				}
				_, err = op.Wait(ctx)
				return err
			},
		},
		{
			Op: "GetTrigger",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetTrigger(ctx, &eventarcpb.GetTriggerRequest{Name: name(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+name(e), "")
			},
		},
		{
			Op:    "ListTriggers",
			Scope: true,
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				it := c.ListTriggers(ctx, &eventarcpb.ListTriggersRequest{Parent: parent(e)})
				out := &eventarcpb.ListTriggersResponse{}
				for {
					t, err := it.Next()
					if err == iterator.Done {
						break
					}
					if err != nil {
						return nil, err
					}
					out.Triggers = append(out.Triggers, t)
				}
				return out, nil
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+parent(e)+"/triggers", "")
			},
		},
		{
			Op: "UpdateTrigger (REST)",
			Mutate: func(ctx context.Context, e *Env) error {
				_, err := e.Rest(ctx, "PATCH", "/v1/"+name(e)+"?updateMask=labels", `{"labels":{"parity":"true"}}`)
				return err
			},
		},
		{
			Op: "GetTriggerAfterUpdate",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetTrigger(ctx, &eventarcpb.GetTriggerRequest{Name: name(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+name(e), "")
			},
		},
		{
			Op: "DeleteTrigger",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := newClient(ctx, e)
				if err != nil {
					return err
				}
				defer c.Close()
				op, err := c.DeleteTrigger(ctx, &eventarcpb.DeleteTriggerRequest{Name: name(e)})
				if err != nil {
					return err
				}
				_, err = op.Wait(ctx)
				return err
			},
		},
	}}
}
