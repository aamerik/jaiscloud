//go:build gcp_parity

package gcpparity

import (
	"context"
	"encoding/json"
	"fmt"

	pubsubapiv1 "cloud.google.com/go/pubsub/v2/apiv1"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// pubSubScenario compares the Pub/Sub topic + subscription admin surface over
// REST and gRPC: create → get → list → update(labels, over REST) → delete.
func pubSubScenario() Scenario {
	topicID := "parity-topic"
	subID := "parity-sub"
	topicName := func(e *Env) string { return fmt.Sprintf("projects/%s/topics/%s", e.Cfg.Project, e.Resource(topicID)) }
	subName := func(e *Env) string {
		return fmt.Sprintf("projects/%s/subscriptions/%s", e.Cfg.Project, e.Resource(subID))
	}
	project := func(e *Env) string { return e.Cfg.ProjectPath() }

	return Scenario{Service: "pubsub", Steps: []Step{
		{
			Op: "CreateTopic",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := pubsubapiv1.NewTopicAdminClient(ctx, e.GRPCClientOptions()...)
				if err != nil {
					return err
				}
				defer c.Close()
				_, err = c.CreateTopic(ctx, &pubsubpb.Topic{Name: topicName(e)})
				return err
			},
		},
		{
			Op: "GetTopic",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := pubsubapiv1.NewTopicAdminClient(ctx, e.GRPCClientOptions()...)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: topicName(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+topicName(e), "")
			},
		},
		{
			Op:    "ListTopics",
			Scope: true,
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := pubsubapiv1.NewTopicAdminClient(ctx, e.GRPCClientOptions()...)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				it := c.ListTopics(ctx, &pubsubpb.ListTopicsRequest{Project: project(e)})
				out := &pubsubpb.ListTopicsResponse{}
				for {
					tp, err := it.Next()
					if err == iterator.Done {
						break
					}
					if err != nil {
						return nil, err
					}
					out.Topics = append(out.Topics, tp)
				}
				return out, nil
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+project(e)+"/topics", "")
			},
		},
		{
			Op: "UpdateTopic (gRPC)",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := pubsubapiv1.NewTopicAdminClient(ctx, e.GRPCClientOptions()...)
				if err != nil {
					return err
				}
				defer c.Close()
				_, err = c.UpdateTopic(ctx, &pubsubpb.UpdateTopicRequest{
					Topic:      &pubsubpb.Topic{Name: topicName(e), Labels: map[string]string{"parity": "true"}},
					UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
				})
				return err
			},
		},
		{
			Op: "GetTopicAfterUpdate",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := pubsubapiv1.NewTopicAdminClient(ctx, e.GRPCClientOptions()...)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: topicName(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+topicName(e), "")
			},
		},
		{
			Op: "CreateSubscription",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := pubsubapiv1.NewSubscriptionAdminClient(ctx, e.GRPCClientOptions()...)
				if err != nil {
					return err
				}
				defer c.Close()
				_, err = c.CreateSubscription(ctx, &pubsubpb.Subscription{
					Name:  subName(e),
					Topic: topicName(e),
				})
				return err
			},
		},
		{
			Op: "GetSubscription",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := pubsubapiv1.NewSubscriptionAdminClient(ctx, e.GRPCClientOptions()...)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: subName(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+subName(e), "")
			},
		},
		{
			Op:    "ListSubscriptions",
			Scope: true,
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := pubsubapiv1.NewSubscriptionAdminClient(ctx, e.GRPCClientOptions()...)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				it := c.ListSubscriptions(ctx, &pubsubpb.ListSubscriptionsRequest{Project: project(e)})
				out := &pubsubpb.ListSubscriptionsResponse{}
				for {
					s, err := it.Next()
					if err == iterator.Done {
						break
					}
					if err != nil {
						return nil, err
					}
					out.Subscriptions = append(out.Subscriptions, s)
				}
				return out, nil
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+project(e)+"/subscriptions", "")
			},
		},
		{
			Op: "UpdateSubscription (gRPC)",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := pubsubapiv1.NewSubscriptionAdminClient(ctx, e.GRPCClientOptions()...)
				if err != nil {
					return err
				}
				defer c.Close()
				_, err = c.UpdateSubscription(ctx, &pubsubpb.UpdateSubscriptionRequest{
					Subscription: &pubsubpb.Subscription{Name: subName(e), Labels: map[string]string{"parity": "true"}},
					UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
				})
				return err
			},
		},
		{
			Op: "GetSubscriptionAfterUpdate",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := pubsubapiv1.NewSubscriptionAdminClient(ctx, e.GRPCClientOptions()...)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: subName(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+subName(e), "")
			},
		},
		{
			Op: "DeleteSubscription",
			Mutate: func(ctx context.Context, e *Env) error {
				return e.RestDelete(ctx, "/v1/"+subName(e))
			},
		},
		{
			Op: "DeleteTopic",
			Mutate: func(ctx context.Context, e *Env) error {
				return e.RestDelete(ctx, "/v1/"+topicName(e))
			},
		},
	}}
}
