//go:build gcp_parity

package gcpparity

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	iam "cloud.google.com/go/iam/apiv1"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	pubsubapiv1 "cloud.google.com/go/pubsub/v2/apiv1"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
)

// iamScenario compares the shared google.iam.v1.IAMPolicy surface over REST and
// gRPC (AUD3-6). The emulator's dual "iam" service is asymmetric: the REST
// provider is service-account admin (projects.serviceAccounts, whose IAM policy
// is only reachable over REST), while the gRPC side is the shared IAMPolicy
// service routed across Pub/Sub, KMS, Eventarc and Metastore. They own no common
// resource of their own, so a like-for-like comparison needs a shared policy
// target — exactly one the exemption in testdata/parity-exemptions.json named.
//
// The scenario uses a run-unique Pub/Sub topic as that target (the same
// precedent as the Workflow Executions prerequisite): the topic is created as a
// fixture, then GetIamPolicy / SetIamPolicy / TestIamPermissions are driven over
// both transports against it. Both paths reach one shared policy store
// (internal/gcp/policy), so this asserts the REST Discovery envelope and the gRPC
// protojson rendering of the same Policy agree — and that a write over either
// transport is visible on the other.
func iamScenario() Scenario {
	topicID := "iam-policy"
	topicName := func(e *Env) string {
		return fmt.Sprintf("projects/%s/topics/%s", e.Cfg.Project, e.Resource(topicID))
	}
	restPath := func(e *Env) string { return "/v1/" + topicName(e) }

	// The two transports must carry the same logical binding so the diff compares
	// like against like; only the encoding differs (REST JSON vs a typed Policy).
	binding := func() (role string, members []string) {
		return "roles/pubsub.viewer", []string{"user:parity@example.com"}
	}
	permissions := []string{"pubsub.topics.get", "pubsub.topics.publish"}

	createTopic := func(ctx context.Context, e *Env) error {
		c, err := pubsubapiv1.NewTopicAdminClient(ctx, e.GRPCClientOptions()...)
		if err != nil {
			return err
		}
		defer c.Close()
		_, err = c.CreateTopic(ctx, &pubsubpb.Topic{Name: topicName(e)})
		return err
	}

	newIAM := func(ctx context.Context, e *Env) (*iam.IamPolicyClient, error) {
		return iam.NewIamPolicyClient(ctx, e.GRPCClientOptions()...)
	}

	// getIamPolicyGRPC reads the target's policy over gRPC.
	getIamPolicyGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		c, err := newIAM(ctx, e)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		return c.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: topicName(e)})
	}
	// getIamPolicyREST reads the same target's policy over REST. getIamPolicy is
	// a POST custom method with an optional request body (here empty options).
	getIamPolicyREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return e.Rest(ctx, http.MethodPost, restPath(e)+":getIamPolicy", "{}")
	}

	setIamPolicyGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		c, err := newIAM(ctx, e)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		role, members := binding()
		return c.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
			Resource: topicName(e),
			Policy:   &iampb.Policy{Bindings: []*iampb.Binding{{Role: role, Members: members}}},
		})
	}
	setIamPolicyREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		role, members := binding()
		body, _ := json.Marshal(map[string]any{
			"policy": map[string]any{
				"bindings": []any{map[string]any{"role": role, "members": members}},
			},
		})
		return e.Rest(ctx, http.MethodPost, restPath(e)+":setIamPolicy", string(body))
	}
	// setIamPolicy{GRPC,REST}Canonical are the Mutate-step forms (the response is
	// discarded); the following read step re-reads the policy over both
	// transports.
	setIamPolicyGRPCCanonical := func(ctx context.Context, e *Env) error {
		_, err := setIamPolicyGRPC(ctx, e)
		return err
	}
	setIamPolicyRESTCanonical := func(ctx context.Context, e *Env) error {
		_, err := setIamPolicyREST(ctx, e)
		return err
	}

	testIamPermissionsGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		c, err := newIAM(ctx, e)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		return c.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{
			Resource:    topicName(e),
			Permissions: permissions,
		})
	}
	testIamPermissionsREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		body, _ := json.Marshal(map[string]any{"permissions": permissions})
		return e.Rest(ctx, http.MethodPost, restPath(e)+":testIamPermissions", string(body))
	}

	deleteTopic := func(ctx context.Context, e *Env) error { return e.RestDelete(ctx, restPath(e)) }

	// A mutation-parity step needs each transport to write its own twin target,
	// so the two writes cannot collide; the Policy response itself carries no
	// resource name, so no projection is needed.
	setIamPolicyGRPCTwin := func(ctx context.Context, e *Env) (protoMessage, error) {
		if err := createTopic(ctx, e); err != nil {
			return nil, err
		}
		return setIamPolicyGRPC(ctx, e)
	}
	setIamPolicyRESTTwin := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		if err := createTopic(ctx, e); err != nil {
			return nil, err
		}
		return setIamPolicyREST(ctx, e)
	}

	return Scenario{Service: "iam", Steps: []Step{
		{Op: "CreateTopic", Mutate: createTopic},
		{Op: "GetIamPolicy (default)", GRPC: getIamPolicyGRPC, REST: getIamPolicyREST},
		// The policy is written over REST and read back over both transports, so
		// the REST write is proven visible to the gRPC service (one shared store).
		{Op: "SetIamPolicy (REST)", Mutate: setIamPolicyRESTCanonical, GRPC: getIamPolicyGRPC, REST: getIamPolicyREST},
		{Op: "SetIamPolicy (gRPC)", Mutate: setIamPolicyGRPCCanonical, GRPC: getIamPolicyGRPC, REST: getIamPolicyREST},
		{Op: "TestIamPermissions", GRPC: testIamPermissionsGRPC, REST: testIamPermissionsREST},
		{
			Op: "SetIamPolicy (parity)",
			Mutation: &MutationParity{
				GRPC: setIamPolicyGRPCTwin, REST: setIamPolicyRESTTwin, Cleanup: deleteTopic,
			},
		},
		{Op: "DeleteTopic", Mutate: deleteTopic},
	}}
}
