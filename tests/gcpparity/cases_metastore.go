//go:build gcp_parity

package gcpparity

import (
	"context"
	"encoding/json"
	"fmt"

	metastore "cloud.google.com/go/metastore/apiv1"
	"cloud.google.com/go/metastore/apiv1/metastorepb"
	"google.golang.org/api/iterator"
)

const metastoreLocation = "us-central1"

// metastoreScenario compares the Dataproc Metastore service surface over REST
// and gRPC: create → get → list → update(labels, over REST) → get → delete.
func metastoreScenario() Scenario {
	id := "parity-ms"
	parent := func(e *Env) string { return fmt.Sprintf("projects/%s/locations/%s", e.Cfg.Project, metastoreLocation) }
	name := func(e *Env) string { return parent(e) + "/services/" + e.Resource(id) }

	newClient := func(ctx context.Context, e *Env) (*metastore.DataprocMetastoreClient, error) {
		return metastore.NewDataprocMetastoreClient(ctx, e.GRPCClientOptions()...)
	}

	return Scenario{Service: "metastore", Steps: []Step{
		{
			Op: "CreateService",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := newClient(ctx, e)
				if err != nil {
					return err
				}
				defer c.Close()
				op, err := c.CreateService(ctx, &metastorepb.CreateServiceRequest{
					Parent:    parent(e),
					ServiceId: e.Resource(id),
					Service:   &metastorepb.Service{Labels: map[string]string{"parity": "true"}},
				})
				if err != nil {
					return err
				}
				_, err = op.Wait(ctx)
				return err
			},
		},
		{
			Op: "GetService",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetService(ctx, &metastorepb.GetServiceRequest{Name: name(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+name(e), "")
			},
		},
		{
			Op:    "ListServices",
			Scope: true,
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				it := c.ListServices(ctx, &metastorepb.ListServicesRequest{Parent: parent(e)})
				out := &metastorepb.ListServicesResponse{}
				for {
					s, err := it.Next()
					if err == iterator.Done {
						break
					}
					if err != nil {
						return nil, err
					}
					out.Services = append(out.Services, s)
				}
				return out, nil
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+parent(e)+"/services", "")
			},
		},
		{
			Op: "UpdateService (REST)",
			Mutate: func(ctx context.Context, e *Env) error {
				_, err := e.Rest(ctx, "PATCH", "/v1/"+name(e)+"?updateMask=labels", `{"labels":{"parity":"updated"}}`)
				return err
			},
		},
		{
			Op: "GetServiceAfterUpdate",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetService(ctx, &metastorepb.GetServiceRequest{Name: name(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+name(e), "")
			},
		},
		{
			Op: "DeleteService",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := newClient(ctx, e)
				if err != nil {
					return err
				}
				defer c.Close()
				op, err := c.DeleteService(ctx, &metastorepb.DeleteServiceRequest{Name: name(e)})
				if err != nil {
					return err
				}
				return op.Wait(ctx)
			},
		},
	}}
}
