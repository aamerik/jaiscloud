//go:build gcp_parity

package gcpparity

import (
	"context"
	"encoding/json"

	monitoring "cloud.google.com/go/monitoring/apiv3"
	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/api/iterator"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
)

// monitoringScenario compares the Cloud Monitoring metric-descriptor surface
// over REST and gRPC: create → get → list. Metric descriptors have no delete
// RPC, so the flow stops at list.
func monitoringScenario() Scenario {
	metricType := func(e *Env) string { return "custom.googleapis.com/parity/" + e.Cfg.Suffix }
	name := func(e *Env) string { return e.Cfg.ProjectPath() + "/metricDescriptors/" + metricType(e) }

	newClient := func(ctx context.Context, e *Env) (*monitoring.MetricClient, error) {
		return monitoring.NewMetricClient(ctx, e.GRPCClientOptions()...)
	}

	return Scenario{Service: "monitoring", Steps: []Step{
		{
			Op: "CreateMetricDescriptor",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := newClient(ctx, e)
				if err != nil {
					return err
				}
				defer c.Close()
				_, err = c.CreateMetricDescriptor(ctx, &monitoringpb.CreateMetricDescriptorRequest{
					Name: e.Cfg.ProjectPath(),
					MetricDescriptor: &metricpb.MetricDescriptor{
						Type:        metricType(e),
						MetricKind:  metricpb.MetricDescriptor_GAUGE,
						ValueType:   metricpb.MetricDescriptor_DOUBLE,
						Unit:        "1",
						Description: "parity descriptor",
					},
				})
				return err
			},
		},
		{
			Op: "GetMetricDescriptor",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetMetricDescriptor(ctx, &monitoringpb.GetMetricDescriptorRequest{Name: name(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v3/"+name(e), "")
			},
		},
		{
			Op:    "ListMetricDescriptors",
			Scope: true,
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				it := c.ListMetricDescriptors(ctx, &monitoringpb.ListMetricDescriptorsRequest{Name: e.Cfg.ProjectPath()})
				out := &monitoringpb.ListMetricDescriptorsResponse{}
				for {
					d, err := it.Next()
					if err == iterator.Done {
						break
					}
					if err != nil {
						return nil, err
					}
					out.MetricDescriptors = append(out.MetricDescriptors, d)
				}
				return out, nil
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v3/"+e.Cfg.ProjectPath()+"/metricDescriptors", "")
			},
		},
	}}
}
