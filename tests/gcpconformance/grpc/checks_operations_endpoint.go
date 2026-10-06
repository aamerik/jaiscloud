package grpcconformance

import (
	"context"
	"fmt"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	managedkafkapb "cloud.google.com/go/managedkafka/apiv1/managedkafkapb"
	metastorepb "cloud.google.com/go/metastore/apiv1/metastorepb"
)

// managedKafkaParent is the project/location parent the Managed Kafka probes use.
func managedKafkaParent(cfg Config) string {
	return fmt.Sprintf("projects/%s/locations/%s", cfg.Project, managedKafkaLocation)
}

// checkMetastoreOperationsEndpoint verifies the shared
// google.longrunning.Operations service is endpoint-scoped, as real GCP serves
// it per service host: a client addressed at the Metastore endpoint resolves and
// lists the operation a Metastore create persisted, while the Managed Kafka
// endpoint (which shares the location parent) does not list it.
func checkMetastoreOperationsEndpoint(ctx context.Context, cfg Config) error {
	client, err := newMetastoreClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("metastore client: %w", err)
	}
	defer client.Close()

	op, err := client.CreateService(ctx, &metastorepb.CreateServiceRequest{
		Parent:    metastoreParent(cfg),
		ServiceId: cfg.ResourceName("gcpc-grpc-ms-ops"),
		Service:   &metastorepb.Service{Labels: map[string]string{"probe": "gcpc-ops"}},
	})
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}
	opName := op.Name()

	own, err := newEndpointOperationsClient(ctx, cfg, "metastore.localhost")
	if err != nil {
		return fmt.Errorf("metastore operations client: %w", err)
	}
	defer own.Close()
	got, err := own.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: opName})
	if err != nil {
		return fmt.Errorf("GetOperation at the metastore endpoint: %w", err)
	}
	if got.GetName() != opName {
		return fmt.Errorf("GetOperation name = %q, want %q", got.GetName(), opName)
	}
	if err := operationsListContains(ctx, own, metastoreParent(cfg), opName); err != nil {
		return err
	}

	sibling, err := newEndpointOperationsClient(ctx, cfg, "managedkafka.localhost")
	if err != nil {
		return fmt.Errorf("sibling operations client: %w", err)
	}
	defer sibling.Close()
	return operationsListExcludes(ctx, sibling, metastoreParent(cfg), opName)
}

// checkManagedKafkaOperationsEndpoint mirrors checkMetastoreOperationsEndpoint
// for Managed Kafka: its endpoint resolves and lists the operation a cluster
// create persisted, and the Metastore endpoint does not list it.
func checkManagedKafkaOperationsEndpoint(ctx context.Context, cfg Config) error {
	client, err := newManagedKafkaClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("managedkafka client: %w", err)
	}
	defer client.Close()

	op, err := client.CreateCluster(ctx, &managedkafkapb.CreateClusterRequest{
		Parent:    managedKafkaParent(cfg),
		ClusterId: cfg.ResourceName("gcpc-grpc-mk-ops"),
		Cluster: &managedkafkapb.Cluster{
			Labels:         map[string]string{"probe": "gcpc-ops"},
			CapacityConfig: &managedkafkapb.CapacityConfig{VcpuCount: 3, MemoryBytes: 3221225472},
		},
	})
	if err != nil {
		return fmt.Errorf("create cluster: %w", err)
	}
	opName := op.Name()

	own, err := newEndpointOperationsClient(ctx, cfg, "managedkafka.localhost")
	if err != nil {
		return fmt.Errorf("managedkafka operations client: %w", err)
	}
	defer own.Close()
	got, err := own.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: opName})
	if err != nil {
		return fmt.Errorf("GetOperation at the managedkafka endpoint: %w", err)
	}
	if got.GetName() != opName {
		return fmt.Errorf("GetOperation name = %q, want %q", got.GetName(), opName)
	}
	if err := operationsListContains(ctx, own, managedKafkaParent(cfg), opName); err != nil {
		return err
	}

	sibling, err := newEndpointOperationsClient(ctx, cfg, "metastore.localhost")
	if err != nil {
		return fmt.Errorf("sibling operations client: %w", err)
	}
	defer sibling.Close()
	return operationsListExcludes(ctx, sibling, managedKafkaParent(cfg), opName)
}
