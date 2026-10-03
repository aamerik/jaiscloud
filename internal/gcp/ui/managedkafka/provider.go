package managedkafkaui

import (
	"context"

	managedkafkacore "jaiscloud/internal/gcp/service/managedkafka"
	mkstore "jaiscloud/internal/gcp/store/managedkafka"
)

// ProviderInterface is the subset of *managedkafkacore.Service used by the
// Managed Kafka UI handlers. The core is transport-neutral and addressable
// directly, so the UI reuses its typed API rather than going through a REST
// adapter. It keeps the UI decoupled from the core's concrete type and hides
// the long-running operations and cursor pages the mutations/lists return.
type ProviderInterface interface {
	// ListAllClusters lists every cluster in a project across all locations,
	// sorted by location then name. It backs the location-optional console
	// list.
	ListAllClusters(ctx context.Context, project string) ([]mkstore.Cluster, error)

	// GetCluster returns one cluster.
	GetCluster(ctx context.Context, project, location, cluster string) (mkstore.Cluster, error)

	// ListAllTopics lists every topic in a project across all locations and
	// clusters, sorted by location, cluster, then name.
	ListAllTopics(ctx context.Context, project string) ([]mkstore.Topic, error)

	// ListTopics lists the topics of one cluster.
	ListTopics(ctx context.Context, project, location, cluster string) ([]mkstore.Topic, error)

	// GetTopic returns one topic of a cluster.
	GetTopic(ctx context.Context, project, location, cluster, topic string) (mkstore.Topic, error)

	// ListAcls lists a cluster's ACLs (read-only console surface).
	ListAcls(ctx context.Context, project, location, cluster string) ([]mkstore.Acl, error)

	// ListConsumerGroups lists a cluster's consumer groups from its live
	// broker's group coordinator, including committed offsets. With no live
	// broker (the default mock topology) the list is empty.
	ListConsumerGroups(ctx context.Context, project, location, cluster string) ([]managedkafkacore.ConsumerGroup, error)
}

// Provider adapts the transport-neutral Managed Kafka core onto
// ProviderInterface, discarding cursor pages and long-running operations.
type Provider struct {
	svc *managedkafkacore.Service
}

// NewProvider returns a UI provider over the Managed Kafka core.
func NewProvider(svc *managedkafkacore.Service) *Provider { return &Provider{svc: svc} }

var _ ProviderInterface = (*Provider)(nil)

// ListAllClusters implements ProviderInterface.
func (p *Provider) ListAllClusters(ctx context.Context, project string) ([]mkstore.Cluster, error) {
	return p.svc.ListAllClusters(ctx, project)
}

// GetCluster implements ProviderInterface.
func (p *Provider) GetCluster(ctx context.Context, project, location, cluster string) (mkstore.Cluster, error) {
	return p.svc.GetCluster(ctx, project, location, cluster)
}

// ListAllTopics implements ProviderInterface.
func (p *Provider) ListAllTopics(ctx context.Context, project string) ([]mkstore.Topic, error) {
	return p.svc.ListAllTopics(ctx, project)
}

// ListTopics implements ProviderInterface; every cursor page is followed so
// the console's client-side pagination sees the full cluster (the core pages at
// pageSize<=1000 otherwise).
func (p *Provider) ListTopics(ctx context.Context, project, location, cluster string) ([]mkstore.Topic, error) {
	var out []mkstore.Topic
	token := ""
	for {
		page, next, err := p.svc.ListTopics(ctx, project, location, cluster, uiPageSize, token)
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if next == "" {
			return out, nil
		}
		token = next
	}
}

// GetTopic implements ProviderInterface.
func (p *Provider) GetTopic(ctx context.Context, project, location, cluster, topic string) (mkstore.Topic, error) {
	return p.svc.GetTopic(ctx, project, location, cluster, topic)
}

// ListAcls implements ProviderInterface; every cursor page is followed so the
// console sees every ACL.
func (p *Provider) ListAcls(ctx context.Context, project, location, cluster string) ([]mkstore.Acl, error) {
	var out []mkstore.Acl
	token := ""
	for {
		page, next, err := p.svc.ListAcls(ctx, project, location, cluster, uiPageSize, token)
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if next == "" {
			return out, nil
		}
		token = next
	}
}

// ListConsumerGroups implements ProviderInterface; the FULL view is requested
// so committed offsets are included, and every cursor page is followed.
func (p *Provider) ListConsumerGroups(ctx context.Context, project, location, cluster string) ([]managedkafkacore.ConsumerGroup, error) {
	var out []managedkafkacore.ConsumerGroup
	token := ""
	for {
		page, next, err := p.svc.ListConsumerGroups(ctx, project, location, cluster, managedkafkacore.ConsumerGroupViewFull, "", uiPageSize, token)
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if next == "" {
			return out, nil
		}
		token = next
	}
}

// uiPageSize is the cursor page size the UI walks. The console paginates
// client-side, so the provider follows every page rather than surfacing a
// next-page token; it mirrors the core's default page cap.
const uiPageSize = 1000
