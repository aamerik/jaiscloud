// Package managedkafka is the transport-neutral core of the Apache Kafka for
// BigQuery (Managed Kafka) v1 service (managedkafka.googleapis.com). It owns all
// cluster, topic, ACL, operation, and consumer-group business logic over
// internal/gcp/store/managedkafka.
//
// It deliberately has no dependency on protobuf or on NormalizedRequest: the
// gRPC transport (internal/gcp/transport/grpc/managedkafka) and the REST
// transport (internal/gcp/transport/rest/managedkafka) both transcode their wire
// format into this package's typed API and then call the SAME Service instance.
// That is the dual-protocol invariant: one core, one piece of state, so the
// transports cannot drift.
//
// A cluster is a logical record only — the emulator never stands up a real
// broker. Cluster create/update/delete return a done google.longrunning.Operation
// inline so official clients that read done+response from the body succeed
// without polling; the operation is also persisted under
// projects/{project}/locations/{location}/operations/{id}. Topic and ACL CRUD
// are fully synchronous. Consumer groups are not tracked: there is no create
// RPC and no broker, so a list is always the empty set and get/update/delete
// report NOT_FOUND, matching real GCP for an absent group.
package managedkafka

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	mkstore "jaiscloud/internal/gcp/store/managedkafka"
	"jaiscloud/internal/model"
)

// Service is the transport-neutral Managed Kafka v1 service.
type Service struct {
	store mkstore.Store
}

// NewService returns a Managed Kafka core backed by the given store.
func NewService(s mkstore.Store) *Service {
	return &Service{store: s}
}

// Reset wipes the store.
func (s *Service) Reset(ctx context.Context) { s.store.Reset(ctx) }

// ClusterInput carries the caller-supplied fields of a cluster create/update.
// Config is the caller's resource body stored verbatim (capacityConfig,
// gcpConfig, ... ); Labels is the extracted labels map.
type ClusterInput struct {
	Labels map[string]string
	Config json.RawMessage
}

// TopicInput carries the caller-supplied fields of a topic create/update.
type TopicInput struct {
	PartitionCount    int
	ReplicationFactor int
	Config            json.RawMessage
}

// randomHex returns n random hexadecimal characters.
func randomHex(n int) string {
	b := make([]byte, (n+1)/2)
	if _, err := rand.Read(b); err != nil {
		b = make([]byte, (n+1)/2)
	}
	return hex.EncodeToString(b)[:n]
}

// pageParams builds the shared cursor-pagination parameter map.
func pageParams(pageSize int, pageToken string) map[string]any {
	params := map[string]any{"pageSize": pageSize}
	if pageToken != "" {
		params["pageToken"] = pageToken
	}
	return params
}

// --- Clusters ---

// CreateCluster creates a cluster and returns it with the done operation that
// carries it as the response.
func (s *Service) CreateCluster(ctx context.Context, project, location, clusterID string, in ClusterInput) (mkstore.Cluster, mkstore.Operation, error) {
	if location == "" || clusterID == "" {
		return mkstore.Cluster{}, mkstore.Operation{}, invalidArgument("missing location or clusterId")
	}
	now := clock.Now().UTC()
	c := mkstore.Cluster{
		Location:   location,
		Name:       clusterID,
		Labels:     in.Labels,
		Config:     in.Config,
		CreateTime: now,
		UpdateTime: now,
	}
	if err := s.store.CreateCluster(ctx, project, location, c); err != nil {
		return mkstore.Cluster{}, mkstore.Operation{}, mapStoreError(err)
	}
	target := ClusterName(project, c.Location, c.Name)
	op, err := s.storeOperation(ctx, project, location, "create", target, ClusterJSON(c, project))
	if err != nil {
		return mkstore.Cluster{}, mkstore.Operation{}, mapStoreError(err)
	}
	return c, op, nil
}

// GetCluster returns one cluster.
func (s *Service) GetCluster(ctx context.Context, project, location, clusterID string) (mkstore.Cluster, error) {
	if location == "" || clusterID == "" {
		return mkstore.Cluster{}, invalidArgument("missing location or clusterId")
	}
	c, err := s.store.GetCluster(ctx, project, location, clusterID)
	if err != nil {
		return mkstore.Cluster{}, mapStoreError(err)
	}
	return c, nil
}

// ListClusters returns a cursor page of the clusters in a location.
func (s *Service) ListClusters(ctx context.Context, project, location string, pageSize int, pageToken string) ([]mkstore.Cluster, string, error) {
	if location == "" {
		return nil, "", invalidArgument("missing location")
	}
	clusters, err := s.store.ListClusters(ctx, project, location)
	if err != nil {
		return nil, "", err
	}
	page, next := paging.Page(clusters, func(c mkstore.Cluster) string { return c.Name }, pageParams(pageSize, pageToken))
	return page, next, nil
}

// UpdateCluster merges the caller's config/labels into the stored cluster and
// returns it with the done update operation.
func (s *Service) UpdateCluster(ctx context.Context, project, location, clusterID string, in ClusterInput) (mkstore.Cluster, mkstore.Operation, error) {
	if location == "" || clusterID == "" {
		return mkstore.Cluster{}, mkstore.Operation{}, invalidArgument("missing location or clusterId")
	}
	c, err := s.store.UpdateClusterAtomic(ctx, project, location, clusterID, func(c mkstore.Cluster) (mkstore.Cluster, error) {
		stored := map[string]any{}
		if len(c.Config) > 0 {
			_ = json.Unmarshal(c.Config, &stored)
		}
		if len(in.Config) > 0 {
			incoming := map[string]any{}
			if json.Unmarshal(in.Config, &incoming) == nil {
				for k, v := range incoming {
					stored[k] = v
				}
			}
		}
		if in.Labels != nil {
			c.Labels = in.Labels
		}
		if data, err := json.Marshal(stored); err == nil {
			c.Config = data
		}
		c.UpdateTime = clock.Now().UTC()
		return c, nil
	})
	if err != nil {
		return mkstore.Cluster{}, mkstore.Operation{}, mapStoreError(err)
	}
	target := ClusterName(project, c.Location, c.Name)
	op, err := s.storeOperation(ctx, project, location, "update", target, ClusterJSON(c, project))
	if err != nil {
		return mkstore.Cluster{}, mkstore.Operation{}, mapStoreError(err)
	}
	return c, op, nil
}

// DeleteCluster deletes a cluster (and its topics/ACLs) and returns the done
// delete operation.
func (s *Service) DeleteCluster(ctx context.Context, project, location, clusterID string) (mkstore.Operation, error) {
	if location == "" || clusterID == "" {
		return mkstore.Operation{}, invalidArgument("missing location or clusterId")
	}
	if err := s.store.DeleteCluster(ctx, project, location, clusterID); err != nil {
		return mkstore.Operation{}, mapStoreError(err)
	}
	target := ClusterName(project, location, clusterID)
	op, err := s.storeOperation(ctx, project, location, "delete", target, map[string]any{})
	if err != nil {
		return mkstore.Operation{}, mapStoreError(err)
	}
	return op, nil
}

// --- Operations ---

// GetOperation returns a persisted cluster-mutation operation.
func (s *Service) GetOperation(ctx context.Context, project, location, opID string) (mkstore.Operation, error) {
	if location == "" || opID == "" {
		return mkstore.Operation{}, invalidArgument("missing location or operationId")
	}
	op, err := s.store.GetOperation(ctx, project, location, opID)
	if err != nil {
		return mkstore.Operation{}, mapStoreError(err)
	}
	return op, nil
}

// ListOperations returns a cursor page of the persisted operations for a
// location.
func (s *Service) ListOperations(ctx context.Context, project, location string, pageSize int, pageToken string) ([]mkstore.Operation, string, error) {
	if location == "" {
		return nil, "", invalidArgument("missing location")
	}
	ops, err := s.store.ListOperations(ctx, project, location)
	if err != nil {
		return nil, "", err
	}
	page, next := paging.Page(ops, func(op mkstore.Operation) string { return op.ID }, pageParams(pageSize, pageToken))
	return page, next, nil
}

// --- Topics ---

// CreateTopic creates a topic under an existing cluster.
func (s *Service) CreateTopic(ctx context.Context, project, location, clusterID, topicID string, in TopicInput) (mkstore.Topic, error) {
	if location == "" || clusterID == "" || topicID == "" {
		return mkstore.Topic{}, invalidArgument("missing location, clusterId, or topicId")
	}
	if _, err := s.store.GetCluster(ctx, project, location, clusterID); err != nil {
		return mkstore.Topic{}, mapStoreError(err)
	}
	now := clock.Now().UTC()
	t := mkstore.Topic{
		Location:          location,
		ClusterName:       clusterID,
		Name:              topicID,
		PartitionCount:    in.PartitionCount,
		ReplicationFactor: in.ReplicationFactor,
		Config:            in.Config,
		CreateTime:        now,
		UpdateTime:        now,
	}
	if err := s.store.CreateTopic(ctx, project, location, clusterID, t); err != nil {
		return mkstore.Topic{}, mapStoreError(err)
	}
	return t, nil
}

// GetTopic returns one topic.
func (s *Service) GetTopic(ctx context.Context, project, location, clusterID, topicID string) (mkstore.Topic, error) {
	if location == "" || clusterID == "" || topicID == "" {
		return mkstore.Topic{}, invalidArgument("missing location, clusterId, or topicId")
	}
	t, err := s.store.GetTopic(ctx, project, location, clusterID, topicID)
	if err != nil {
		return mkstore.Topic{}, mapStoreError(err)
	}
	return t, nil
}

// ListTopics returns a cursor page of the topics in a cluster.
func (s *Service) ListTopics(ctx context.Context, project, location, clusterID string, pageSize int, pageToken string) ([]mkstore.Topic, string, error) {
	if location == "" || clusterID == "" {
		return nil, "", invalidArgument("missing location or clusterId")
	}
	if _, err := s.store.GetCluster(ctx, project, location, clusterID); err != nil {
		return nil, "", mapStoreError(err)
	}
	topics, err := s.store.ListTopics(ctx, project, location, clusterID)
	if err != nil {
		return nil, "", err
	}
	page, next := paging.Page(topics, func(t mkstore.Topic) string { return t.Name }, pageParams(pageSize, pageToken))
	return page, next, nil
}

// UpdateTopic merges the caller's fields into the stored topic.
func (s *Service) UpdateTopic(ctx context.Context, project, location, clusterID, topicID string, in TopicInput) (mkstore.Topic, error) {
	if location == "" || clusterID == "" || topicID == "" {
		return mkstore.Topic{}, invalidArgument("missing location, clusterId, or topicId")
	}
	t, err := s.store.UpdateTopicAtomic(ctx, project, location, clusterID, topicID, func(t mkstore.Topic) (mkstore.Topic, error) {
		if in.PartitionCount != 0 {
			t.PartitionCount = in.PartitionCount
		}
		if in.ReplicationFactor != 0 {
			t.ReplicationFactor = in.ReplicationFactor
		}
		if len(in.Config) > 0 {
			stored := map[string]any{}
			if len(t.Config) > 0 {
				_ = json.Unmarshal(t.Config, &stored)
			}
			incoming := map[string]any{}
			if json.Unmarshal(in.Config, &incoming) == nil {
				for k, v := range incoming {
					stored[k] = v
				}
			}
			if data, err := json.Marshal(stored); err == nil {
				t.Config = data
			}
		}
		t.UpdateTime = clock.Now().UTC()
		return t, nil
	})
	if err != nil {
		return mkstore.Topic{}, mapStoreError(err)
	}
	return t, nil
}

// DeleteTopic deletes a topic.
func (s *Service) DeleteTopic(ctx context.Context, project, location, clusterID, topicID string) error {
	if location == "" || clusterID == "" || topicID == "" {
		return invalidArgument("missing location, clusterId, or topicId")
	}
	if err := s.store.DeleteTopic(ctx, project, location, clusterID, topicID); err != nil {
		return mapStoreError(err)
	}
	return nil
}

// --- Consumer groups ---

// ListConsumerGroups returns an empty page — the emulator tracks no broker
// consumer-group state. The empty set still passes through paging.Page so the
// wire shape matches the real paginated API.
func (s *Service) ListConsumerGroups(ctx context.Context, project, location, clusterID string, pageSize int, pageToken string) ([]string, string, error) {
	if location == "" || clusterID == "" {
		return nil, "", invalidArgument("missing location or clusterId")
	}
	if _, err := s.store.GetCluster(ctx, project, location, clusterID); err != nil {
		return nil, "", mapStoreError(err)
	}
	groups := []string{}
	page, next := paging.Page(groups, func(g string) string { return g }, pageParams(pageSize, pageToken))
	return page, next, nil
}

// GetConsumerGroup reports NOT_FOUND: there is no broker, so no consumer group
// exists. Real GCP returns NOT_FOUND for an absent group.
func (s *Service) GetConsumerGroup(_ context.Context, _, _, _, _ string) error {
	return consumerGroupNotFound()
}

// UpdateConsumerGroup reports NOT_FOUND for the same reason as GetConsumerGroup.
func (s *Service) UpdateConsumerGroup(_ context.Context, _, _, _, _ string) error {
	return consumerGroupNotFound()
}

// DeleteConsumerGroup reports NOT_FOUND for the same reason as GetConsumerGroup.
func (s *Service) DeleteConsumerGroup(_ context.Context, _, _, _, _ string) error {
	return consumerGroupNotFound()
}

func consumerGroupNotFound() error {
	return model.NewProviderError("NotFound", "consumer group not found", 404)
}

// --- errors ---

// invalidArgument builds the canonical InvalidArgument provider error both
// transports map onto their wire status.
func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

// mapStoreError maps a managedkafka store error onto a canonical provider error.
func mapStoreError(err error) error {
	switch {
	case errors.Is(err, mkstore.ErrNoSuchCluster):
		return model.NewProviderError("NotFound", "cluster not found", 404)
	case errors.Is(err, mkstore.ErrNoSuchTopic):
		return model.NewProviderError("NotFound", "topic not found", 404)
	case errors.Is(err, mkstore.ErrNoSuchAcl):
		return model.NewProviderError("NotFound", "acl not found", 404)
	case errors.Is(err, mkstore.ErrNoSuchOperation):
		return model.NewProviderError("NotFound", "operation not found", 404)
	case errors.Is(err, mkstore.ErrAlreadyExists):
		return model.NewProviderError("AlreadyExists", "resource already exists", 409)
	default:
		return err
	}
}
