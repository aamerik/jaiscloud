// Package managedkafka implements the Apache Kafka for BigQuery (Managed Kafka)
// v1 provider (managedkafka.googleapis.com/v1): Cluster and Topic resources.
//
// A cluster is a logical record only — the emulator never stands up a real
// broker. Cluster create/update/delete return a proper
// google.longrunning.Operation (name, metadata with @type, done, response)
// returned inline with done:true and response set to the cluster (or {} for
// delete), so SDKs that read done+response from the body succeed without
// polling. The operation is also persisted under
// projects/{project}/locations/{location}/operations/{id} and is served by the
// registered GetOperation/ListOperations handlers for direct dispatch.
//
// Managed Kafka's operations share the locations/{location}/operations/{id}
// path with Cloud Workflows' LRO surface, which is host-ambiguous on a single
// emulator host; the adapter deliberately routes that path to Workflows (the
// Dataproc-Metastore convention). Because every operation is returned inline
// already done, no Managed Kafka client needs to poll operations.get, so the
// handlers below are reachable only via direct dispatch and the shared
// /operations/ routing is intentionally left unchanged.
//
// Topic CRUD is fully synchronous and returns the resource directly. Consumer
// groups are not tracked — their list endpoint always returns an empty list.
package managedkafka

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	mkstore "jaiscloud/internal/gcp/store/managedkafka"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// operationMetadataType is the @type of the Managed Kafka v1 OperationMetadata.
const operationMetadataType = "type.googleapis.com/google.cloud.managedkafka.v1.OperationMetadata"

// Provider handles Managed Kafka v1 cluster and topic resources.
type Provider struct {
	store mkstore.Store
}

// New returns a Provider backed by the given store.
func New(s mkstore.Store) *Provider {
	return &Provider{store: s}
}

// Reset wipes the store.
func (p *Provider) Reset(ctx context.Context) { p.store.Reset(ctx) }

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"ManagedKafka.CreateCluster":      p.CreateCluster,
		"ManagedKafka.GetCluster":         p.GetCluster,
		"ManagedKafka.ListClusters":       p.ListClusters,
		"ManagedKafka.UpdateCluster":      p.UpdateCluster,
		"ManagedKafka.DeleteCluster":      p.DeleteCluster,
		"ManagedKafka.GetOperation":       p.GetOperation,
		"ManagedKafka.ListOperations":     p.ListOperations,
		"ManagedKafka.CreateTopic":        p.CreateTopic,
		"ManagedKafka.GetTopic":           p.GetTopic,
		"ManagedKafka.ListTopics":         p.ListTopics,
		"ManagedKafka.UpdateTopic":        p.UpdateTopic,
		"ManagedKafka.DeleteTopic":        p.DeleteTopic,
		"ManagedKafka.ListConsumerGroups": p.ListConsumerGroups,
		// Consumer-group resource operations (get/update/delete) are not
		// tracked by the emulator; they route to an Unimplemented response.
		"ManagedKafka.GetConsumerGroup":    p.consumerGroupUnimplemented,
		"ManagedKafka.UpdateConsumerGroup": p.consumerGroupUnimplemented,
		"ManagedKafka.DeleteConsumerGroup": p.consumerGroupUnimplemented,
	}
}

func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

func bodyMap(body map[string]any, key string) map[string]any {
	if body == nil {
		return nil
	}
	m, _ := body[key].(map[string]any)
	return m
}

func bodyStringMap(body map[string]any, key string) map[string]string {
	m := bodyMap(body, key)
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

func bodyInt(body map[string]any, key string) int {
	if body == nil {
		return 0
	}
	switch v := body[key].(type) {
	case float64:
		return int(v)
	case int:
		return v
	case json.Number:
		if n, err := v.Int64(); err == nil {
			return int(n)
		}
	}
	return 0
}

func mapErr(err error) error {
	switch {
	case errors.Is(err, mkstore.ErrNoSuchCluster):
		return model.NewProviderError("NotFound", "cluster not found", 404)
	case errors.Is(err, mkstore.ErrNoSuchTopic):
		return model.NewProviderError("NotFound", "topic not found", 404)
	case errors.Is(err, mkstore.ErrNoSuchOperation):
		return model.NewProviderError("NotFound", "operation not found", 404)
	case errors.Is(err, mkstore.ErrAlreadyExists):
		return model.NewProviderError("AlreadyExists", "resource already exists", 409)
	}
	return err
}

// randomHex returns n random hexadecimal characters.
func randomHex(n int) string {
	b := make([]byte, (n+1)/2)
	if _, err := rand.Read(b); err != nil {
		b = make([]byte, (n+1)/2)
	}
	return hex.EncodeToString(b)[:n]
}

func formatTimestamp(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// --- Wire rendering ---

// clusterMap renders a store Cluster as a managedkafka.v1.Cluster wire map.
// capacityConfig, gcpConfig, rebalanceConfig, and tlsConfig are echoed from the
// stored config verbatim.
func (p *Provider) clusterMap(nr *model.NormalizedRequest, c mkstore.Cluster) map[string]any {
	name := nr.ResourceID("managedkafka-cluster", c.Location+"/"+c.Name)
	out := map[string]any{
		"name":       name,
		"state":      "ACTIVE",
		"createTime": formatTimestamp(c.CreateTime),
		"updateTime": formatTimestamp(c.UpdateTime),
	}
	var cfg map[string]any
	if len(c.Config) > 0 {
		_ = json.Unmarshal(c.Config, &cfg)
	}
	for _, k := range []string{"capacityConfig", "gcpConfig", "rebalanceConfig", "tlsConfig"} {
		if v, ok := cfg[k].(map[string]any); ok {
			out[k] = v
		}
	}
	labels := c.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	out["labels"] = labels
	return out
}

// topicMap renders a store Topic as a managedkafka.v1.Topic wire map.
func (p *Provider) topicMap(nr *model.NormalizedRequest, t mkstore.Topic) map[string]any {
	name := nr.ResourceID("managedkafka-topic", t.Location+"/"+t.ClusterName+"/"+t.Name)
	return map[string]any{
		"name":              name,
		"partitionCount":    t.PartitionCount,
		"replicationFactor": t.ReplicationFactor,
	}
}

// --- Clusters ---

// operationMetadata renders the managedkafka.v1.OperationMetadata for an
// operation. The emulator completes every cluster mutation synchronously, so
// createTime and endTime are the same instant.
func operationMetadata(target, verb string, start time.Time) map[string]any {
	return map[string]any{
		"@type":      operationMetadataType,
		"createTime": formatTimestamp(start),
		"endTime":    formatTimestamp(start),
		"target":     target,
		"verb":       verb,
		"apiVersion": "v1",
	}
}

// operationMap renders a stored Operation as a google.longrunning.Operation.
func (p *Provider) operationMap(nr *model.NormalizedRequest, op mkstore.Operation) map[string]any {
	name := nr.ResourceID("managedkafka-operation", op.Location+"/"+op.ID)
	var metadata any = map[string]any{}
	if op.Metadata != "" {
		_ = json.Unmarshal([]byte(op.Metadata), &metadata)
	}
	out := map[string]any{
		"name":     name,
		"metadata": metadata,
		"done":     op.Done,
	}
	if op.Done && op.Response != "" {
		var response any = map[string]any{}
		if json.Unmarshal([]byte(op.Response), &response) == nil {
			out["response"] = response
		}
	}
	return out
}

// storeOperation persists a done google.longrunning.Operation and returns its
// wire map. Marshalling metadata/response cannot fail for the plain maps the
// provider builds, so a marshal error is ignored and surfaces as an empty
// JSON object on read-back.
func (p *Provider) storeOperation(ctx context.Context, nr *model.NormalizedRequest, location, verb, target string, response map[string]any) (map[string]any, error) {
	now := clock.Now().UTC()
	metaJSON, _ := json.Marshal(operationMetadata(target, verb, now))
	respJSON, _ := json.Marshal(response)
	op := mkstore.Operation{
		ID:         randomHex(12),
		ProjectID:  nr.AccountID,
		Location:   location,
		Done:       true,
		Metadata:   string(metaJSON),
		Response:   string(respJSON),
		Verb:       verb,
		Target:     target,
		CreateTime: now,
		EndTime:    now,
	}
	if err := p.store.CreateOperation(ctx, nr.AccountID, location, op); err != nil {
		return nil, err
	}
	return p.operationMap(nr, op), nil
}

func (p *Provider) CreateCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	clusterID := strParam(nr, "clusterId")
	if location == "" || clusterID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or clusterId", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	now := clock.Now().UTC()
	c := mkstore.Cluster{
		Location:   location,
		Name:       clusterID,
		Labels:     bodyStringMap(body, "labels"),
		CreateTime: now,
		UpdateTime: now,
	}
	if body != nil {
		if data, err := json.Marshal(body); err == nil {
			c.Config = data
		}
	}
	if err := p.store.CreateCluster(ctx, nr.AccountID, location, c); err != nil {
		return nil, mapErr(err)
	}
	target := nr.ResourceID("managedkafka-cluster", c.Location+"/"+c.Name)
	op, err := p.storeOperation(ctx, nr, location, "create", target, p.clusterMap(nr, c))
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(op), nil
}

func (p *Provider) GetCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	clusterID := strParam(nr, "clusterId")
	if location == "" || clusterID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or clusterId", 400)
	}
	c, err := p.store.GetCluster(ctx, nr.AccountID, location, clusterID)
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(p.clusterMap(nr, c)), nil
}

func (p *Provider) ListClusters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	if location == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location", 400)
	}
	clusters, err := p.store.ListClusters(ctx, nr.AccountID, location)
	if err != nil {
		return nil, err
	}
	page, next := paging.Page(clusters, func(c mkstore.Cluster) string { return c.Name }, nr.Params)
	items := make([]any, 0, len(page))
	for _, c := range page {
		items = append(items, p.clusterMap(nr, c))
	}
	resp := map[string]any{"clusters": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) UpdateCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	clusterID := strParam(nr, "clusterId")
	if location == "" || clusterID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or clusterId", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	c, err := p.store.UpdateClusterAtomic(ctx, nr.AccountID, location, clusterID, func(c mkstore.Cluster) (mkstore.Cluster, error) {
		if body != nil {
			stored := map[string]any{}
			if len(c.Config) > 0 {
				_ = json.Unmarshal(c.Config, &stored)
			}
			for k, v := range body {
				stored[k] = v
			}
			if labels := bodyStringMap(body, "labels"); labels != nil {
				c.Labels = labels
			}
			if data, err := json.Marshal(stored); err == nil {
				c.Config = data
			}
		}
		c.UpdateTime = clock.Now().UTC()
		return c, nil
	})
	if err != nil {
		return nil, mapErr(err)
	}
	target := nr.ResourceID("managedkafka-cluster", c.Location+"/"+c.Name)
	op, err := p.storeOperation(ctx, nr, location, "update", target, p.clusterMap(nr, c))
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(op), nil
}

func (p *Provider) DeleteCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	clusterID := strParam(nr, "clusterId")
	if location == "" || clusterID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or clusterId", 400)
	}
	if err := p.store.DeleteCluster(ctx, nr.AccountID, location, clusterID); err != nil {
		return nil, mapErr(err)
	}
	target := nr.ResourceID("managedkafka-cluster", location+"/"+clusterID)
	op, err := p.storeOperation(ctx, nr, location, "delete", target, map[string]any{})
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(op), nil
}

// --- Operations ---

// GetOperation serves a persisted cluster-mutation operation. On a single host
// the locations/{location}/operations/{id} path is routed to Cloud Workflows
// (see the package doc), so this handler is reachable only by direct dispatch;
// every operation is already returned inline with done:true, so no Managed
// Kafka client needs to poll it.
func (p *Provider) GetOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	opID := strParam(nr, "operationId")
	if location == "" || opID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or operationId", 400)
	}
	op, err := p.store.GetOperation(ctx, nr.AccountID, location, opID)
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(p.operationMap(nr, op)), nil
}

// ListOperations lists the persisted cluster-mutation operations for a
// location. Like GetOperation it is reachable only by direct dispatch.
func (p *Provider) ListOperations(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	if location == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location", 400)
	}
	ops, err := p.store.ListOperations(ctx, nr.AccountID, location)
	if err != nil {
		return nil, err
	}
	page, next := paging.Page(ops, func(op mkstore.Operation) string { return op.ID }, nr.Params)
	items := make([]any, 0, len(page))
	for _, op := range page {
		items = append(items, p.operationMap(nr, op))
	}
	resp := map[string]any{"operations": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

// --- Topics ---

func (p *Provider) CreateTopic(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	clusterID := strParam(nr, "clusterId")
	topicID := strParam(nr, "topicId")
	if location == "" || clusterID == "" || topicID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location, clusterId, or topicId", 400)
	}
	if _, err := p.store.GetCluster(ctx, nr.AccountID, location, clusterID); err != nil {
		return nil, mapErr(err)
	}
	body, _ := nr.Params["body"].(map[string]any)
	now := clock.Now().UTC()
	t := mkstore.Topic{
		Location:          location,
		ClusterName:       clusterID,
		Name:              topicID,
		PartitionCount:    bodyInt(body, "partitionCount"),
		ReplicationFactor: bodyInt(body, "replicationFactor"),
		CreateTime:        now,
		UpdateTime:        now,
	}
	if body != nil {
		if data, err := json.Marshal(body); err == nil {
			t.Config = data
		}
	}
	if err := p.store.CreateTopic(ctx, nr.AccountID, location, clusterID, t); err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(p.topicMap(nr, t)), nil
}

func (p *Provider) GetTopic(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	clusterID := strParam(nr, "clusterId")
	topicID := strParam(nr, "topicId")
	if location == "" || clusterID == "" || topicID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location, clusterId, or topicId", 400)
	}
	t, err := p.store.GetTopic(ctx, nr.AccountID, location, clusterID, topicID)
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(p.topicMap(nr, t)), nil
}

func (p *Provider) ListTopics(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	clusterID := strParam(nr, "clusterId")
	if location == "" || clusterID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or clusterId", 400)
	}
	topics, err := p.store.ListTopics(ctx, nr.AccountID, location, clusterID)
	if err != nil {
		return nil, err
	}
	page, next := paging.Page(topics, func(t mkstore.Topic) string { return t.Name }, nr.Params)
	items := make([]any, 0, len(page))
	for _, t := range page {
		items = append(items, p.topicMap(nr, t))
	}
	resp := map[string]any{"topics": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) UpdateTopic(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	clusterID := strParam(nr, "clusterId")
	topicID := strParam(nr, "topicId")
	if location == "" || clusterID == "" || topicID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location, clusterId, or topicId", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	t, err := p.store.UpdateTopicAtomic(ctx, nr.AccountID, location, clusterID, topicID, func(t mkstore.Topic) (mkstore.Topic, error) {
		if body != nil {
			if _, ok := body["partitionCount"]; ok {
				t.PartitionCount = bodyInt(body, "partitionCount")
			}
			if _, ok := body["replicationFactor"]; ok {
				t.ReplicationFactor = bodyInt(body, "replicationFactor")
			}
			stored := map[string]any{}
			if len(t.Config) > 0 {
				_ = json.Unmarshal(t.Config, &stored)
			}
			for k, v := range body {
				stored[k] = v
			}
			if data, err := json.Marshal(stored); err == nil {
				t.Config = data
			}
		}
		t.UpdateTime = clock.Now().UTC()
		return t, nil
	})
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(p.topicMap(nr, t)), nil
}

func (p *Provider) DeleteTopic(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	clusterID := strParam(nr, "clusterId")
	topicID := strParam(nr, "topicId")
	if location == "" || clusterID == "" || topicID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location, clusterId, or topicId", 400)
	}
	if err := p.store.DeleteTopic(ctx, nr.AccountID, location, clusterID, topicID); err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(map[string]any{}), nil
}

// ListConsumerGroups returns an empty list — the emulator does not track
// consumer-group state. The empty set is still passed through paging.Page so
// the list wire-shape matches the real paginated API.
func (p *Provider) ListConsumerGroups(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	var groups []string
	page, next := paging.Page(groups, func(s string) string { return s }, nr.Params)
	resp := map[string]any{"consumerGroups": page}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

// consumerGroupUnimplemented returns Unimplemented for the consumer-group
// resource operations (get/update/delete) — the emulator tracks only the
// (empty) consumer-group list, not individual groups.
func (p *Provider) consumerGroupUnimplemented(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return nil, model.NewProviderError("Unimplemented", "consumer group operations are not implemented", 404)
}
