package managedkafkaui

import (
	"encoding/json"
	"net/http"
	"sort"
	"time"

	"jaiscloud/internal/config"
	managedkafkacore "jaiscloud/internal/gcp/service/managedkafka"
	mkstore "jaiscloud/internal/gcp/store/managedkafka"
	"jaiscloud/internal/gcp/ui/uihelper"
)

// Handler serves Managed Kafka UI API requests by calling the Managed Kafka
// core.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// account resolves the project for a request, falling back to the configured
// project when the inject-config middleware has not populated the context.
func (h *Handler) account(r *http.Request) string {
	if a := uihelper.AccountFrom(r); a != "" {
		return a
	}
	return h.cfg.AccountID
}

// formatTime renders a stored timestamp as RFC3339, or "" when zero.
func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// topicConfigs extracts the Kafka property overrides (the wire `configs` map)
// from a stored topic's verbatim config body.
func topicConfigs(raw json.RawMessage) map[string]string {
	if len(raw) == 0 {
		return nil
	}
	var body struct {
		Configs map[string]string `json:"configs"`
	}
	if json.Unmarshal(raw, &body) != nil || len(body.Configs) == 0 {
		return nil
	}
	return body.Configs
}

// renderCluster flattens a stored cluster into the console row. The verbatim
// config is included only on the detail read to keep the list payload small.
func renderCluster(project string, c mkstore.Cluster, full bool) Cluster {
	// Match the wire read-back: a cluster with no live broker (the default mock
	// topology) renders the synthesized cloud.goog bootstrap address.
	addr := c.BootstrapAddress
	if addr == "" {
		addr = managedkafkacore.BootstrapAddress(project, c.Location, c.Name)
	}
	out := Cluster{
		ID:               c.Name,
		Name:             managedkafkacore.ClusterName(project, c.Location, c.Name),
		Location:         c.Location,
		State:            "ACTIVE",
		BootstrapAddress: addr,
		Labels:           c.Labels,
		CreateTime:       formatTime(c.CreateTime),
		UpdateTime:       formatTime(c.UpdateTime),
	}
	if full {
		out.Config = canonicalClusterConfig(c.Config)
	}
	return out
}

// canonicalClusterConfig narrows a stored cluster's verbatim request body to the
// config keys the wire read-back renders (capacityConfig, gcpConfig,
// rebalanceConfig, tlsConfig), dropping request-only fields such as the nested
// labels. It matches managedkafka.ClusterJSON so the console shows the same
// object the REST/gRPC APIs return.
func canonicalClusterConfig(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	var cfg map[string]json.RawMessage
	if json.Unmarshal(raw, &cfg) != nil {
		return raw
	}
	out := map[string]json.RawMessage{}
	for _, k := range []string{"capacityConfig", "gcpConfig", "rebalanceConfig", "tlsConfig"} {
		if v, ok := cfg[k]; ok {
			out[k] = v
		}
	}
	if len(out) == 0 {
		return nil
	}
	b, err := json.Marshal(out)
	if err != nil {
		return raw
	}
	return b
}

// renderTopic flattens a stored topic into the console row. The Kafka property
// overrides are included only on the detail read.
func renderTopic(project string, t mkstore.Topic, full bool) Topic {
	out := Topic{
		ID:                t.Name,
		Name:              managedkafkacore.TopicName(project, t.Location, t.ClusterName, t.Name),
		Location:          t.Location,
		Cluster:           t.ClusterName,
		PartitionCount:    t.PartitionCount,
		ReplicationFactor: t.ReplicationFactor,
		CreateTime:        formatTime(t.CreateTime),
		UpdateTime:        formatTime(t.UpdateTime),
	}
	if full {
		out.Configs = topicConfigs(t.Config)
	}
	return out
}

// renderAcl flattens a stored ACL into the console row.
func renderAcl(project string, a mkstore.Acl) Acl {
	entries := make([]AclEntry, 0, len(a.AclEntries))
	for _, e := range a.AclEntries {
		entries = append(entries, AclEntry{
			Principal:      e.Principal,
			PermissionType: e.PermissionType,
			Operation:      e.Operation,
			Host:           e.Host,
		})
	}
	return Acl{
		ID:           a.Name,
		Name:         managedkafkacore.AclName(project, a.Location, a.ClusterName, a.Name),
		Location:     a.Location,
		Cluster:      a.ClusterName,
		ResourceType: a.ResourceType,
		ResourceName: a.ResourceName,
		PatternType:  a.PatternType,
		Etag:         a.Etag,
		Entries:      entries,
	}
}

// renderConsumerGroup flattens a group's topic-keyed committed offsets into the
// console's sorted row.
func renderConsumerGroup(project string, g managedkafkacore.ConsumerGroup) ConsumerGroup {
	offsets := make([]ConsumerGroupOffset, 0)
	for topic, gt := range g.Topics {
		for partition, meta := range gt.Partitions {
			offsets = append(offsets, ConsumerGroupOffset{
				Topic:     topic,
				Partition: partition,
				Offset:    meta.Offset,
				Metadata:  meta.Metadata,
			})
		}
	}
	sort.Slice(offsets, func(i, j int) bool {
		if offsets[i].Topic != offsets[j].Topic {
			return offsets[i].Topic < offsets[j].Topic
		}
		return offsets[i].Partition < offsets[j].Partition
	})
	return ConsumerGroup{
		ID:       g.Name,
		Name:     managedkafkacore.ConsumerGroupName(project, g.Location, g.Cluster, g.Name),
		Location: g.Location,
		Cluster:  g.Cluster,
		Offsets:  offsets,
	}
}

// ─── Clusters ────────────────────────────────────────────────────────────────

// GET /clusters
func (h *Handler) ListClusters(w http.ResponseWriter, r *http.Request) {
	project := h.account(r)
	clusters, err := h.provider.ListAllClusters(r.Context(), project)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	out := make([]Cluster, 0, len(clusters))
	for _, c := range clusters {
		out = append(out, renderCluster(project, c, false))
	}
	uihelper.WriteJSON(w, ListClustersResponse{Clusters: out, Total: len(out)})
}

// GET /clusters/{location}/{cluster}
func (h *Handler) GetCluster(w http.ResponseWriter, r *http.Request) {
	location, cluster, ok := h.target(w, r, "cluster")
	if !ok {
		return
	}
	project := h.account(r)
	c, err := h.provider.GetCluster(r.Context(), project, location, cluster)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, renderCluster(project, c, true))
}

// ─── Topics ──────────────────────────────────────────────────────────────────

// GET /topics
func (h *Handler) ListTopics(w http.ResponseWriter, r *http.Request) {
	project := h.account(r)
	topics, err := h.provider.ListAllTopics(r.Context(), project)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	h.writeTopics(w, project, topics)
}

// GET /clusters/{location}/{cluster}/topics
func (h *Handler) ListClusterTopics(w http.ResponseWriter, r *http.Request) {
	location, cluster, ok := h.target(w, r, "cluster")
	if !ok {
		return
	}
	project := h.account(r)
	topics, err := h.provider.ListTopics(r.Context(), project, location, cluster)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	h.writeTopics(w, project, topics)
}

// GET /clusters/{location}/{cluster}/topics/{topic}
func (h *Handler) GetTopic(w http.ResponseWriter, r *http.Request) {
	location, cluster, ok := h.target(w, r, "cluster")
	if !ok {
		return
	}
	topic, ok := uihelper.Segment(r, "topic")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid topic", http.StatusBadRequest)
		return
	}
	project := h.account(r)
	t, err := h.provider.GetTopic(r.Context(), project, location, cluster, topic)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, renderTopic(project, t, true))
}

func (h *Handler) writeTopics(w http.ResponseWriter, project string, topics []mkstore.Topic) {
	out := make([]Topic, 0, len(topics))
	for _, t := range topics {
		out = append(out, renderTopic(project, t, false))
	}
	uihelper.WriteJSON(w, ListTopicsResponse{Topics: out, Total: len(out)})
}

// ─── ACLs (read-only) ────────────────────────────────────────────────────────

// GET /clusters/{location}/{cluster}/acls
func (h *Handler) ListAcls(w http.ResponseWriter, r *http.Request) {
	location, cluster, ok := h.target(w, r, "cluster")
	if !ok {
		return
	}
	project := h.account(r)
	acls, err := h.provider.ListAcls(r.Context(), project, location, cluster)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	out := make([]Acl, 0, len(acls))
	for _, a := range acls {
		out = append(out, renderAcl(project, a))
	}
	uihelper.WriteJSON(w, ListAclsResponse{Acls: out, Total: len(out)})
}

// ─── Consumer groups (read-only) ─────────────────────────────────────────────

// GET /clusters/{location}/{cluster}/consumer-groups
func (h *Handler) ListConsumerGroups(w http.ResponseWriter, r *http.Request) {
	location, cluster, ok := h.target(w, r, "cluster")
	if !ok {
		return
	}
	project := h.account(r)
	groups, err := h.provider.ListConsumerGroups(r.Context(), project, location, cluster)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	out := make([]ConsumerGroup, 0, len(groups))
	for _, g := range groups {
		out = append(out, renderConsumerGroup(project, g))
	}
	uihelper.WriteJSON(w, ListConsumerGroupsResponse{Groups: out, Total: len(out)})
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// target reads and validates the {location}/{<resource>} path parameters,
// writing a 400 and returning false when either is missing or malformed. key is
// the chi parameter name of the resource id segment ("cluster").
func (h *Handler) target(w http.ResponseWriter, r *http.Request, key string) (string, string, bool) {
	location, ok := uihelper.Segment(r, "location")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid location", http.StatusBadRequest)
		return "", "", false
	}
	id, ok := uihelper.Segment(r, key)
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid "+key, http.StatusBadRequest)
		return "", "", false
	}
	return location, id, true
}
