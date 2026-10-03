// Package managedkafkaui serves the Managed Kafka (Apache Kafka for BigQuery)
// console UI API. Handlers call the transport-neutral Managed Kafka core
// directly (in-process) rather than over the wire.
//
// The console is location-optional: the cluster and topic lists aggregate every
// resource in the project across all locations (no location picker) and show
// the location as a read-only field. Detail/actions link back with the location
// carried from the list row, so a resource id never has to be disambiguated by
// hand.
//
// This surface is read + cluster lifecycle only: clusters list/detail, topics
// list/detail, and read-only ACL and consumer-group tabs on the cluster detail.
// Cluster create/update, topic create and all ACL/consumer-group writes are
// deferred (see the console-UI plan's deferral rows).
package managedkafkaui

import "encoding/json"

// Cluster is the console rendering of a Managed Kafka cluster, flattened across
// locations for the list page. Config carries the caller's wire body verbatim
// and is only populated on the detail read, not the list.
type Cluster struct {
	ID               string            `json:"id"`
	Name             string            `json:"name"`
	Location         string            `json:"location"`
	State            string            `json:"state,omitempty"`
	BootstrapAddress string            `json:"bootstrapAddress,omitempty"`
	Labels           map[string]string `json:"labels,omitempty"`
	Config           json.RawMessage   `json:"config,omitempty"`
	CreateTime       string            `json:"createTime,omitempty"`
	UpdateTime       string            `json:"updateTime,omitempty"`
}

// ListClustersResponse is the response for GET /clusters.
type ListClustersResponse struct {
	Clusters []Cluster `json:"clusters"`
	Total    int       `json:"total"`
}

// Topic is the console rendering of a Managed Kafka topic. Cluster and Location
// identify the parent cluster, so a topic row is addressable without context.
// Configs holds the Kafka property overrides (the wire `configs` map) and is
// only populated on the detail read.
type Topic struct {
	ID                string            `json:"id"`
	Name              string            `json:"name"`
	Location          string            `json:"location"`
	Cluster           string            `json:"cluster"`
	PartitionCount    int               `json:"partitionCount"`
	ReplicationFactor int               `json:"replicationFactor"`
	Configs           map[string]string `json:"configs,omitempty"`
	CreateTime        string            `json:"createTime,omitempty"`
	UpdateTime        string            `json:"updateTime,omitempty"`
}

// ListTopicsResponse is the response for GET /topics and
// GET /clusters/{location}/{cluster}/topics.
type ListTopicsResponse struct {
	Topics []Topic `json:"topics"`
	Total  int     `json:"total"`
}

// AclEntry is one access grant within an ACL.
type AclEntry struct {
	Principal      string `json:"principal,omitempty"`
	PermissionType string `json:"permissionType,omitempty"`
	Operation      string `json:"operation,omitempty"`
	Host           string `json:"host,omitempty"`
}

// Acl is the console rendering of a cluster ACL.
type Acl struct {
	ID           string     `json:"id"`
	Name         string     `json:"name"`
	Location     string     `json:"location"`
	Cluster      string     `json:"cluster"`
	ResourceType string     `json:"resourceType,omitempty"`
	ResourceName string     `json:"resourceName,omitempty"`
	PatternType  string     `json:"patternType,omitempty"`
	Etag         string     `json:"etag,omitempty"`
	Entries      []AclEntry `json:"aclEntries"`
}

// ListAclsResponse is the response for GET /clusters/{location}/{cluster}/acls.
type ListAclsResponse struct {
	Acls  []Acl `json:"acls"`
	Total int   `json:"total"`
}

// ConsumerGroupOffset is one committed partition offset, flattened from the
// core's topic-keyed map for the console table.
type ConsumerGroupOffset struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Metadata  string `json:"metadata,omitempty"`
}

// ConsumerGroup is the console rendering of a Kafka consumer group. Offsets is
// flattened from the core's topic-keyed map and sorted by topic then partition.
// With the default mock topology (no live broker) the list is empty.
type ConsumerGroup struct {
	ID       string                `json:"id"`
	Name     string                `json:"name"`
	Location string                `json:"location"`
	Cluster  string                `json:"cluster"`
	Offsets  []ConsumerGroupOffset `json:"offsets"`
}

// ListConsumerGroupsResponse is the response for
// GET /clusters/{location}/{cluster}/consumer-groups.
type ListConsumerGroupsResponse struct {
	Groups []ConsumerGroup `json:"groups"`
	Total  int             `json:"total"`
}
