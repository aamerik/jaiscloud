import { api } from '../client'

const BASE = '/api/ui/v1/gcp/managedkafka'

/** A Managed Kafka cluster, flattened across locations for the list. */
export interface ManagedKafkaCluster {
  id: string
  name: string
  location: string
  /** Cluster state; the emulator renders ACTIVE (its LRO settles lazily). */
  state?: string
  bootstrapAddress?: string
  labels?: Record<string, string>
  /** Verbatim cluster wire body (capacityConfig, gcpConfig, ...); detail only. */
  config?: unknown
  createTime?: string
  updateTime?: string
}

export interface ListClustersResponse {
  clusters: ManagedKafkaCluster[]
  total: number
}

/** A Managed Kafka topic. `cluster` + `location` identify its parent cluster. */
export interface ManagedKafkaTopic {
  id: string
  name: string
  location: string
  cluster: string
  partitionCount: number
  replicationFactor: number
  /** Kafka property overrides (wire `configs` map); detail only. */
  configs?: Record<string, string>
  createTime?: string
  updateTime?: string
}

export interface ListTopicsResponse {
  topics: ManagedKafkaTopic[]
  total: number
}

/** One access grant within a Managed Kafka ACL. */
export interface ManagedKafkaAclEntry {
  principal?: string
  permissionType?: string
  operation?: string
  host?: string
}

export interface ManagedKafkaAcl {
  id: string
  name: string
  location: string
  cluster: string
  resourceType?: string
  resourceName?: string
  patternType?: string
  etag?: string
  aclEntries: ManagedKafkaAclEntry[]
}

export interface ListAclsResponse {
  acls: ManagedKafkaAcl[]
  total: number
}

/** One committed partition offset of a consumer group. */
export interface ManagedKafkaConsumerGroupOffset {
  topic: string
  partition: number
  offset: number
  metadata?: string
}

export interface ManagedKafkaConsumerGroup {
  id: string
  name: string
  location: string
  cluster: string
  offsets: ManagedKafkaConsumerGroupOffset[]
}

export interface ListConsumerGroupsResponse {
  groups: ManagedKafkaConsumerGroup[]
  total: number
}

const clusterPath = (location: string, cluster: string) =>
  `${BASE}/clusters/${encodeURIComponent(location)}/${encodeURIComponent(cluster)}`

const topicPath = (location: string, cluster: string, topic: string) =>
  `${clusterPath(location, cluster)}/topics/${encodeURIComponent(topic)}`

export const listClusters = () => api.get<ListClustersResponse>(`${BASE}/clusters`)

export const getCluster = (location: string, cluster: string) =>
  api.get<ManagedKafkaCluster>(clusterPath(location, cluster))

export const listTopics = () => api.get<ListTopicsResponse>(`${BASE}/topics`)

export const listClusterTopics = (location: string, cluster: string) =>
  api.get<ListTopicsResponse>(`${clusterPath(location, cluster)}/topics`)

export const getTopic = (location: string, cluster: string, topic: string) =>
  api.get<ManagedKafkaTopic>(topicPath(location, cluster, topic))

export const listAcls = (location: string, cluster: string) =>
  api.get<ListAclsResponse>(`${clusterPath(location, cluster)}/acls`)

export const listConsumerGroups = (location: string, cluster: string) =>
  api.get<ListConsumerGroupsResponse>(`${clusterPath(location, cluster)}/consumer-groups`)
