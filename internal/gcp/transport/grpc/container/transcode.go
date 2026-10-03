package container

import (
	"strings"
	"time"

	containerpb "cloud.google.com/go/container/apiv1/containerpb"

	core "jaiscloud/internal/gcp/service/container"
	containerstore "jaiscloud/internal/gcp/store/container"
)

// timeFormat is the RFC3339 timestamp encoding GKE v1 uses for its string
// timestamps (Cluster.createTime, Operation.startTime/endTime).
const timeFormat = time.RFC3339Nano

// clusterToProto renders a stored cluster as the proto Cluster. Name is the
// short cluster id, matching the REST wire shape (the core stores the short
// name; the canonical path is derived by the caller).
func clusterToProto(c containerstore.Cluster) *containerpb.Cluster {
	out := &containerpb.Cluster{
		Name:                  c.Name,
		Location:              c.Location,
		Status:                clusterStatusToProto(c.Status),
		Endpoint:              c.Endpoint,
		SelfLink:              c.SelfLink,
		InitialClusterVersion: c.InitialClusterVersion,
		CurrentMasterVersion:  c.CurrentMasterVersion,
		CurrentNodeVersion:    c.CurrentNodeVersion,
		Network:               c.Network,
		Subnetwork:            c.Subnetwork,
		InitialNodeCount:      c.InitialNodeCount,
		ResourceLabels:        c.ResourceLabels,
		MasterAuth:            &containerpb.MasterAuth{ClusterCaCertificate: c.CaCertificate},
	}
	if !c.CreateTime.IsZero() {
		out.CreateTime = c.CreateTime.UTC().Format(timeFormat)
	}
	for _, p := range c.NodePools {
		out.NodePools = append(out.NodePools, nodePoolToProto(p))
	}
	return out
}

// nodePoolToProto renders a stored node pool as the proto NodePool.
func nodePoolToProto(p containerstore.NodePool) *containerpb.NodePool {
	return &containerpb.NodePool{
		Name:             p.Name,
		Status:           nodePoolStatusToProto(p.Status),
		InitialNodeCount: p.InitialNodeCount,
	}
}

// operationToProto renders a stored operation as the GKE proto Operation (the
// emulator's own google.container.v1.Operation shape, not
// google.longrunning.Operation).
func operationToProto(op containerstore.Operation) *containerpb.Operation {
	out := &containerpb.Operation{
		Name:          op.Name,
		Location:      op.Location,
		OperationType: operationTypeToProto(op.OperationType),
		Status:        operationStatusToProto(op.Status),
		SelfLink:      op.SelfLink,
		TargetLink:    op.TargetLink,
	}
	if !op.StartTime.IsZero() {
		out.StartTime = op.StartTime.UTC().Format(timeFormat)
	}
	if !op.EndTime.IsZero() {
		out.EndTime = op.EndTime.UTC().Format(timeFormat)
	}
	return out
}

// clusterFromProto builds the core's writable Cluster subset from a request
// Cluster. Input-only/derived fields are ignored; the core fills defaults. A
// canonical cluster path in name is reduced to its short id, mirroring the REST
// codec.
func clusterFromProto(pb *containerpb.Cluster) containerstore.Cluster {
	if pb == nil {
		return containerstore.Cluster{}
	}
	c := containerstore.Cluster{
		Name:                  pb.GetName(),
		InitialClusterVersion: pb.GetInitialClusterVersion(),
		Network:               pb.GetNetwork(),
		Subnetwork:            pb.GetSubnetwork(),
		InitialNodeCount:      pb.GetInitialNodeCount(),
		ResourceLabels:        pb.GetResourceLabels(),
	}
	if strings.Contains(c.Name, "/") {
		if _, _, id, ok := core.ParseClusterName(c.Name); ok {
			c.Name = id
		}
	}
	return c
}

func clusterStatusToProto(status string) containerpb.Cluster_Status {
	switch status {
	case containerstore.StatusProvisioning:
		return containerpb.Cluster_PROVISIONING
	case containerstore.StatusRunning:
		return containerpb.Cluster_RUNNING
	case containerstore.StatusError:
		return containerpb.Cluster_ERROR
	default:
		return containerpb.Cluster_STATUS_UNSPECIFIED
	}
}

func nodePoolStatusToProto(status string) containerpb.NodePool_Status {
	switch status {
	case containerstore.StatusProvisioning:
		return containerpb.NodePool_PROVISIONING
	case containerstore.StatusRunning:
		return containerpb.NodePool_RUNNING
	case containerstore.StatusError:
		return containerpb.NodePool_ERROR
	default:
		return containerpb.NodePool_STATUS_UNSPECIFIED
	}
}

func operationTypeToProto(opType string) containerpb.Operation_Type {
	switch opType {
	case containerstore.OperationCreateCluster:
		return containerpb.Operation_CREATE_CLUSTER
	case containerstore.OperationDeleteCluster:
		return containerpb.Operation_DELETE_CLUSTER
	default:
		return containerpb.Operation_TYPE_UNSPECIFIED
	}
}

func operationStatusToProto(status string) containerpb.Operation_Status {
	switch status {
	case containerstore.OperationStatusDone:
		return containerpb.Operation_DONE
	default:
		return containerpb.Operation_STATUS_UNSPECIFIED
	}
}
