package grpcconformance

import (
	"context"
	"fmt"

	dataproc "cloud.google.com/go/dataproc/v2/apiv1"
	dataprocpb "cloud.google.com/go/dataproc/v2/apiv1/dataprocpb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// dataprocChecks covers the Cloud Dataproc v1 surface
// (google.cloud.dataproc.v1.ClusterController and JobController) via the
// official generated cloud.google.com/go/dataproc/v2 client: cluster CRUD +
// start/stop, and job submit/get/list/update/cancel/delete. Cluster mutations
// and SubmitJobAsOperation are long-running operations whose response is packed
// inline as a typed Any, so the client's Wait observes them without polling.
//
// DiagnoseCluster is deliberately not probed: it is an explicit Unimplemented
// stub. Every probe is self-contained (a run-unique cluster/job via
// cfg.ResourceName), so a long-lived emulator never sees cross-run collisions.
func dataprocChecks() []Check {
	return []Check{
		{Service: "dataproc", RPC: "CreateCluster", Method: "CreateCluster", KeyField: "LRO done + RUNNING cluster", Run: checkDPCreateCluster},
		{Service: "dataproc", RPC: "GetCluster", Method: "GetCluster", KeyField: "clusterName round-trip", Run: checkDPGetCluster},
		{Service: "dataproc", RPC: "ListClusters", Method: "ListClusters", KeyField: "created cluster present", Run: checkDPListClusters},
		{Service: "dataproc", RPC: "UpdateCluster", Method: "UpdateCluster", KeyField: "labels updated via LRO", Run: checkDPUpdateCluster},
		{Service: "dataproc", RPC: "StopCluster", Method: "StopCluster", KeyField: "STOPPED cluster returned", Run: checkDPStopCluster},
		{Service: "dataproc", RPC: "StartCluster", Method: "StartCluster", KeyField: "RUNNING cluster returned", Run: checkDPStartCluster},
		{Service: "dataproc", RPC: "DeleteCluster", Method: "DeleteCluster", KeyField: "NotFound after delete", Run: checkDPDeleteCluster},
		{Service: "dataproc", RPC: "SubmitJob", Method: "SubmitJob", KeyField: "PENDING job polls to DONE", Run: checkDPSubmitJob},
		{Service: "dataproc", RPC: "SubmitJobAsOperation", Method: "SubmitJobAsOperation", KeyField: "in-flight LRO -> typed Job response", Run: checkDPSubmitJobAsOperation},
		{Service: "dataproc", RPC: "GetJob", Method: "GetJob", KeyField: "job reference round-trip", Run: checkDPGetJob},
		{Service: "dataproc", RPC: "ListJobs", Method: "ListJobs", KeyField: "submitted job present", Run: checkDPListJobs},
		{Service: "dataproc", RPC: "UpdateJob", Method: "UpdateJob", KeyField: "labels updated", Run: checkDPUpdateJob},
		{Service: "dataproc", RPC: "CancelJob", Method: "CancelJob", KeyField: "CANCEL_PENDING polls to CANCELLED", Run: checkDPCancelJob},
		{Service: "dataproc", RPC: "DeleteJob", Method: "DeleteJob", KeyField: "NotFound after delete", Run: checkDPDeleteJob},
	}
}

const dataprocRegion = "us-central1"

func dataprocClientOptions(cfg Config) []option.ClientOption {
	return []option.ClientOption{
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	}
}

func newDataprocClusterClient(ctx context.Context, cfg Config) (*dataproc.ClusterControllerClient, error) {
	return dataproc.NewClusterControllerClient(ctx, dataprocClientOptions(cfg)...)
}

func newDataprocJobClient(ctx context.Context, cfg Config) (*dataproc.JobControllerClient, error) {
	return dataproc.NewJobControllerClient(ctx, dataprocClientOptions(cfg)...)
}

// dataprocClusterID is the run-unique cluster shared by the probes.
func dataprocClusterID(cfg Config) string { return cfg.ResourceName("gcpc-grpc-dp") }

// ensureDataprocCluster creates the run-unique probe cluster, treating
// AlreadyExists as success so repeated probes are idempotent.
func ensureDataprocCluster(ctx context.Context, client *dataproc.ClusterControllerClient, cfg Config) (string, error) {
	id := dataprocClusterID(cfg)
	op, err := client.CreateCluster(ctx, &dataprocpb.CreateClusterRequest{
		ProjectId: cfg.Project,
		Region:    dataprocRegion,
		Cluster:   &dataprocpb.Cluster{ClusterName: id, Labels: map[string]string{"probe": "gcpc"}},
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return id, nil
		}
		return "", fmt.Errorf("create cluster: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return "", fmt.Errorf("wait cluster: %w", err)
	}
	return id, nil
}

// dataprocJob returns a minimal run-unique PySpark job body.
func dataprocJob(jobID, cluster string) *dataprocpb.Job {
	return &dataprocpb.Job{
		Reference: &dataprocpb.JobReference{JobId: jobID},
		Placement: &dataprocpb.JobPlacement{ClusterName: cluster},
		TypeJob: &dataprocpb.Job_PysparkJob{
			PysparkJob: &dataprocpb.PySparkJob{MainPythonFileUri: "gs://bucket/main.py"},
		},
	}
}

// ─── Clusters ─────────────────────────────────────────────────────────────────

// Check 1: CreateCluster returns a done operation whose response is a RUNNING
// cluster.
func checkDPCreateCluster(ctx context.Context, cfg Config) error {
	client, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-grpc-dp-create")
	op, err := client.CreateCluster(ctx, &dataprocpb.CreateClusterRequest{
		ProjectId: cfg.Project,
		Region:    dataprocRegion,
		Cluster:   &dataprocpb.Cluster{ClusterName: id, Labels: map[string]string{"probe": "gcpc"}},
	})
	if err != nil {
		return fmt.Errorf("CreateCluster: %w", err)
	}
	// Cluster create is asynchronous: the operation is returned in flight and
	// completes only when polled through google.longrunning.Operations.
	if op.Done() {
		return fmt.Errorf("CreateCluster completed inline; want an in-flight LRO")
	}
	meta, err := op.Metadata()
	if err != nil {
		return fmt.Errorf("operation metadata: %w", err)
	}
	if meta.GetClusterName() != id || meta.GetOperationType() != "CREATE" {
		return fmt.Errorf("operation metadata = %+v", meta)
	}
	if _, err := op.Poll(ctx); err != nil {
		return fmt.Errorf("poll create operation: %w", err)
	}
	cluster, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if cluster.GetClusterName() != id {
		return fmt.Errorf("cluster name = %q, want %q", cluster.GetClusterName(), id)
	}
	if cluster.GetStatus().GetState() != dataprocpb.ClusterStatus_RUNNING {
		return fmt.Errorf("cluster state = %v, want RUNNING", cluster.GetStatus().GetState())
	}
	return nil
}

// Check 2: GetCluster round-trips.
func checkDPGetCluster(ctx context.Context, cfg Config) error {
	client, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id, err := ensureDataprocCluster(ctx, client, cfg)
	if err != nil {
		return err
	}
	got, err := client.GetCluster(ctx, &dataprocpb.GetClusterRequest{ProjectId: cfg.Project, Region: dataprocRegion, ClusterName: id})
	if err != nil {
		return fmt.Errorf("GetCluster: %w", err)
	}
	if got.GetClusterName() != id || got.GetLabels()["probe"] != "gcpc" {
		return fmt.Errorf("cluster = %+v", got)
	}
	return nil
}

// Check 3: ListClusters includes the probe cluster.
func checkDPListClusters(ctx context.Context, cfg Config) error {
	client, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id, err := ensureDataprocCluster(ctx, client, cfg)
	if err != nil {
		return err
	}
	it := client.ListClusters(ctx, &dataprocpb.ListClustersRequest{ProjectId: cfg.Project, Region: dataprocRegion})
	for {
		got, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListClusters did not include %q", id)
		}
		if err != nil {
			return fmt.Errorf("ListClusters: %w", err)
		}
		if got.GetClusterName() == id {
			return nil
		}
	}
}

// Check 4: UpdateCluster applies labels through an LRO.
func checkDPUpdateCluster(ctx context.Context, cfg Config) error {
	client, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id, err := ensureDataprocCluster(ctx, client, cfg)
	if err != nil {
		return err
	}
	op, err := client.UpdateCluster(ctx, &dataprocpb.UpdateClusterRequest{
		ProjectId:   cfg.Project,
		Region:      dataprocRegion,
		ClusterName: id,
		Cluster:     &dataprocpb.Cluster{Labels: map[string]string{"updated": "yes"}},
		UpdateMask:  &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateCluster: %w", err)
	}
	cluster, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if cluster.GetLabels()["updated"] != "yes" {
		return fmt.Errorf("updated labels = %v", cluster.GetLabels())
	}
	return nil
}

// Check 5: StopCluster returns a STOPPED cluster.
func checkDPStopCluster(ctx context.Context, cfg Config) error {
	client, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id, err := ensureDataprocCluster(ctx, client, cfg)
	if err != nil {
		return err
	}
	op, err := client.StopCluster(ctx, &dataprocpb.StopClusterRequest{ProjectId: cfg.Project, Region: dataprocRegion, ClusterName: id})
	if err != nil {
		return fmt.Errorf("StopCluster: %w", err)
	}
	cluster, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if cluster.GetStatus().GetState() != dataprocpb.ClusterStatus_STOPPED {
		return fmt.Errorf("cluster state = %v, want STOPPED", cluster.GetStatus().GetState())
	}
	return nil
}

// Check 6: StartCluster returns a RUNNING cluster.
func checkDPStartCluster(ctx context.Context, cfg Config) error {
	client, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id, err := ensureDataprocCluster(ctx, client, cfg)
	if err != nil {
		return err
	}
	op, err := client.StartCluster(ctx, &dataprocpb.StartClusterRequest{ProjectId: cfg.Project, Region: dataprocRegion, ClusterName: id})
	if err != nil {
		return fmt.Errorf("StartCluster: %w", err)
	}
	cluster, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if cluster.GetStatus().GetState() != dataprocpb.ClusterStatus_RUNNING {
		return fmt.Errorf("cluster state = %v, want RUNNING", cluster.GetStatus().GetState())
	}
	return nil
}

// Check 7: DeleteCluster removes the cluster.
func checkDPDeleteCluster(ctx context.Context, cfg Config) error {
	client, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-grpc-dp-del")
	if op, err := client.CreateCluster(ctx, &dataprocpb.CreateClusterRequest{
		ProjectId: cfg.Project, Region: dataprocRegion, Cluster: &dataprocpb.Cluster{ClusterName: id},
	}); err != nil {
		return fmt.Errorf("create: %w", err)
	} else if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("create wait: %w", err)
	}
	delOp, err := client.DeleteCluster(ctx, &dataprocpb.DeleteClusterRequest{ProjectId: cfg.Project, Region: dataprocRegion, ClusterName: id})
	if err != nil {
		return fmt.Errorf("DeleteCluster: %w", err)
	}
	// Delete is asynchronous as well: poll the operation to completion.
	if delOp.Done() {
		return fmt.Errorf("DeleteCluster completed inline; want an in-flight LRO")
	}
	if err := delOp.Poll(ctx); err != nil {
		return fmt.Errorf("poll delete operation: %w", err)
	}
	if err := delOp.Wait(ctx); err != nil {
		return fmt.Errorf("delete wait: %w", err)
	}
	if _, err := client.GetCluster(ctx, &dataprocpb.GetClusterRequest{ProjectId: cfg.Project, Region: dataprocRegion, ClusterName: id}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetCluster after delete = %v, want NotFound", err)
	}
	return nil
}

// ─── Jobs ─────────────────────────────────────────────────────────────────────

// dpPollJobTerminal polls GetJob until the job reaches a terminal state,
// driving the emulator's lazy job state machine one hop per read.
func dpPollJobTerminal(ctx context.Context, jc *dataproc.JobControllerClient, project, jobID string) (*dataprocpb.Job, error) {
	for i := 0; i < 32; i++ {
		j, err := jc.GetJob(ctx, &dataprocpb.GetJobRequest{ProjectId: project, Region: dataprocRegion, JobId: jobID})
		if err != nil {
			return nil, fmt.Errorf("GetJob: %w", err)
		}
		switch j.GetStatus().GetState() {
		case dataprocpb.JobStatus_DONE, dataprocpb.JobStatus_ERROR, dataprocpb.JobStatus_CANCELLED:
			return j, nil
		}
	}
	return nil, fmt.Errorf("job %s did not reach a terminal state", jobID)
}

// Check 8: SubmitJob returns an in-flight job that walks to DONE in mock mode.
func checkDPSubmitJob(ctx context.Context, cfg Config) error {
	cc, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer cc.Close()
	cluster, err := ensureDataprocCluster(ctx, cc, cfg)
	if err != nil {
		return err
	}
	jc, err := newDataprocJobClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer jc.Close()
	jobID := cfg.ResourceName("gcpc-grpc-dp-job")
	j, err := jc.SubmitJob(ctx, &dataprocpb.SubmitJobRequest{ProjectId: cfg.Project, Region: dataprocRegion, Job: dataprocJob(jobID, cluster)})
	if err != nil {
		return fmt.Errorf("SubmitJob: %w", err)
	}
	if j.GetReference().GetJobId() != jobID {
		return fmt.Errorf("job reference = %+v", j.GetReference())
	}
	if j.GetStatus().GetState() != dataprocpb.JobStatus_PENDING {
		return fmt.Errorf("submitted job state = %v, want PENDING", j.GetStatus().GetState())
	}
	final, err := dpPollJobTerminal(ctx, jc, cfg.Project, jobID)
	if err != nil {
		return err
	}
	if final.GetStatus().GetState() != dataprocpb.JobStatus_DONE {
		return fmt.Errorf("job state = %v, want DONE", final.GetStatus().GetState())
	}
	return nil
}

// Check 9: SubmitJobAsOperation returns an in-flight LRO that completes with a
// typed Job response.
func checkDPSubmitJobAsOperation(ctx context.Context, cfg Config) error {
	cc, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer cc.Close()
	cluster, err := ensureDataprocCluster(ctx, cc, cfg)
	if err != nil {
		return err
	}
	jc, err := newDataprocJobClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer jc.Close()
	jobID := cfg.ResourceName("gcpc-grpc-dp-jobsop")
	op, err := jc.SubmitJobAsOperation(ctx, &dataprocpb.SubmitJobRequest{ProjectId: cfg.Project, Region: dataprocRegion, Job: dataprocJob(jobID, cluster)})
	if err != nil {
		return fmt.Errorf("SubmitJobAsOperation: %w", err)
	}
	// The job is asynchronous: the operation is returned in flight and
	// completes when polled through google.longrunning.Operations.
	if op.Done() {
		return fmt.Errorf("SubmitJobAsOperation completed inline; want an in-flight LRO")
	}
	meta, err := op.Metadata()
	if err != nil {
		return fmt.Errorf("operation metadata: %w", err)
	}
	if meta.GetJobId() != jobID {
		return fmt.Errorf("operation metadata = %+v", meta)
	}
	job, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if job.GetReference().GetJobId() != jobID {
		return fmt.Errorf("job = %+v", job)
	}
	if job.GetStatus().GetState() != dataprocpb.JobStatus_DONE {
		return fmt.Errorf("job state = %v, want DONE", job.GetStatus().GetState())
	}
	return nil
}

// Check 10: GetJob round-trips.
func checkDPGetJob(ctx context.Context, cfg Config) error {
	cc, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer cc.Close()
	cluster, err := ensureDataprocCluster(ctx, cc, cfg)
	if err != nil {
		return err
	}
	jc, err := newDataprocJobClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer jc.Close()
	jobID := cfg.ResourceName("gcpc-grpc-dp-job-get")
	if _, err := jc.SubmitJob(ctx, &dataprocpb.SubmitJobRequest{ProjectId: cfg.Project, Region: dataprocRegion, Job: dataprocJob(jobID, cluster)}); err != nil {
		return fmt.Errorf("SubmitJob: %w", err)
	}
	got, err := jc.GetJob(ctx, &dataprocpb.GetJobRequest{ProjectId: cfg.Project, Region: dataprocRegion, JobId: jobID})
	if err != nil {
		return fmt.Errorf("GetJob: %w", err)
	}
	if got.GetReference().GetJobId() != jobID {
		return fmt.Errorf("job = %+v", got)
	}
	return nil
}

// Check 11: ListJobs includes the probe job.
func checkDPListJobs(ctx context.Context, cfg Config) error {
	cc, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer cc.Close()
	cluster, err := ensureDataprocCluster(ctx, cc, cfg)
	if err != nil {
		return err
	}
	jc, err := newDataprocJobClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer jc.Close()
	jobID := cfg.ResourceName("gcpc-grpc-dp-job-list")
	if _, err := jc.SubmitJob(ctx, &dataprocpb.SubmitJobRequest{ProjectId: cfg.Project, Region: dataprocRegion, Job: dataprocJob(jobID, cluster)}); err != nil {
		return fmt.Errorf("SubmitJob: %w", err)
	}
	it := jc.ListJobs(ctx, &dataprocpb.ListJobsRequest{ProjectId: cfg.Project, Region: dataprocRegion})
	for {
		got, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListJobs did not include %q", jobID)
		}
		if err != nil {
			return fmt.Errorf("ListJobs: %w", err)
		}
		if got.GetReference().GetJobId() == jobID {
			return nil
		}
	}
}

// Check 12: UpdateJob applies labels.
func checkDPUpdateJob(ctx context.Context, cfg Config) error {
	cc, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer cc.Close()
	cluster, err := ensureDataprocCluster(ctx, cc, cfg)
	if err != nil {
		return err
	}
	jc, err := newDataprocJobClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer jc.Close()
	jobID := cfg.ResourceName("gcpc-grpc-dp-job-upd")
	if _, err := jc.SubmitJob(ctx, &dataprocpb.SubmitJobRequest{ProjectId: cfg.Project, Region: dataprocRegion, Job: dataprocJob(jobID, cluster)}); err != nil {
		return fmt.Errorf("SubmitJob: %w", err)
	}
	got, err := jc.UpdateJob(ctx, &dataprocpb.UpdateJobRequest{
		ProjectId:  cfg.Project,
		Region:     dataprocRegion,
		JobId:      jobID,
		Job:        &dataprocpb.Job{Reference: &dataprocpb.JobReference{JobId: jobID}, Labels: map[string]string{"updated": "yes"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateJob: %w", err)
	}
	if got.GetLabels()["updated"] != "yes" {
		return fmt.Errorf("job labels = %v", got.GetLabels())
	}
	return nil
}

// Check 13: CancelJob starts the cancel progression; polling settles CANCELLED.
func checkDPCancelJob(ctx context.Context, cfg Config) error {
	cc, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer cc.Close()
	cluster, err := ensureDataprocCluster(ctx, cc, cfg)
	if err != nil {
		return err
	}
	jc, err := newDataprocJobClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer jc.Close()
	jobID := cfg.ResourceName("gcpc-grpc-dp-job-cancel")
	if _, err := jc.SubmitJob(ctx, &dataprocpb.SubmitJobRequest{ProjectId: cfg.Project, Region: dataprocRegion, Job: dataprocJob(jobID, cluster)}); err != nil {
		return fmt.Errorf("SubmitJob: %w", err)
	}
	// CancelJob starts the cancel progression (CANCEL_PENDING); the job settles
	// to CANCELLED when polled.
	if _, err := jc.CancelJob(ctx, &dataprocpb.CancelJobRequest{ProjectId: cfg.Project, Region: dataprocRegion, JobId: jobID}); err != nil {
		return fmt.Errorf("CancelJob: %w", err)
	}
	final, err := dpPollJobTerminal(ctx, jc, cfg.Project, jobID)
	if err != nil {
		return err
	}
	if final.GetStatus().GetState() != dataprocpb.JobStatus_CANCELLED {
		return fmt.Errorf("job state after cancel = %v, want CANCELLED", final.GetStatus().GetState())
	}
	return nil
}

// Check 14: DeleteJob removes a terminal job.
func checkDPDeleteJob(ctx context.Context, cfg Config) error {
	cc, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer cc.Close()
	cluster, err := ensureDataprocCluster(ctx, cc, cfg)
	if err != nil {
		return err
	}
	jc, err := newDataprocJobClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer jc.Close()
	jobID := cfg.ResourceName("gcpc-grpc-dp-job-del")
	if _, err := jc.SubmitJob(ctx, &dataprocpb.SubmitJobRequest{ProjectId: cfg.Project, Region: dataprocRegion, Job: dataprocJob(jobID, cluster)}); err != nil {
		return fmt.Errorf("SubmitJob: %w", err)
	}
	// Only a terminal job can be deleted; poll it to DONE first.
	if _, err := dpPollJobTerminal(ctx, jc, cfg.Project, jobID); err != nil {
		return err
	}
	if err := jc.DeleteJob(ctx, &dataprocpb.DeleteJobRequest{ProjectId: cfg.Project, Region: dataprocRegion, JobId: jobID}); err != nil {
		return fmt.Errorf("DeleteJob: %w", err)
	}
	if _, err := jc.GetJob(ctx, &dataprocpb.GetJobRequest{ProjectId: cfg.Project, Region: dataprocRegion, JobId: jobID}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetJob after delete = %v, want NotFound", err)
	}
	return nil
}
