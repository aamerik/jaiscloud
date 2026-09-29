package dataproc

import (
	"context"
	"testing"

	dataprocpb "cloud.google.com/go/dataproc/v2/apiv1/dataprocpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"jaiscloud/internal/clock"
	core "jaiscloud/internal/gcp/service/dataproc"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/store"
)

func newTestService() *Service {
	return NewService(core.NewService(dpstore.NewMemoryStore(), store.NewMemoryResourceStore()), "proj")
}

func createClusterReq(id string) *dataprocpb.CreateClusterRequest {
	return &dataprocpb.CreateClusterRequest{
		ProjectId: "proj",
		Region:    "us-central1",
		Cluster:   &dataprocpb.Cluster{ClusterName: id, Labels: map[string]string{"env": "dev"}},
	}
}

func TestCreateCluster_TypedOperation(t *testing.T) {
	s := newTestService()
	ctx := context.Background()
	op, err := s.CreateCluster(ctx, createClusterReq("c1"))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	// Cluster create is asynchronous: the returned operation is in flight.
	if op.GetDone() {
		t.Fatal("create operation unexpectedly done")
	}
	var meta dataprocpb.ClusterOperationMetadata
	if err := op.GetMetadata().UnmarshalTo(&meta); err != nil {
		t.Fatalf("metadata UnmarshalTo: %v", err)
	}
	if meta.GetClusterName() != "c1" || meta.GetOperationType() != "CREATE" {
		t.Fatalf("metadata = %+v", &meta)
	}
	// Polling through the longrunning Operations resolver advances the cluster
	// and completes the operation with the typed Cluster response.
	polled, handled, err := s.ResolveOperation(ctx, op.GetName())
	if err != nil || !handled {
		t.Fatalf("ResolveOperation: handled=%v err=%v", handled, err)
	}
	if !polled.GetDone() {
		t.Fatal("polled operation not done")
	}
	var cl dataprocpb.Cluster
	if err := polled.GetResponse().UnmarshalTo(&cl); err != nil {
		t.Fatalf("response UnmarshalTo: %v", err)
	}
	if cl.GetClusterName() != "c1" || cl.GetStatus().GetState() != dataprocpb.ClusterStatus_RUNNING {
		t.Fatalf("cluster = %+v", &cl)
	}
}

func TestGetCluster_RoundTrip(t *testing.T) {
	s := newTestService()
	if _, err := s.CreateCluster(context.Background(), createClusterReq("c1")); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	cl, err := s.GetCluster(context.Background(), &dataprocpb.GetClusterRequest{
		ProjectId: "proj", Region: "us-central1", ClusterName: "c1",
	})
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if cl.GetProjectId() != "proj" || cl.GetClusterName() != "c1" || cl.GetLabels()["env"] != "dev" {
		t.Fatalf("cluster = %+v", cl)
	}
}

func TestListClusters_IncludesCreated(t *testing.T) {
	s := newTestService()
	if _, err := s.CreateCluster(context.Background(), createClusterReq("c1")); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	resp, err := s.ListClusters(context.Background(), &dataprocpb.ListClustersRequest{ProjectId: "proj", Region: "us-central1"})
	if err != nil {
		t.Fatalf("ListClusters: %v", err)
	}
	if len(resp.GetClusters()) != 1 || resp.GetClusters()[0].GetClusterName() != "c1" {
		t.Fatalf("clusters = %+v", resp.GetClusters())
	}
}

func TestUpdateCluster_LabelsMask(t *testing.T) {
	s := newTestService()
	ctx := context.Background()
	if _, err := s.CreateCluster(ctx, createClusterReq("c1")); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	op, err := s.UpdateCluster(ctx, &dataprocpb.UpdateClusterRequest{
		ProjectId:   "proj",
		Region:      "us-central1",
		ClusterName: "c1",
		Cluster:     &dataprocpb.Cluster{ClusterName: "c1", Labels: map[string]string{"env": "prod"}},
		UpdateMask:  &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		t.Fatalf("UpdateCluster: %v", err)
	}
	// The update is asynchronous: poll to settle the UPDATING transition.
	polled, handled, err := s.ResolveOperation(ctx, op.GetName())
	if err != nil || !handled {
		t.Fatalf("ResolveOperation: handled=%v err=%v", handled, err)
	}
	var cl dataprocpb.Cluster
	if err := polled.GetResponse().UnmarshalTo(&cl); err != nil {
		t.Fatalf("response UnmarshalTo: %v", err)
	}
	if cl.GetLabels()["env"] != "prod" {
		t.Fatalf("labels = %v", cl.GetLabels())
	}
}

func TestDeleteCluster_EmptyResponse(t *testing.T) {
	s := newTestService()
	ctx := context.Background()
	if _, err := s.CreateCluster(ctx, createClusterReq("c1")); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	op, err := s.DeleteCluster(ctx, &dataprocpb.DeleteClusterRequest{ProjectId: "proj", Region: "us-central1", ClusterName: "c1"})
	if err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}
	if op.GetDone() {
		t.Fatal("delete operation unexpectedly done")
	}
	// Polling settles the DELETING transition, removes the cluster, and packs
	// the empty response.
	polled, handled, err := s.ResolveOperation(ctx, op.GetName())
	if err != nil || !handled {
		t.Fatalf("ResolveOperation: handled=%v err=%v", handled, err)
	}
	if !polled.GetDone() {
		t.Fatal("polled delete operation not done")
	}
	if polled.GetResponse().GetTypeUrl() != "type.googleapis.com/google.protobuf.Empty" {
		t.Fatalf("delete response type = %q", polled.GetResponse().GetTypeUrl())
	}
	if _, err := s.GetCluster(ctx, &dataprocpb.GetClusterRequest{ProjectId: "proj", Region: "us-central1", ClusterName: "c1"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetCluster after delete = %v, want NotFound", err)
	}
}

func submitReq(jobID string) *dataprocpb.SubmitJobRequest {
	return &dataprocpb.SubmitJobRequest{
		ProjectId: "proj",
		Region:    "us-central1",
		Job: &dataprocpb.Job{
			Reference: &dataprocpb.JobReference{JobId: jobID},
			Placement: &dataprocpb.JobPlacement{ClusterName: "c1"},
			TypeJob: &dataprocpb.Job_PysparkJob{
				PysparkJob: &dataprocpb.PySparkJob{MainPythonFileUri: "gs://b/main.py"},
			},
		},
	}
}

func TestSubmitJob_MockDONE(t *testing.T) {
	s := newTestService()
	ctx := context.Background()
	if _, err := s.CreateCluster(ctx, createClusterReq("c1")); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	j, err := s.SubmitJob(ctx, submitReq("j1"))
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	if j.GetStatus().GetState() != dataprocpb.JobStatus_DONE {
		t.Fatalf("job state = %v, want DONE", j.GetStatus().GetState())
	}
	if j.GetReference().GetJobId() != "j1" {
		t.Fatalf("job reference = %+v", j.GetReference())
	}
}

func TestSubmitJobAsOperation_TypedJobResponse(t *testing.T) {
	s := newTestService()
	ctx := context.Background()
	if _, err := s.CreateCluster(ctx, createClusterReq("c1")); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	op, err := s.SubmitJobAsOperation(ctx, submitReq("j1"))
	if err != nil {
		t.Fatalf("SubmitJobAsOperation: %v", err)
	}
	if !op.GetDone() {
		t.Fatal("mock-mode submit operation should be done")
	}
	var meta dataprocpb.JobMetadata
	if err := op.GetMetadata().UnmarshalTo(&meta); err != nil {
		t.Fatalf("metadata UnmarshalTo: %v", err)
	}
	if meta.GetJobId() != "j1" {
		t.Fatalf("metadata = %+v", &meta)
	}
	var jb dataprocpb.Job
	if err := op.GetResponse().UnmarshalTo(&jb); err != nil {
		t.Fatalf("response UnmarshalTo: %v", err)
	}
	if jb.GetReference().GetJobId() != "j1" || jb.GetStatus().GetState() != dataprocpb.JobStatus_DONE {
		t.Fatalf("job = %+v", &jb)
	}
}

func TestUpdateJob_Labels(t *testing.T) {
	s := newTestService()
	ctx := context.Background()
	if _, err := s.CreateCluster(ctx, createClusterReq("c1")); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if _, err := s.SubmitJob(ctx, submitReq("j1")); err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	j, err := s.UpdateJob(ctx, &dataprocpb.UpdateJobRequest{
		ProjectId:  "proj",
		Region:     "us-central1",
		JobId:      "j1",
		Job:        &dataprocpb.Job{Reference: &dataprocpb.JobReference{JobId: "j1"}, Labels: map[string]string{"env": "prod"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	if j.GetLabels()["env"] != "prod" {
		t.Fatalf("labels = %v", j.GetLabels())
	}
}

func TestDeleteJob_TerminalSucceeds(t *testing.T) {
	s := newTestService()
	ctx := context.Background()
	if _, err := s.CreateCluster(ctx, createClusterReq("c1")); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if _, err := s.SubmitJob(ctx, submitReq("j1")); err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	if _, err := s.DeleteJob(ctx, &dataprocpb.DeleteJobRequest{ProjectId: "proj", Region: "us-central1", JobId: "j1"}); err != nil {
		t.Fatalf("DeleteJob terminal: %v", err)
	}
	if _, err := s.GetJob(ctx, &dataprocpb.GetJobRequest{ProjectId: "proj", Region: "us-central1", JobId: "j1"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetJob after delete = %v, want NotFound", err)
	}
}

// TestDeleteJob_ActiveFailsPrecondition seeds a non-terminal job directly (the
// mock executor completes jobs synchronously, so SubmitJob cannot produce one)
// and asserts the FAILED_PRECONDITION mapping.
func TestDeleteJob_ActiveFailsPrecondition(t *testing.T) {
	st := dpstore.NewMemoryStore()
	s := NewService(core.NewService(st, store.NewMemoryResourceStore()), "proj")
	ctx := context.Background()
	if err := st.CreateJob(ctx, "proj", "us-central1", dpstore.Job{
		ProjectID: "proj", Region: "us-central1", JobID: "j-active", PlacementClusterName: "c1",
		Type: "pysparkJob", Status: dpstore.JobStatus{State: "RUNNING", StateStartTime: clock.Now().UTC()},
	}); err != nil {
		t.Fatalf("seed job: %v", err)
	}
	if _, err := s.DeleteJob(ctx, &dataprocpb.DeleteJobRequest{ProjectId: "proj", Region: "us-central1", JobId: "j-active"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeleteJob active = %v, want FailedPrecondition", err)
	}
}

// TestJobToProto_SparkHybridPreservesStatusAndJar guards the cross-transport
// case where a REST-created sparkJob carries both mainJarFileUri and mainClass:
// SparkJob.driver is a proto oneof, so the renderer must move the jar into
// jarFileUris (the proto's documented encoding) instead of losing the whole job.
func TestJobToProto_SparkHybridPreservesStatusAndJar(t *testing.T) {
	j := dpstore.Job{
		ProjectID: "proj", Region: "us-central1", JobID: "j1", PlacementClusterName: "c1",
		Type:    "sparkJob",
		TypeJob: []byte(`{"mainJarFileUri":"gs://b/app.jar","mainClass":"com.example.Main"}`),
		Status:  dpstore.JobStatus{State: "RUNNING", StateStartTime: clock.Now().UTC(), Substate: "QUEUED"},
	}
	got := jobToProto(j)
	if got.GetReference().GetJobId() != "j1" {
		t.Fatalf("job reference lost: %+v", got.GetReference())
	}
	if got.GetStatus().GetState() != dataprocpb.JobStatus_RUNNING {
		t.Fatalf("status = %v, want RUNNING", got.GetStatus().GetState())
	}
	sj := got.GetSparkJob()
	if sj == nil {
		t.Fatal("sparkJob type job lost in transcode")
	}
	if sj.GetMainClass() != "com.example.Main" {
		t.Fatalf("mainClass = %q", sj.GetMainClass())
	}
	if uris := sj.GetJarFileUris(); len(uris) != 1 || uris[0] != "gs://b/app.jar" {
		t.Fatalf("jarFileUris = %v, want the hybrid jar", uris)
	}
}

// gkeClusterReq builds a CreateClusterRequest carrying a GKE
// virtualClusterConfig with an existing-cluster target.
func gkeClusterReq(id string) *dataprocpb.CreateClusterRequest {
	return &dataprocpb.CreateClusterRequest{
		ProjectId: "proj",
		Region:    "us-central1",
		Cluster: &dataprocpb.Cluster{
			ClusterName: id,
			VirtualClusterConfig: &dataprocpb.VirtualClusterConfig{
				StagingBucket: "dataproc-staging-proj",
				InfrastructureConfig: &dataprocpb.VirtualClusterConfig_KubernetesClusterConfig{
					KubernetesClusterConfig: &dataprocpb.KubernetesClusterConfig{
						Config: &dataprocpb.KubernetesClusterConfig_GkeClusterConfig{
							GkeClusterConfig: &dataprocpb.GkeClusterConfig{
								GkeClusterTarget: "projects/proj/locations/us-central1/clusters/gke-1",
							},
						},
					},
				},
			},
		},
	}
}

// TestCreateCluster_GKEVirtualClusterConfig verifies the gRPC transport keeps a
// virtualClusterConfig through create/get and never invents a GCE config.
func TestCreateCluster_GKEVirtualClusterConfig(t *testing.T) {
	s := newTestService()
	ctx := context.Background()
	if _, err := s.CreateCluster(ctx, gkeClusterReq("gke-1")); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	cl, err := s.GetCluster(ctx, &dataprocpb.GetClusterRequest{ProjectId: "proj", Region: "us-central1", ClusterName: "gke-1"})
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if cl.GetConfig() != nil {
		t.Fatalf("GKE cluster must not carry a GCE config: %+v", cl.GetConfig())
	}
	vcc := cl.GetVirtualClusterConfig()
	if vcc == nil {
		t.Fatal("virtualClusterConfig lost in gRPC transcode")
	}
	if vcc.GetStagingBucket() != "dataproc-staging-proj" {
		t.Fatalf("stagingBucket = %q", vcc.GetStagingBucket())
	}
	if got := vcc.GetKubernetesClusterConfig().GetGkeClusterConfig().GetGkeClusterTarget(); got != "projects/proj/locations/us-central1/clusters/gke-1" {
		t.Fatalf("gkeClusterTarget = %q", got)
	}
}

// TestGetCluster_RESTCreatedGKEVirtualClusterConfig seeds a cluster from a
// Discovery-shaped (REST) body and verifies it is served correctly over gRPC —
// the cross-transport path that shares the core store.
func TestGetCluster_RESTCreatedGKEVirtualClusterConfig(t *testing.T) {
	st := dpstore.NewMemoryStore()
	s := NewService(core.NewService(st, store.NewMemoryResourceStore()), "proj")
	ctx := context.Background()
	restBody := map[string]any{
		"virtualClusterConfig": map[string]any{
			"stagingBucket": "b",
			"kubernetesClusterConfig": map[string]any{
				"gkeClusterConfig": map[string]any{
					"gkeClusterTarget": "projects/proj/locations/us-central1/clusters/gke-1",
				},
			},
		},
	}
	if _, _, err := s.core.CreateCluster(ctx, "proj", "us-central1", "gke-1", core.ClusterInputFromMap(restBody)); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	cl, err := s.GetCluster(ctx, &dataprocpb.GetClusterRequest{ProjectId: "proj", Region: "us-central1", ClusterName: "gke-1"})
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if cl.GetConfig() != nil {
		t.Fatalf("REST-created GKE cluster surfaced a GCE config over gRPC: %+v", cl.GetConfig())
	}
	if got := cl.GetVirtualClusterConfig().GetKubernetesClusterConfig().GetGkeClusterConfig().GetGkeClusterTarget(); got != "projects/proj/locations/us-central1/clusters/gke-1" {
		t.Fatalf("gkeClusterTarget = %q", got)
	}
}

// TestCreateCluster_MalformedVirtualClusterConfig verifies the gRPC transport
// maps a virtualClusterConfig without a GKE target to InvalidArgument.
func TestCreateCluster_MalformedVirtualClusterConfig(t *testing.T) {
	s := newTestService()
	_, err := s.CreateCluster(context.Background(), &dataprocpb.CreateClusterRequest{
		ProjectId: "proj",
		Region:    "us-central1",
		Cluster: &dataprocpb.Cluster{
			ClusterName: "bad-gke",
			VirtualClusterConfig: &dataprocpb.VirtualClusterConfig{
				InfrastructureConfig: &dataprocpb.VirtualClusterConfig_KubernetesClusterConfig{
					KubernetesClusterConfig: &dataprocpb.KubernetesClusterConfig{
						Config: &dataprocpb.KubernetesClusterConfig_GkeClusterConfig{
							GkeClusterConfig: &dataprocpb.GkeClusterConfig{},
						},
					},
				},
			},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateCluster malformed vcc = %v, want InvalidArgument", err)
	}
}

// TestUpdateCluster_VirtualClusterConfigSnakeMask verifies a gRPC FieldMask's
// snake_case virtual_cluster_config path is normalized and applies the update.
func TestUpdateCluster_VirtualClusterConfigSnakeMask(t *testing.T) {
	s := newTestService()
	ctx := context.Background()
	if _, err := s.CreateCluster(ctx, gkeClusterReq("gke-1")); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if _, err := s.UpdateCluster(ctx, &dataprocpb.UpdateClusterRequest{
		ProjectId:   "proj",
		Region:      "us-central1",
		ClusterName: "gke-1",
		Cluster: &dataprocpb.Cluster{
			ClusterName: "gke-1",
			VirtualClusterConfig: &dataprocpb.VirtualClusterConfig{
				InfrastructureConfig: &dataprocpb.VirtualClusterConfig_KubernetesClusterConfig{
					KubernetesClusterConfig: &dataprocpb.KubernetesClusterConfig{
						Config: &dataprocpb.KubernetesClusterConfig_GkeClusterConfig{
							GkeClusterConfig: &dataprocpb.GkeClusterConfig{
								GkeClusterTarget: "projects/proj/locations/us-central1/clusters/gke-2",
							},
						},
					},
				},
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"virtual_cluster_config"}},
	}); err != nil {
		t.Fatalf("UpdateCluster: %v", err)
	}
	cl, err := s.GetCluster(ctx, &dataprocpb.GetClusterRequest{ProjectId: "proj", Region: "us-central1", ClusterName: "gke-1"})
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if got := cl.GetVirtualClusterConfig().GetKubernetesClusterConfig().GetGkeClusterConfig().GetGkeClusterTarget(); got != "projects/proj/locations/us-central1/clusters/gke-2" {
		t.Fatalf("gkeClusterTarget = %q, want the updated target", got)
	}
}

func TestDiagnoseCluster_Unimplemented(t *testing.T) {
	s := newTestService()
	_, err := s.DiagnoseCluster(context.Background(), &dataprocpb.DiagnoseClusterRequest{
		ProjectId: "proj", Region: "us-central1", ClusterName: "c1",
	})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("DiagnoseCluster = %v, want Unimplemented", err)
	}
}
