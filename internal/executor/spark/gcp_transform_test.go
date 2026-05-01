package spark

import (
	"strings"
	"testing"
)

func gcpCfgWithSecret() SparkConfig {
	cfg := SparkConfigFrom("k8s", SizeSmall)
	cfg.GCPProjectID = "my-project"
	cfg.GCPServiceAccountSecret = "gcp-sa-key-secret"
	return cfg
}

func gcpCfgWithKeyPath() SparkConfig {
	cfg := SparkConfigFrom("k8s", SizeSmall)
	cfg.GCPProjectID = "my-project"
	cfg.GCPServiceAccountKeyPath = "/etc/gcp/key.json"
	return cfg
}

func TestGCPTransform_PodEnv_WithKeyPath(t *testing.T) {
	envs := gcpTransform{}.PodEnv(gcpCfgWithKeyPath())
	names := make(map[string]string, len(envs))
	for _, e := range envs {
		names[e.Name] = e.Value
	}
	if names["GOOGLE_CLOUD_PROJECT"] != "my-project" {
		t.Errorf("GOOGLE_CLOUD_PROJECT: got %q", names["GOOGLE_CLOUD_PROJECT"])
	}
	if names["GOOGLE_APPLICATION_CREDENTIALS"] != "/etc/gcp/key.json" {
		t.Errorf("GOOGLE_APPLICATION_CREDENTIALS: got %q", names["GOOGLE_APPLICATION_CREDENTIALS"])
	}
}

func TestGCPTransform_PodEnv_WithSecret(t *testing.T) {
	envs := gcpTransform{}.PodEnv(gcpCfgWithSecret())
	names := make(map[string]string, len(envs))
	for _, e := range envs {
		names[e.Name] = e.Value
	}
	wantCreds := gcpSAMountPath + "/key.json"
	if names["GOOGLE_APPLICATION_CREDENTIALS"] != wantCreds {
		t.Errorf("GOOGLE_APPLICATION_CREDENTIALS: got %q, want %q", names["GOOGLE_APPLICATION_CREDENTIALS"], wantCreds)
	}
}

func TestGCPTransform_PodVolumes_WithSecret(t *testing.T) {
	vols, mounts := gcpTransform{}.PodVolumes(gcpCfgWithSecret())
	if len(vols) != 1 {
		t.Fatalf("expected 1 volume; got %d", len(vols))
	}
	if vols[0].Name != gcpSAVolumeName {
		t.Errorf("volume name: got %q, want %q", vols[0].Name, gcpSAVolumeName)
	}
	if vols[0].Secret == nil || vols[0].Secret.SecretName != "gcp-sa-key-secret" {
		t.Errorf("volume should be a secret named gcp-sa-key-secret; got %+v", vols[0])
	}
	if len(mounts) != 1 || mounts[0].MountPath != gcpSAMountPath {
		t.Errorf("mount path: got %+v", mounts)
	}
	if !mounts[0].ReadOnly {
		t.Error("SA key mount should be read-only")
	}
}

func TestGCPTransform_PodVolumes_NoSecret(t *testing.T) {
	cfg := gcpCfgWithKeyPath() // key path set, no secret
	vols, mounts := gcpTransform{}.PodVolumes(cfg)
	if len(vols) != 0 || len(mounts) != 0 {
		t.Errorf("no secret should produce no volumes/mounts; got %d vols, %d mounts", len(vols), len(mounts))
	}
}

func TestGCPTransform_SparkConfs_AlwaysIncludesAuth(t *testing.T) {
	cfg := gcpCfgWithSecret()
	confs := gcpTransform{}.SparkConfs(cfg)
	confsStr := strings.Join(confs, " ")
	if !strings.Contains(confsStr, "google.cloud.auth.service.account.enable=true") {
		t.Error("GCP confs should always include service account auth enable")
	}
	if !strings.Contains(confsStr, "fs.gs.project.id=my-project") {
		t.Error("GCP confs should include project ID")
	}
	if len(confs)%2 != 0 {
		t.Errorf("confs must have even length; got %d: %v", len(confs), confs)
	}
	for i := 0; i < len(confs); i += 2 {
		if confs[i] != "--conf" {
			t.Errorf("confs[%d] should be --conf, got %q", i, confs[i])
		}
	}
}

func TestGCPTransform_SparkConfs_StorageEndpoint(t *testing.T) {
	cfg := gcpCfgWithSecret()
	cfg.GCPStorageEndpoint = "https://storage.googleapis.com"
	confs := gcpTransform{}.SparkConfs(cfg)
	confsStr := strings.Join(confs, " ")
	if !strings.Contains(confsStr, "fs.gs.storage.root.url=https://storage.googleapis.com") {
		t.Error("GCP confs should include custom storage endpoint")
	}
}

func TestGCPTransform_ValidateURIs_GSAllowed(t *testing.T) {
	args := []string{"gs://my-bucket/data/file.parquet"}
	tr := gcpTransform{}
	if err := tr.ValidateURIs(args, SparkConfig{}); err != nil {
		t.Errorf("gs:// should be allowed on GCP, got: %v", err)
	}
}

func TestGCPTransform_ValidateURIs_S3aRejected(t *testing.T) {
	args := []string{"s3a://my-bucket/data/file.parquet"}
	tr := gcpTransform{}
	if err := tr.ValidateURIs(args, SparkConfig{}); err == nil {
		t.Error("s3a:// should be rejected on GCP")
	}
}
