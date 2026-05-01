package spark

import (
	"strings"
	"testing"
)

func azureCfgSharedKey() SparkConfig {
	cfg := SparkConfigFrom("k8s", SizeSmall)
	cfg.AzureStorageAccount = "mystorageacct"
	cfg.AzureStorageKey = "base64key=="
	return cfg
}

func azureCfgOAuth() SparkConfig {
	cfg := SparkConfigFrom("k8s", SizeSmall)
	cfg.AzureStorageAccount = "mystorageacct"
	cfg.AzureClientID = "client-id"
	cfg.AzureClientSecret = "client-secret"
	cfg.AzureTenantID = "tenant-id"
	return cfg
}

func TestAzureTransform_PodEnv_SharedKey(t *testing.T) {
	envs := azureTransform{}.PodEnv(azureCfgSharedKey())
	names := make(map[string]string, len(envs))
	for _, e := range envs {
		names[e.Name] = e.Value
	}
	if names["AZURE_STORAGE_ACCOUNT"] != "mystorageacct" {
		t.Errorf("AZURE_STORAGE_ACCOUNT: got %q", names["AZURE_STORAGE_ACCOUNT"])
	}
	if names["AZURE_STORAGE_KEY"] != "base64key==" {
		t.Errorf("AZURE_STORAGE_KEY: got %q", names["AZURE_STORAGE_KEY"])
	}
	if _, ok := names["AZURE_CLIENT_ID"]; ok {
		t.Error("AZURE_CLIENT_ID should not be set in SharedKey mode")
	}
}

func TestAzureTransform_PodEnv_OAuth(t *testing.T) {
	envs := azureTransform{}.PodEnv(azureCfgOAuth())
	names := make(map[string]string, len(envs))
	for _, e := range envs {
		names[e.Name] = e.Value
	}
	if names["AZURE_CLIENT_ID"] != "client-id" {
		t.Errorf("AZURE_CLIENT_ID: got %q", names["AZURE_CLIENT_ID"])
	}
	if names["AZURE_TENANT_ID"] != "tenant-id" {
		t.Errorf("AZURE_TENANT_ID: got %q", names["AZURE_TENANT_ID"])
	}
	if _, ok := names["AZURE_STORAGE_KEY"]; ok {
		t.Error("AZURE_STORAGE_KEY should not be set in OAuth mode")
	}
}

func TestAzureTransform_PodVolumes_SharedKey_NoVolumes(t *testing.T) {
	vols, mounts := azureTransform{}.PodVolumes(azureCfgSharedKey())
	if len(vols) != 0 || len(mounts) != 0 {
		t.Errorf("SharedKey mode should produce no volumes/mounts; got %d vols, %d mounts", len(vols), len(mounts))
	}
}

func TestAzureTransform_PodVolumes_OAuth_ProjectedVolume(t *testing.T) {
	vols, mounts := azureTransform{}.PodVolumes(azureCfgOAuth())
	if len(vols) != 1 {
		t.Fatalf("OAuth mode should produce 1 volume; got %d", len(vols))
	}
	if vols[0].Name != azureIdentityVolumeName {
		t.Errorf("volume name: got %q, want %q", vols[0].Name, azureIdentityVolumeName)
	}
	if vols[0].Projected == nil {
		t.Error("volume should be projected")
	}
	if len(mounts) != 1 {
		t.Fatalf("OAuth mode should produce 1 mount; got %d", len(mounts))
	}
	if mounts[0].MountPath != "/var/run/secrets/azure/identity" {
		t.Errorf("mount path: got %q", mounts[0].MountPath)
	}
}

func TestAzureTransform_SparkConfs_SharedKey(t *testing.T) {
	confs := azureTransform{}.SparkConfs(azureCfgSharedKey())
	confsStr := strings.Join(confs, " ")
	if !strings.Contains(confsStr, "SharedKey") {
		t.Error("SharedKey confs should contain auth.type=SharedKey")
	}
	if !strings.Contains(confsStr, "account.key.mystorageacct") {
		t.Error("SharedKey confs should contain the account key conf")
	}
	if strings.Contains(confsStr, "OAuth") {
		t.Error("SharedKey confs must not contain OAuth")
	}
	// Verify proper --conf pairing (even number of elements)
	if len(confs)%2 != 0 {
		t.Errorf("confs must have even length (--conf KEY=VAL pairs); got %d", len(confs))
	}
	for i := 0; i < len(confs); i += 2 {
		if confs[i] != "--conf" {
			t.Errorf("confs[%d] should be --conf, got %q", i, confs[i])
		}
	}
}

func TestAzureTransform_SparkConfs_OAuth(t *testing.T) {
	confs := azureTransform{}.SparkConfs(azureCfgOAuth())
	confsStr := strings.Join(confs, " ")
	if !strings.Contains(confsStr, "OAuth") {
		t.Error("OAuth confs should contain auth.type=OAuth")
	}
	if strings.Contains(confsStr, "SharedKey") {
		t.Error("OAuth confs must not contain SharedKey")
	}
	if !strings.Contains(confsStr, "client-id") {
		t.Error("OAuth confs should contain client ID")
	}
	if !strings.Contains(confsStr, "tenant-id") {
		t.Error("OAuth confs should contain tenant endpoint")
	}
	if len(confs)%2 != 0 {
		t.Errorf("confs must have even length; got %d", len(confs))
	}
}

func TestAzureTransform_SparkConfs_Empty_NoAccount(t *testing.T) {
	cfg := SparkConfigFrom("k8s", SizeSmall)
	confs := azureTransform{}.SparkConfs(cfg)
	if len(confs) != 0 {
		t.Errorf("no storage account should produce empty confs; got %v", confs)
	}
}

func TestAzureTransform_ValidateURIs_ABFSSAllowed(t *testing.T) {
	tr := azureTransform{}
	cfg := azureCfgSharedKey()
	args := []string{"abfss://container@mystorageacct.dfs.core.windows.net/app.jar"}
	if err := tr.ValidateURIs(args, cfg); err != nil {
		t.Errorf("abfss:// should be allowed on Azure, got: %v", err)
	}
}

func TestAzureTransform_ValidateURIs_S3Rejected(t *testing.T) {
	tr := azureTransform{}
	cfg := azureCfgSharedKey()
	args := []string{"s3a://bucket/app.jar"}
	if err := tr.ValidateURIs(args, cfg); err == nil {
		t.Error("s3a:// should be rejected on Azure")
	}
}

func TestAzureTransform_SparkConfs_StorageEndpoint(t *testing.T) {
	cfg := azureCfgSharedKey()
	cfg.AzureStorageEndpoint = "https://custom-endpoint.azure.com"
	confs := azureTransform{}.SparkConfs(cfg)
	confsStr := strings.Join(confs, " ")
	if !strings.Contains(confsStr, "fs.azure.endpoint=https://custom-endpoint.azure.com") {
		t.Error("custom storage endpoint should appear in confs")
	}
}
