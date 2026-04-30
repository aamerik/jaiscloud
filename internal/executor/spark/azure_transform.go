package spark

import (
	"context"
	"fmt"

	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/k8stypes"
	"jaiscloud/internal/model"
)

type azureTransform struct{}

func init() { RegisterTransform(model.CloudAzure, azureTransform{}) }

func (azureTransform) Cloud() model.Cloud { return model.CloudAzure }

func (azureTransform) Rewrite(uri string, cfg SparkConfig) string {
	return rewriteS3aToABFS(uri, cfg.AzureStorageAccount)
}

func (azureTransform) ResolveCommand(job SparkJob, cfg SparkConfig) (SparkSubmitCommand, error) {
	image := resolveImage(job, cfg)
	binary := resolveContainerBinary("spark-submit")
	args, err := resolveMasterArgs(job, SparkSubmitArgs(job))
	if err != nil {
		return SparkSubmitCommand{}, err
	}
	return SparkSubmitCommand{Binary: binary, Args: args, Image: image}, nil
}

func (azureTransform) UploadTemplate(_ context.Context, _ blobfs.BlobStore, _ SparkConfig, _ string, _ []byte) (string, string, error) {
	return "", "", fmt.Errorf("cluster mode on Azure is not yet supported — upload via abfss:// not implemented")
}

func (azureTransform) DeleteTemplate(_ context.Context, _ blobfs.BlobStore, _ string) error { return nil }

func (azureTransform) DriverFetchEnv(_ SparkConfig) []envVar { return nil }

func (azureTransform) PodEnv(cfg SparkConfig) []k8stypes.EnvVar {
	m := map[string]string{
		"AZURE_STORAGE_ACCOUNT": cfg.AzureStorageAccount,
	}
	if cfg.AzureStorageKey != "" {
		// Shared key auth
		m["AZURE_STORAGE_KEY"] = cfg.AzureStorageKey
	} else {
		// OAuth / Workload Identity
		m["AZURE_CLIENT_ID"] = cfg.AzureClientID
		m["AZURE_CLIENT_SECRET"] = cfg.AzureClientSecret
		m["AZURE_TENANT_ID"] = cfg.AzureTenantID
	}
	return toEnvVars(m)
}

func (azureTransform) PodVolumes(cfg SparkConfig) ([]k8stypes.Volume, []k8stypes.VolumeMount) {
	// Workload Identity projected volume only in OAuth mode (no storage key).
	if cfg.AzureStorageKey != "" {
		return nil, nil
	}
	expirySeconds := int64(3600)
	vol := k8stypes.Volume{
		Name: azureIdentityVolumeName,
		Projected: &k8stypes.ProjectedVol{
			Sources: []k8stypes.ProjectionElement{
				{ServiceAccountToken: &k8stypes.SATokenProjection{
					Audience:          "api://AzureADTokenExchange",
					ExpirationSeconds: expirySeconds,
					Path:              "token",
				}},
				{DownwardAPI: &k8stypes.DownwardAPIProjection{
					Items: []k8stypes.DownwardAPIItem{
						{Path: "labels", FieldRef: &k8stypes.ObjectFieldSelector{FieldPath: "metadata.labels"}},
						{Path: "namespace", FieldRef: &k8stypes.ObjectFieldSelector{FieldPath: "metadata.namespace"}},
					},
				}},
			},
		},
	}
	mount := k8stypes.VolumeMount{
		Name:      azureIdentityVolumeName,
		MountPath: "/var/run/secrets/azure/identity",
	}
	return []k8stypes.Volume{vol}, []k8stypes.VolumeMount{mount}
}

func (azureTransform) SparkConfs(cfg SparkConfig) []string {
	acct := cfg.AzureStorageAccount
	if acct == "" {
		return nil
	}
	var confs []string
	if cfg.AzureStorageKey != "" {
		confs = append(confs,
			"--conf", fmt.Sprintf("fs.azure.account.auth.type.%s.dfs.core.windows.net=SharedKey", acct),
			"--conf", fmt.Sprintf("fs.azure.account.key.%s.dfs.core.windows.net=%s", acct, cfg.AzureStorageKey),
		)
	} else {
		// OAuth / Workload Identity
		confs = append(confs,
			"--conf", fmt.Sprintf("fs.azure.account.auth.type.%s.dfs.core.windows.net=OAuth", acct),
			"--conf", fmt.Sprintf("fs.azure.account.oauth.provider.type.%s.dfs.core.windows.net=org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider", acct),
			"--conf", fmt.Sprintf("fs.azure.account.oauth2.client.id.%s.dfs.core.windows.net=%s", acct, cfg.AzureClientID),
			"--conf", fmt.Sprintf("fs.azure.account.oauth2.client.secret.%s.dfs.core.windows.net=%s", acct, cfg.AzureClientSecret),
			"--conf", fmt.Sprintf("fs.azure.account.oauth2.client.endpoint.%s.dfs.core.windows.net=https://login.microsoftonline.com/%s/oauth2/token", acct, cfg.AzureTenantID),
		)
	}
	if cfg.AzureStorageEndpoint != "" {
		confs = append(confs, "--conf", "fs.azure.endpoint="+cfg.AzureStorageEndpoint)
	}
	return confs
}
