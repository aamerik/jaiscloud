package spark

import (
	"jaiscloud/internal/k8stypes"
	"jaiscloud/internal/model"
)

const gcpSAMountPath = "/etc/gcp"

type gcpTransform struct{}

func init() { RegisterTransform(model.CloudGCP, gcpTransform{}) }

func (gcpTransform) Cloud() model.Cloud { return model.CloudGCP }

var gcpAllowedSchemes = map[string]bool{
	"gs":    true,
	"file":  true, "local": true,
	"": true,
}

func (gcpTransform) ValidateURIs(args []string, cfg SparkConfig) error {
	return validateAgainstAllowlist(model.CloudGCP, gcpAllowedSchemes, args)
}

func (gcpTransform) ResolveCommand(job SparkJob, cfg SparkConfig) (SparkSubmitCommand, error) {
	image := resolveImage(job, cfg)
	binary := resolveContainerBinary("spark-submit")
	args, err := resolveMasterArgs(job, SparkSubmitArgs(job))
	if err != nil {
		return SparkSubmitCommand{}, err
	}
	return SparkSubmitCommand{Binary: binary, Args: args, Image: image}, nil
}

func (gcpTransform) PodEnv(cfg SparkConfig) []k8stypes.EnvVar {
	m := map[string]string{
		"GOOGLE_CLOUD_PROJECT": cfg.GCPProjectID,
	}
	if cfg.GCPServiceAccountKeyPath != "" {
		m["GOOGLE_APPLICATION_CREDENTIALS"] = cfg.GCPServiceAccountKeyPath
	} else if cfg.GCPServiceAccountSecret != "" {
		m["GOOGLE_APPLICATION_CREDENTIALS"] = gcpSAMountPath + "/key.json"
	}
	return toEnvVars(m)
}

func (gcpTransform) PodVolumes(cfg SparkConfig) ([]k8stypes.Volume, []k8stypes.VolumeMount) {
	if cfg.GCPServiceAccountSecret == "" {
		return nil, nil
	}
	vol := k8stypes.Volume{
		Name:   gcpSAVolumeName,
		Secret: &k8stypes.SecretVol{SecretName: cfg.GCPServiceAccountSecret},
	}
	mount := k8stypes.VolumeMount{
		Name:      gcpSAVolumeName,
		MountPath: gcpSAMountPath,
		ReadOnly:  true,
	}
	return []k8stypes.Volume{vol}, []k8stypes.VolumeMount{mount}
}

func (gcpTransform) SparkConfs(cfg SparkConfig) []string {
	confs := []string{
		"--conf", "spark.hadoop.google.cloud.auth.service.account.enable=true",
	}
	if cfg.GCPProjectID != "" {
		confs = append(confs, "--conf", "spark.hadoop.fs.gs.project.id="+cfg.GCPProjectID)
	}
	if cfg.GCPStorageEndpoint != "" {
		confs = append(confs, "--conf", "spark.hadoop.fs.gs.storage.root.url="+cfg.GCPStorageEndpoint)
	}
	return confs
}
