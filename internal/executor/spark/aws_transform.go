package spark

import (
	"context"
	"fmt"
	"strings"

	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/k8stypes"
	"jaiscloud/internal/model"
)

// awsTransform implements CloudSparkTransform for AWS EMR / EMR-on-EKS.
type awsTransform struct{}

func init() { RegisterTransform(model.CloudAWS, awsTransform{}) }

func (awsTransform) Cloud() model.Cloud { return model.CloudAWS }

func (awsTransform) Rewrite(uri string, _ SparkConfig) string { return uri } // s3a:// is native on AWS

func (t awsTransform) ResolveCommand(job SparkJob, cfg SparkConfig) (SparkSubmitCommand, error) {
	return AWSResolveSparkCommand(job, cfg)
}

func (awsTransform) UploadTemplate(
	ctx context.Context, blobs blobfs.BlobStore, cfg SparkConfig, jobID string, body []byte,
) (string, string, error) {
	if blobs == nil {
		return "", "", fmt.Errorf("cluster mode: no blob store wired — pass blobfs.BlobStore to NewK8sExecutor")
	}
	bucket := cfg.TemplateBucket
	if bucket == "" {
		bucket = "jaiscloud-spark-templates"
	}
	key := fmt.Sprintf("pod-templates/%s-executor.yaml", sanitizeLabel(jobID))
	if err := blobs.Put(ctx, bucket, key, body); err != nil {
		return "", "", fmt.Errorf("put executor template into %s/%s: %w", bucket, key, err)
	}
	return fmt.Sprintf("s3://%s/%s", bucket, key), bucket + "/" + key, nil
}

func (awsTransform) DeleteTemplate(ctx context.Context, blobs blobfs.BlobStore, cleanupKey string) error {
	if blobs == nil {
		return nil
	}
	parts := strings.SplitN(cleanupKey, "/", 2)
	if len(parts) != 2 {
		return fmt.Errorf("cluster mode: invalid cleanup key %q", cleanupKey)
	}
	return blobs.Delete(ctx, parts[0], parts[1])
}

func (awsTransform) DriverFetchEnv(cfg SparkConfig) []envVar {
	return []envVar{
		{Name: "AWS_ENDPOINT_URL", Value: cfg.S3Endpoint},
		{Name: "AWS_ACCESS_KEY_ID", Value: cfg.AWSAccessKey},
		{Name: "AWS_SECRET_ACCESS_KEY", Value: cfg.AWSSecretKey},
		{Name: "AWS_REGION", Value: cfg.Region},
		{Name: "AWS_S3_FORCE_PATH_STYLE", Value: "true"},
	}
}

func (awsTransform) PodEnv(cfg SparkConfig) []k8stypes.EnvVar {
	if cfg.S3Endpoint == "" {
		return nil
	}
	return []k8stypes.EnvVar{
		{Name: "AWS_ENDPOINT_URL", Value: cfg.S3Endpoint},
		{Name: "AWS_REGION", Value: cfg.Region},
		{Name: "AWS_ACCESS_KEY_ID", Value: cfg.AWSAccessKey},
		{Name: "AWS_SECRET_ACCESS_KEY", Value: cfg.AWSSecretKey},
		{Name: "AWS_S3_FORCE_PATH_STYLE", Value: "true"},
	}
}

func (awsTransform) PodVolumes(cfg SparkConfig) ([]k8stypes.Volume, []k8stypes.VolumeMount) {
	if cfg.S3Endpoint == "" {
		return nil, nil
	}
	vols := []k8stypes.Volume{{
		Name:      "jaiscloud-aws-credentials",
		ConfigMap: &k8stypes.ConfigMapVol{Name: "jaiscloud-aws-credentials"},
	}}
	mounts := []k8stypes.VolumeMount{{
		Name:      "jaiscloud-aws-credentials",
		MountPath: "/etc/aws",
		ReadOnly:  true,
	}}
	return vols, mounts
}

func (awsTransform) SparkConfs(cfg SparkConfig) []string {
	if cfg.S3Endpoint == "" {
		return nil
	}
	image := cfg.Image
	if image == "" {
		image = DefaultImage
	}
	return awsDevboxSparkConfs(cfg, image)
}

// SparkSubmitCommand holds the resolved container command and arguments
// after applying all transformations for local execution.
type SparkSubmitCommand struct {
	Binary string   // absolute path to spark-submit binary
	Args   []string // transformed spark-submit arguments
	Image  string   // resolved container image
}

// AWSResolveSparkCommand resolves a SparkJob into a container command + args.
// Handles three EMR submission patterns; applies devbox S3 conf when S3Endpoint
// is configured. Returns an error when cluster mode is active and the --master
// value is incompatible with Spark K8s cluster mode (see resolveMasterArgs).
func AWSResolveSparkCommand(job SparkJob, cfg SparkConfig) (SparkSubmitCommand, error) {
	image := resolveImage(job, cfg)
	binary, rawArgs := resolveAWSPattern(job, cfg, image)

	args, err := resolveMasterArgs(job, rawArgs)
	if err != nil {
		return SparkSubmitCommand{}, err
	}

	if cfg.S3Endpoint != "" {
		args = stripSparkConfs(args,
			"spark.extraListeners",
			"spark.sql.extensions",
		)
		args = insertBeforeJar(args, awsDevboxSparkConfs(cfg, image))
	}

	return SparkSubmitCommand{Binary: binary, Args: args, Image: image}, nil
}

// resolveAWSPattern detects which EMR submission pattern was used and
// returns the spark-submit binary path and rewritten args.
func resolveAWSPattern(job SparkJob, cfg SparkConfig, image string) (string, []string) {
	defaultBinary := resolveContainerBinary("spark-submit")
	altBinary := cfg.SparkSubmitPath
	useAlt := altBinary != "" && strings.Contains(strings.ToLower(image), "emr")

	binary := defaultBinary
	if useAlt {
		binary = altBinary
	}

	switch {
	case strings.HasPrefix(job.JarURI, "/") && len(job.Args) > 0:
		// Pattern 1: absolute path entry point (EMR Containers / StartJobRun)
		entryPoint := job.JarURI
		if useAlt && entryPoint == defaultBinary {
			entryPoint = altBinary
		}
		return entryPoint, job.Args

	case job.JarURI == "command-runner.jar" && len(job.Args) > 0:
		// Pattern 2: command-runner.jar (EMR classic / AddJobFlowSteps)
		resolved := resolveContainerBinary(job.Args[0])
		if useAlt && resolved == defaultBinary {
			resolved = altBinary
		}
		return resolved, job.Args[1:]

	default:
		// Pattern 3: real JAR — build full spark-submit invocation
		return binary, SparkSubmitArgs(job)
	}
}

// resolveImage picks the Spark container image from job-level override,
// executor default, or hardcoded fallback.
func resolveImage(job SparkJob, cfg SparkConfig) string {
	if v, ok := job.SparkConf["spark.kubernetes.container.image"]; ok && v != "" {
		return v
	}
	if cfg.Image != "" {
		return cfg.Image
	}
	return DefaultImage
}

// awsDevboxSparkConfs returns the --conf flags injected in devbox mode for
// AWS EMR jobs: S3/EMRFS endpoint, credentials, image pull policy, container image.
func awsDevboxSparkConfs(cfg SparkConfig, image string) []string {
	return []string{
		"--conf", "spark.hadoop.fs.s3.endpoint=" + cfg.S3Endpoint,
		"--conf", "spark.hadoop.fs.s3a.endpoint=" + cfg.S3Endpoint,
		"--conf", "spark.hadoop.fs.s3a.path.style.access=true",
		"--conf", "spark.hadoop.fs.s3.path.style.access=true",
		"--conf", "spark.hadoop.fs.s3.awsAccessKeyId=" + cfg.AWSAccessKey,
		"--conf", "spark.hadoop.fs.s3.awsSecretAccessKey=" + cfg.AWSSecretKey,
		"--conf", "spark.hadoop.fs.s3a.access.key=" + cfg.AWSAccessKey,
		"--conf", "spark.hadoop.fs.s3a.secret.key=" + cfg.AWSSecretKey,
		"--conf", "spark.hadoop.fs.s3.customAWSCredentialsProvider=",
		"--conf", "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
		"--conf", "spark.kubernetes.container.image.pullPolicy=Never",
		"--conf", "spark.kubernetes.container.image=" + image,
	}
}
