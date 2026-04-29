package spark

import (
	"strings"

	"jaiscloud/internal/k8stypes"
	"jaiscloud/internal/model"
)

// awsTransform implements CloudSparkTransform for AWS EMR / EMR-on-EKS.
type awsTransform struct{}

func init() { RegisterTransform(model.CloudAWS, awsTransform{}) }

func (awsTransform) Cloud() model.Cloud { return model.CloudAWS }

func (awsTransform) Rewrite(uri string, _ SparkConfig) string { return uri } // s3a:// is native on AWS

func (t awsTransform) ResolveCommand(job SparkJob, cfg SparkConfig) SparkSubmitCommand {
	return AWSResolveSparkCommand(job, cfg)
}

func (awsTransform) PodEnv(cfg SparkConfig) []k8stypes.EnvVar {
	if cfg.S3Endpoint == "" {
		return nil
	}
	return toEnvVars(map[string]string{
		"AWS_ENDPOINT_URL":          cfg.S3Endpoint,
		"AWS_REGION":                cfg.Region,
		"AWS_ACCESS_KEY_ID":         cfg.AWSAccessKey,
		"AWS_SECRET_ACCESS_KEY":     cfg.AWSSecretKey,
		"AWS_S3_FORCE_PATH_STYLE":   "true",
	})
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

// AWSResolveSparkCommand resolves a SparkJob from an AWS EMR/EMR-Containers
// API call into a container command + args for local execution.
//
// It handles three AWS-specific patterns:
//   - Pattern 1: EMR Containers StartJobRun (JarURI = absolute path entry point)
//   - Pattern 2: EMR AddJobFlowSteps with command-runner.jar
//   - Pattern 3: EMR AddJobFlowSteps with a real JAR
//
// Then applies devbox transformations when S3Endpoint is configured:
//   - Rewrites --master (yarn/k8s → local[*]) for container execution
//   - Strips incompatible Spark confs (extraListeners, sql.extensions)
//   - Injects S3/EMRFS endpoint and credential configuration
func AWSResolveSparkCommand(job SparkJob, cfg SparkConfig) SparkSubmitCommand {
	image := resolveImage(job, cfg)
	binary, args := resolveAWSPattern(job, cfg, image)

	if cfg.S3Endpoint != "" {
		args = stripSparkConfs(args,
			"spark.extraListeners",
			"spark.sql.extensions",
		)
		args = insertBeforeJar(args, awsDevboxSparkConfs(cfg, image))
	}

	return SparkSubmitCommand{Binary: binary, Args: args, Image: image}
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
		return entryPoint, rewriteSparkMaster(job.Args)

	case job.JarURI == "command-runner.jar" && len(job.Args) > 0:
		// Pattern 2: command-runner.jar (EMR classic / AddJobFlowSteps)
		resolved := resolveContainerBinary(job.Args[0])
		if useAlt && resolved == defaultBinary {
			resolved = altBinary
		}
		return resolved, rewriteSparkMaster(job.Args[1:])

	default:
		// Pattern 3: real JAR — build full spark-submit invocation
		return binary, rewriteSparkMaster(SparkSubmitArgs(job))
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
