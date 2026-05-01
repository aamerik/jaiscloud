package spark

import (
	"fmt"
	"strings"
)

// BuildSparkJob constructs a SparkJob, always forwarding cfg so that
// image/namespace/SA are never the zero value.
// sparkParams (may be empty) is the raw sparkSubmitParameters string from the
// EMR on EKS API; its --conf key=value pairs are merged into SparkConf so
// per-job overrides (e.g. a custom container image) take effect.
func BuildSparkJob(jobID, jar, mainClass string, args []string, sparkParams string, cfg SparkConfig) SparkJob {
	job := SparkJob{
		JobID:     jobID,
		JarURI:    jar,
		MainClass: mainClass,
		Args:      args,
		Config:    cfg,
	}
	if sparkParams != "" {
		job.SparkConf = map[string]string{}
		tokens := strings.Fields(sparkParams)
		for i := 0; i+1 < len(tokens); i++ {
			switch tokens[i] {
			case "--conf":
				if k, v, ok := strings.Cut(tokens[i+1], "="); ok {
					job.SparkConf[strings.TrimSpace(k)] = strings.TrimSpace(v)
				}
				i++
			case "--class":
				job.MainClass = tokens[i+1]
				i++
			}
		}
	}
	return job
}

// SparkSubmitArgs builds the spark-submit argument list for a SparkJob.
// The caller is responsible for prepending the spark-submit binary path.
func SparkSubmitArgs(job SparkJob) []string {
	args := []string{}

	if job.Config.SparkMode == "k8s" {
		args = append(args, "--master", "k8s://"+job.Config.APIServer)
		args = append(args,
			"--deploy-mode", "cluster",
			"--conf", fmt.Sprintf("spark.kubernetes.container.image=%s", job.Config.Image),
			"--conf", fmt.Sprintf("spark.kubernetes.namespace=%s", job.Config.Namespace),
		)
		if job.Config.ServiceAccount != "" {
			args = append(args, "--conf",
				fmt.Sprintf("spark.kubernetes.authenticate.driver.serviceAccountName=%s", job.Config.ServiceAccount))
		}
		p := job.Config.Resources
		args = append(args,
			"--conf", fmt.Sprintf("spark.driver.cores=%s", p.DriverCPU),
			"--conf", fmt.Sprintf("spark.driver.memory=%s", p.DriverMemory),
			"--conf", fmt.Sprintf("spark.executor.cores=%s", p.ExecutorCPU),
			"--conf", fmt.Sprintf("spark.executor.memory=%s", p.ExecutorMemory),
			"--conf", fmt.Sprintf("spark.executor.instances=%d", p.ExecutorCount),
		)
		if job.Config.S3LogURI != "" {
			args = append(args,
				"--conf", fmt.Sprintf("spark.eventLog.enabled=true"),
				"--conf", fmt.Sprintf("spark.eventLog.dir=%s", job.Config.S3LogURI),
			)
		}
	}

	// User-supplied --conf overrides
	for k, v := range job.SparkConf {
		args = append(args, "--conf", fmt.Sprintf("%s=%s", k, v))
	}

	if job.MainClass != "" {
		args = append(args, "--class", job.MainClass)
	}

	args = append(args, job.JarURI)
	args = append(args, job.Args...)

	return args
}
