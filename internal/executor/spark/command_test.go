package spark_test

import (
	"testing"

	"jaiscloud/internal/executor/spark"
)

func TestSparkSubmitArgs_BasicJob(t *testing.T) {
	job := spark.SparkJob{
		JobID:     "j1",
		JarURI:    "s3://bucket/app.jar",
		MainClass: "com.example.Main",
		Args:      []string{"arg1", "arg2"},
	}
	args := spark.SparkSubmitArgs(job)

	// Last element should be the jar; main class should be present
	if args[len(args)-3] != job.JarURI {
		t.Errorf("jar URI not at expected position, args=%v", args)
	}
	found := false
	for i, a := range args {
		if a == "--class" && i+1 < len(args) && args[i+1] == job.MainClass {
			found = true
		}
	}
	if !found {
		t.Errorf("--class %s not found in args: %v", job.MainClass, args)
	}
}

func TestSparkSubmitArgs_K8sMode_HasMaster(t *testing.T) {
	cfg := spark.SparkConfigFrom("k8s", spark.SizeSmall)
	job := spark.SparkJob{
		JobID:  "j1",
		JarURI: "local:///opt/spark/jars/app.jar",
		Config: cfg,
	}
	args := spark.SparkSubmitArgs(job)

	hasMaster := false
	for i, a := range args {
		if a == "--master" && i+1 < len(args) {
			hasMaster = true
		}
	}
	if !hasMaster {
		t.Errorf("k8s mode must include --master, args=%v", args)
	}
}

func TestSparkSubmitArgs_K8sMode_UsesConfigImage(t *testing.T) {
	cfg := spark.SparkConfigFrom("k8s", spark.SizeSmall, func(c *spark.SparkConfig) {
		c.Image = "my-org/spark:3.0"
	})
	job := spark.BuildSparkJob("j1", "s3://bucket/app.jar", "", nil, "", cfg)
	args := spark.SparkSubmitArgs(job)

	found := false
	for i, a := range args {
		if a == "--conf" && i+1 < len(args) && args[i+1] == "spark.kubernetes.container.image=my-org/spark:3.0" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected spark.kubernetes.container.image=my-org/spark:3.0 in args: %v", args)
	}
}

func TestBuildSparkJob_ForwardsConfig(t *testing.T) {
	cfg := spark.SparkConfigFrom("k8s", spark.SizeSmall, func(c *spark.SparkConfig) {
		c.Image = "custom:1.0"
		c.Namespace = "spark-ns"
	})
	job := spark.BuildSparkJob("j1", "app.jar", "com.Main", []string{"a"}, "", cfg)
	if job.Config.Image != "custom:1.0" {
		t.Errorf("Config.Image not forwarded, got %q", job.Config.Image)
	}
	if job.Config.Namespace != "spark-ns" {
		t.Errorf("Config.Namespace not forwarded, got %q", job.Config.Namespace)
	}
}

func TestBuildSparkJob_SparkSubmitParameters(t *testing.T) {
	cfg := spark.SparkConfigFrom("k8s", spark.SizeSmall)
	params := "--conf spark.kubernetes.container.image=override:2.0 --conf spark.executor.instances=4"
	job := spark.BuildSparkJob("j1", "app.jar", "", nil, params, cfg)

	if job.SparkConf["spark.kubernetes.container.image"] != "override:2.0" {
		t.Errorf("expected image override in SparkConf, got %v", job.SparkConf)
	}
	if job.SparkConf["spark.executor.instances"] != "4" {
		t.Errorf("expected executor instances in SparkConf, got %v", job.SparkConf)
	}
}

func TestBuildSparkJob_EmptySparkParams_NoSparkConf(t *testing.T) {
	cfg := spark.SparkConfigFrom("k8s", spark.SizeSmall)
	job := spark.BuildSparkJob("j1", "app.jar", "", nil, "", cfg)
	if job.SparkConf != nil {
		t.Errorf("expected nil SparkConf when sparkParams is empty, got %v", job.SparkConf)
	}
}

func TestSparkSubmitArgs_SparkConf(t *testing.T) {
	job := spark.SparkJob{
		JobID:     "j1",
		JarURI:    "s3://bucket/app.jar",
		SparkConf: map[string]string{"spark.sql.shuffle.partitions": "10"},
	}
	args := spark.SparkSubmitArgs(job)

	found := false
	for i, a := range args {
		if a == "--conf" && i+1 < len(args) && args[i+1] == "spark.sql.shuffle.partitions=10" {
			found = true
		}
	}
	if !found {
		t.Errorf("--conf spark.sql.shuffle.partitions=10 not found in args: %v", args)
	}
}
