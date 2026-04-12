package spark_test

import (
	"testing"

	"github.com/jaiscloud/plugin-aws-emr-spark/internal/executor/spark"
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
