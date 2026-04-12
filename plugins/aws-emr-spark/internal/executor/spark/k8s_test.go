package spark_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jaiscloud/plugin-aws-emr-spark/internal/executor/spark"
)

func TestK8sExecutor_Submit_UsesK8sMaster(t *testing.T) {
	cfg := spark.SparkConfigFrom("k8s", spark.SizeMedium, func(c *spark.SparkConfig) {
		c.Namespace = "spark-jobs"
		c.ServiceAccount = "spark-sa"
		c.Image = "my-registry/spark:3.5"
	})
	ex := spark.NewK8sExecutor(cfg)
	defer ex.Close()

	job := spark.SparkJob{
		JobID:     "k8s-job-1",
		JarURI:    "local:///opt/spark/app.jar",
		MainClass: "com.example.App",
		Config:    cfg,
	}
	if err := ex.Submit(context.Background(), job); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// Verify the spark-submit args include K8s-specific flags
	args := spark.SparkSubmitArgs(job)
	joined := strings.Join(args, " ")

	checks := []struct {
		needle string
		desc   string
	}{
		{"--master", "k8s master flag"},
		{"k8s://", "k8s master scheme"},
		{"--deploy-mode cluster", "cluster deploy mode"},
		{"spark.kubernetes.container.image=my-registry/spark:3.5", "custom image"},
		{"spark.kubernetes.namespace=spark-jobs", "namespace"},
		{"spark.kubernetes.authenticate.driver.serviceAccountName=spark-sa", "service account"},
		{"spark.executor.instances=2", "medium executor count"},
		{"--class com.example.App", "main class"},
		{"local:///opt/spark/app.jar", "jar URI"},
	}
	for _, c := range checks {
		if !strings.Contains(joined, c.needle) {
			t.Errorf("missing %s: %q not found in args:\n%s", c.desc, c.needle, joined)
		}
	}
}

func TestK8sExecutor_Submit_S3LogURI(t *testing.T) {
	cfg := spark.SparkConfigFrom("k8s", spark.SizeSmall, func(c *spark.SparkConfig) {
		c.S3LogURI = "s3://my-bucket/spark-logs"
	})
	job := spark.SparkJob{JobID: "j1", JarURI: "s3://b/app.jar", Config: cfg}
	args := spark.SparkSubmitArgs(job)
	joined := strings.Join(args, " ")

	if !strings.Contains(joined, "spark.eventLog.enabled=true") {
		t.Error("S3LogURI should enable eventLog")
	}
	if !strings.Contains(joined, "spark.eventLog.dir=s3://my-bucket/spark-logs") {
		t.Errorf("S3LogURI not found in args: %s", joined)
	}
}

func TestK8sExecutor_Status_DelegatestoMock(t *testing.T) {
	cfg := spark.SparkConfigFrom("k8s", spark.SizeSmall)
	ex := spark.NewK8sExecutor(cfg)
	defer ex.Close()

	ex.Submit(context.Background(), spark.SparkJob{JobID: "j1", Config: cfg})
	status, err := ex.Status(context.Background(), "j1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	// Mock immediately completes
	if status.State != spark.StateCompleted {
		t.Errorf("expected COMPLETED, got %s", status.State)
	}
}

func TestK8sExecutor_Cancel_DelegatesToMock(t *testing.T) {
	cfg := spark.SparkConfigFrom("k8s", spark.SizeSmall)
	ex := spark.NewK8sExecutor(cfg)
	defer ex.Close()

	ex.Submit(context.Background(), spark.SparkJob{JobID: "j1", Config: cfg})
	if err := ex.Cancel(context.Background(), "j1"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
}

func TestK8sExecutor_Close(t *testing.T) {
	ex := spark.NewK8sExecutor(spark.SparkConfigFrom("k8s", spark.SizeSmall))
	if err := ex.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
