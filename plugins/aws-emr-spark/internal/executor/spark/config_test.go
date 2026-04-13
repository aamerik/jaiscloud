package spark_test

import (
	"testing"

	"github.com/jaiscloud/plugin-aws-emr-spark/internal/executor/spark"
)

func TestSparkConfigFrom_Small(t *testing.T) {
	cfg := spark.SparkConfigFrom("mock", spark.SizeSmall)
	if cfg.Resources.ExecutorCount != 1 {
		t.Errorf("small: expected 1 executor, got %d", cfg.Resources.ExecutorCount)
	}
	if cfg.Image != spark.DefaultImage {
		t.Errorf("expected default image %q, got %q", spark.DefaultImage, cfg.Image)
	}
}

func TestSparkConfigFrom_Medium(t *testing.T) {
	cfg := spark.SparkConfigFrom("k8s", spark.SizeMedium)
	if cfg.Resources.ExecutorCount != 2 {
		t.Errorf("medium: expected 2 executors, got %d", cfg.Resources.ExecutorCount)
	}
}

func TestSparkConfigFrom_Large(t *testing.T) {
	cfg := spark.SparkConfigFrom("k8s", spark.SizeLarge)
	if cfg.Resources.ExecutorCount != 4 {
		t.Errorf("large: expected 4 executors, got %d", cfg.Resources.ExecutorCount)
	}
}

func TestSparkConfigFrom_UnknownSizeFallsBack(t *testing.T) {
	cfg := spark.SparkConfigFrom("mock", "unknown-size")
	if cfg.Resources.ExecutorCount != 1 {
		t.Errorf("unknown size should fall back to small (1 executor), got %d", cfg.Resources.ExecutorCount)
	}
}

func TestSparkConfigFrom_EnvImageOverride(t *testing.T) {
	t.Setenv("JAISCLOUD_K8S_SPARK_IMAGE", "custom/spark:2.0")
	cfg := spark.SparkConfigFrom("k8s", spark.SizeSmall)
	if cfg.Image != "custom/spark:2.0" {
		t.Errorf("expected image %q, got %q", "custom/spark:2.0", cfg.Image)
	}
}

func TestSparkConfigFrom_EnvImageOverride_FuncOverrideTakesPrecedence(t *testing.T) {
	t.Setenv("JAISCLOUD_K8S_SPARK_IMAGE", "env/spark:1.0")
	cfg := spark.SparkConfigFrom("k8s", spark.SizeSmall, func(c *spark.SparkConfig) {
		c.Image = "func/spark:2.0"
	})
	if cfg.Image != "func/spark:2.0" {
		t.Errorf("functional override should take precedence over env var, got %q", cfg.Image)
	}
}

func TestSparkConfigFrom_Override(t *testing.T) {
	cfg := spark.SparkConfigFrom("k8s", spark.SizeSmall, func(c *spark.SparkConfig) {
		c.Namespace = "spark-jobs"
		c.S3LogURI = "s3://my-bucket/logs"
	})
	if cfg.Namespace != "spark-jobs" {
		t.Errorf("override failed: namespace = %q", cfg.Namespace)
	}
	if cfg.S3LogURI != "s3://my-bucket/logs" {
		t.Errorf("override failed: S3LogURI = %q", cfg.S3LogURI)
	}
}
