package spark

import (
	"testing"

	"jaiscloud/internal/model"
)

func TestSelectTransform_Registered(t *testing.T) {
	for _, cloud := range []model.Cloud{model.CloudAWS, model.CloudAzure, model.CloudGCP} {
		t.Run(string(cloud), func(t *testing.T) {
			tr, err := selectTransform(cloud)
			if err != nil {
				t.Fatalf("selectTransform(%q): %v", cloud, err)
			}
			if tr.Cloud() != cloud {
				t.Errorf("Cloud() = %q, want %q", tr.Cloud(), cloud)
			}
		})
	}
}

func TestSelectTransform_Unknown(t *testing.T) {
	_, err := selectTransform(model.Cloud("nublar"))
	if err == nil {
		t.Fatal("expected error for unknown cloud, got nil")
	}
}

func TestValidateURIs_AWS_S3AAllowed(t *testing.T) {
	tr, _ := selectTransform(model.CloudAWS)
	args := []string{"s3a://bucket/app.jar", "--master", "local[*]"}
	if err := tr.ValidateURIs(args, SparkConfig{}); err != nil {
		t.Errorf("s3a:// should be allowed on AWS, got: %v", err)
	}
}

func TestValidateURIs_AWS_ABFSSRejected(t *testing.T) {
	tr, _ := selectTransform(model.CloudAWS)
	args := []string{"abfss://container@acct.dfs.core.windows.net/app.jar"}
	if err := tr.ValidateURIs(args, SparkConfig{}); err == nil {
		t.Error("abfss:// should be rejected on AWS")
	}
}

func TestValidateURIs_Azure_ABFSSAllowed(t *testing.T) {
	tr, _ := selectTransform(model.CloudAzure)
	args := []string{"abfss://container@acct.dfs.core.windows.net/app.jar"}
	if err := tr.ValidateURIs(args, SparkConfig{}); err != nil {
		t.Errorf("abfss:// should be allowed on Azure, got: %v", err)
	}
}

func TestValidateURIs_Azure_S3ARejected(t *testing.T) {
	tr, _ := selectTransform(model.CloudAzure)
	args := []string{"s3a://bucket/app.jar"}
	if err := tr.ValidateURIs(args, SparkConfig{}); err == nil {
		t.Error("s3a:// should be rejected on Azure")
	}
}

func TestValidateURIs_GCP_GSAllowed(t *testing.T) {
	tr, _ := selectTransform(model.CloudGCP)
	args := []string{"gs://bucket/app.jar"}
	if err := tr.ValidateURIs(args, SparkConfig{}); err != nil {
		t.Errorf("gs:// should be allowed on GCP, got: %v", err)
	}
}

func TestValidateURIs_GCP_S3Rejected(t *testing.T) {
	tr, _ := selectTransform(model.CloudGCP)
	args := []string{"s3://bucket/app.jar"}
	if err := tr.ValidateURIs(args, SparkConfig{}); err == nil {
		t.Error("s3:// should be rejected on GCP")
	}
}

func TestValidateURIs_BarePathAllowedOnAllClouds(t *testing.T) {
	args := []string{"/opt/spark/examples/jars/spark-examples.jar", "--class", "App"}
	for _, cloud := range []model.Cloud{model.CloudAWS, model.CloudAzure, model.CloudGCP} {
		tr, _ := selectTransform(cloud)
		if err := tr.ValidateURIs(args, SparkConfig{}); err != nil {
			t.Errorf("cloud=%s: bare path should be allowed, got: %v", cloud, err)
		}
	}
}

func TestValidateURIs_HTTPSkipped(t *testing.T) {
	// https:// is in schemesToSkip — should not be flagged even on AWS.
	tr, _ := selectTransform(model.CloudAWS)
	args := []string{"--conf", "spark.kubernetes.container.image=myregistry.example.com/spark:3.5.0",
		"--conf", "spark.driver.extraJavaOptions=-Djava.net.useSystemProxies=true",
		"https://example.com/should-be-skipped",
	}
	if err := tr.ValidateURIs(args, SparkConfig{}); err != nil {
		t.Errorf("https:// should be skipped, got: %v", err)
	}
}
