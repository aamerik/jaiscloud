package spark

import (
	"strings"
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

func TestRewriteS3aToABFS_WithBucket(t *testing.T) {
	got := rewriteS3aToABFS("s3a://mybucket/some/key.parquet", "mystorageacct")
	want := "abfss://mybucket@mystorageacct.dfs.core.windows.net/some/key.parquet"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestRewriteS3aToABFS_BucketOnly(t *testing.T) {
	got := rewriteS3aToABFS("s3a://mybucket", "acct")
	want := "abfss://mybucket@acct.dfs.core.windows.net"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestRewriteS3aToABFS_NonS3APassthrough(t *testing.T) {
	uri := "gs://bucket/key"
	if got := rewriteS3aToABFS(uri, "acct"); got != uri {
		t.Errorf("non-s3a URI should pass through unchanged, got %q", got)
	}
}

func TestRewriteS3aToABFS_PlainArgPassthrough(t *testing.T) {
	arg := "--master"
	if got := rewriteS3aToABFS(arg, "acct"); got != arg {
		t.Errorf("plain arg should pass through unchanged, got %q", got)
	}
}

func TestRewriteS3aToGCS(t *testing.T) {
	got := rewriteS3aToGCS("s3a://bucket/path/to/file.jar")
	want := "gs://bucket/path/to/file.jar"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestRewriteS3aToGCS_NonS3APassthrough(t *testing.T) {
	uri := "abfss://bucket@acct.dfs.core.windows.net/key"
	if got := rewriteS3aToGCS(uri); got != uri {
		t.Errorf("non-s3a URI should pass through unchanged, got %q", got)
	}
}

func TestRewriteURIs_GCP(t *testing.T) {
	tr, _ := selectTransform(model.CloudGCP)
	args := []string{"--class", "com.example.Main", "s3a://bucket/app.jar", "s3a://bucket/data"}
	got := rewriteURIs(tr, args, SparkConfig{})
	for i, a := range got {
		if strings.HasPrefix(a, "s3a://") {
			t.Errorf("arg[%d] still has s3a:// prefix after rewrite: %q", i, a)
		}
	}
	if got[0] != "--class" {
		t.Errorf("non-URI arg[0] changed: got %q, want %q", got[0], "--class")
	}
	if got[2] != "gs://bucket/app.jar" {
		t.Errorf("arg[2]: got %q, want %q", got[2], "gs://bucket/app.jar")
	}
}

func TestRewriteURIs_AWS_Passthrough(t *testing.T) {
	tr, _ := selectTransform(model.CloudAWS)
	args := []string{"s3a://bucket/app.jar", "--master", "local[*]"}
	got := rewriteURIs(tr, args, SparkConfig{})
	for i := range args {
		if got[i] != args[i] {
			t.Errorf("AWS rewrite should be identity; arg[%d]: got %q, want %q", i, got[i], args[i])
		}
	}
}
