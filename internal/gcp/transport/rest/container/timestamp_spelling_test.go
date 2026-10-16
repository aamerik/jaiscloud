package container

import (
	"testing"
	"time"

	containerstore "jaiscloud/internal/gcp/store/container"
)

// TestClusterToJSONTimestampSpelling pins the protojson spelling of the Cluster
// createTime google-datetime field.
func TestClusterToJSONTimestampSpelling(t *testing.T) {
	out := clusterToJSON(containerstore.Cluster{
		ProjectID:  "proj",
		Location:   "us-central1",
		Name:       "c1",
		CreateTime: time.Date(2024, 3, 1, 12, 0, 0, 500_000_000, time.UTC),
	})
	if got := out["createTime"]; got != "2024-03-01T12:00:00.500Z" {
		t.Fatalf("createTime = %v, want 2024-03-01T12:00:00.500Z", got)
	}
}
