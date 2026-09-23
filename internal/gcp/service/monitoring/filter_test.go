package monitoring

import (
	"testing"

	monitoringstore "jaiscloud/internal/gcp/store/monitoring"
)

func TestCompileTSFilter(t *testing.T) {
	f, err := compileTSFilter(`metric.type = "custom.googleapis.com/foo" AND resource.type = "global"`)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if f.metricType != "custom.googleapis.com/foo" || f.resourceType != "global" {
		t.Fatalf("filter = %+v", f)
	}

	// Single clause is fine.
	f, err = compileTSFilter(`metric.type = "custom.googleapis.com/foo"`)
	if err != nil || f.metricType != "custom.googleapis.com/foo" || f.resourceType != "" {
		t.Fatalf("single clause = %+v, %v", f, err)
	}

	// Empty filter is rejected.
	if _, err := compileTSFilter(""); err == nil {
		t.Fatal("empty filter should error")
	}

	// Unsupported key is rejected.
	if _, err := compileTSFilter(`resource.label.foo = "bar"`); err == nil {
		t.Fatal("unsupported key should error")
	}

	// Unquoted value is rejected.
	if _, err := compileTSFilter(`metric.type = bare`); err == nil {
		t.Fatal("unquoted value should error")
	}
}

func TestTSFilterMatch(t *testing.T) {
	f := tsFilter{metricType: "custom.googleapis.com/foo", resourceType: "global"}
	ts := monitoringstore.TimeSeries{MetricType: "custom.googleapis.com/foo", ResourceType: "global"}
	if !f.match(ts) {
		t.Fatal("match failed for identical series")
	}
	if f.match(monitoringstore.TimeSeries{MetricType: "other", ResourceType: "global"}) {
		t.Fatal("metric.type mismatch should not match")
	}
	if f.match(monitoringstore.TimeSeries{MetricType: "custom.googleapis.com/foo", ResourceType: "aws_ec2_instance"}) {
		t.Fatal("resource.type mismatch should not match")
	}
}
