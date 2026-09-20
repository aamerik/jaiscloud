package grpcconformance

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestRunAllClassifies verifies the three-way status classification, including
// the wrapped-Unimplemented case (an RPC error wrapped with %w must still be
// recognized as a fidelity gap, not a generic failure).
func TestRunAllClassifies(t *testing.T) {
	checks := []Check{
		{Service: "svc", RPC: "Ok", Run: func(context.Context, Config) error { return nil }},
		{Service: "svc", RPC: "Gap", Run: func(context.Context, Config) error {
			return status.Error(codes.Unimplemented, "not implemented")
		}},
		{Service: "svc", RPC: "WrappedGap", Run: func(context.Context, Config) error {
			return fmt.Errorf("call failed: %w", status.Error(codes.Unimplemented, "nested"))
		}},
		{Service: "svc", RPC: "Bad", Run: func(context.Context, Config) error {
			return status.Error(codes.Internal, "boom")
		}},
	}

	results := RunAll(context.Background(), Config{}, checks)
	want := []Status{StatusPass, StatusUnimplemented, StatusUnimplemented, StatusFail}
	if len(results) != len(want) {
		t.Fatalf("got %d results, want %d", len(results), len(want))
	}
	for i, r := range results {
		if r.Status != want[i] {
			t.Errorf("%s status = %s, want %s", r.RPC, r.Status, want[i])
		}
	}
	if results[0].Error != "" {
		t.Errorf("pass result should carry no error, got %q", results[0].Error)
	}
	for _, i := range []int{1, 2, 3} {
		if results[i].Error == "" {
			t.Errorf("%s should carry an error message", results[i].RPC)
		}
	}
}

// TestBuildReportRollup verifies the per-service summary and the suite rollup.
func TestBuildReportRollup(t *testing.T) {
	results := []Result{
		{Service: "storage", RPC: "CreateBucket", Status: StatusPass},
		{Service: "storage", RPC: "GetBucket", Status: StatusFail, Error: "boom"},
		{Service: "kms", RPC: "GetKeyRing", Status: StatusUnimplemented, Error: "gap"},
	}
	rep := BuildReport(Config{Endpoint: "localhost:8081", Project: "p"}, results)

	if rep.Total != 3 || rep.Passed != 1 || rep.Failed != 1 || rep.Unimplemented != 1 {
		t.Fatalf("totals = total:%d pass:%d fail:%d unimpl:%d, want 3/1/1/1",
			rep.Total, rep.Passed, rep.Failed, rep.Unimplemented)
	}
	if rep.Rollup.Services != 2 || rep.Rollup.Checks != 3 {
		t.Errorf("rollup = %+v, want services=2 checks=3", rep.Rollup)
	}
	if got, want := rep.Rollup.PassRate, 100.0/3.0; got < want-0.01 || got > want+0.01 {
		t.Errorf("pass rate = %v, want ~%v", got, want)
	}
	if len(rep.Services) != 2 || rep.Services[0].Service != "kms" || rep.Services[1].Service != "storage" {
		t.Errorf("services not sorted by name: %+v", rep.Services)
	}
}
