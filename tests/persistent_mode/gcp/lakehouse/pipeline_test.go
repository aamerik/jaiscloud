//go:build lakehouse_e2e

package lakehouse_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// TestLakehousePipelineK3d runs the vendor-neutral Medallion (multi-hop) ELT
// pipeline entirely inside the k3d cluster and verifies the published data
// product.
//
// Ingest seeds raw events into the emulator's GCS; Transform and Derive are
// real Spark jobs submitted through the emulator's Dataproc API and executed as
// client-mode pods in the cluster; Publish copies the curated output to the
// serving bucket and writes a manifest.
//
// The test asserts both halves of the contract: the in-cluster Job's own
// success summary, and the actual bytes in the emulator's GCS (read back
// through a port-forward), so a pipeline that "succeeds" without producing
// data still fails.
func TestLakehousePipelineK3d(t *testing.T) {
	requireK3d(t)

	deleteJob(t)
	applyPipeline(t)
	t.Cleanup(func() {
		_, _ = kubectl("-n", namespace(), "delete", "job", jobName, "--ignore-not-found")
	})

	// Both Spark hops run as Dataproc jobs; the emulator waits for each before
	// the driver proceeds, so Job completion implies both reached DONE.
	waitJobComplete(t, 15*time.Minute)

	logs := jobLogs(t)
	if strings.Contains(logs, "PIPELINE_FAILED") {
		t.Fatalf("pipeline reported failure:\n%s", logs)
	}
	sum := parseSummary(t, logs)

	if !sum.OK {
		t.Fatalf("pipeline summary not OK: %+v", sum)
	}
	if sum.Pipeline != "medallion-elt" {
		t.Errorf("pipeline = %q, want medallion-elt", sum.Pipeline)
	}
	if got, want := strings.Join(sum.Stages, ","), "ingest,transform,derive,publish"; got != want {
		t.Errorf("stages = %q, want %q", got, want)
	}
	if sum.InputRows != 24 {
		t.Errorf("input_rows = %d, want 24", sum.InputRows)
	}
	if len(sum.CuratedObjects) == 0 {
		t.Fatalf("no curated objects in summary: %+v", sum)
	}
	if len(sum.PublishedObjects) != len(sum.CuratedObjects) {
		t.Errorf("published %d object(s), want %d (one per curated part)",
			len(sum.PublishedObjects), len(sum.CuratedObjects))
	}

	base, stop := startPortForward(t)
	defer stop()

	// The serving manifest is the pipeline's published contract.
	manifestRaw := getObject(t, base, publishedBucket, "manifest.json")
	var manifest struct {
		Pipeline         string   `json:"pipeline"`
		InputRows        int      `json:"input_rows"`
		PublishedObjects []string `json:"published_objects"`
	}
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil {
		t.Fatalf("parse published manifest: %v\n%s", err, manifestRaw)
	}
	if manifest.Pipeline != "medallion-elt" || manifest.InputRows != 24 {
		t.Errorf("published manifest mismatch: %+v", manifest)
	}

	// Read every published part and aggregate orders per region.
	orders := map[string]int{}
	for _, name := range sum.PublishedObjects {
		for _, line := range strings.Split(string(getObject(t, base, publishedBucket, name)), "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			var row struct {
				Region string `json:"region"`
				Orders int    `json:"orders"`
			}
			if err := json.Unmarshal([]byte(line), &row); err != nil {
				t.Fatalf("parse published row %q: %v", line, err)
			}
			orders[row.Region] += row.Orders
		}
	}

	want := map[string]int{"US": 12, "EU": 8, "APAC": 4}
	if len(orders) != len(want) {
		t.Fatalf("regions = %v, want %v", orders, want)
	}
	total := 0
	for region, n := range want {
		if orders[region] != n {
			t.Errorf("orders[%s] = %d, want %d", region, orders[region], n)
		}
		total += n
	}
	if total != 24 {
		t.Errorf("total orders = %d, want 24", total)
	}

	t.Logf("pipeline OK: %d input rows -> %d published object(s), orders=%v",
		sum.InputRows, len(sum.PublishedObjects), orders)
}
