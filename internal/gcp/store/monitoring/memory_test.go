package monitoring

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func double(v float64) *float64 { return &v }

func testPoint(v float64, ts time.Time) Point {
	return Point{EndTime: ts, Value: TypedValue{DoubleValue: double(v)}}
}

func TestMemoryStoreMetricDescriptorCRUD(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	if got, err := s.ListMetricDescriptors(ctx, "p1"); err != nil || len(got) != 0 {
		t.Fatalf("initial list = %v, %v", got, err)
	}

	d := MetricDescriptor{Type: "custom.googleapis.com/test", ValueType: 3, MetricKind: 1, Unit: "1", Labels: []LabelDescriptor{{Key: "old", ValueType: 1}}}
	if _, err := s.CreateMetricDescriptor(ctx, "p1", d); err != nil {
		t.Fatal(err)
	}

	// Re-creating the same type upserts: fields are overwritten and existing
	// labels are unioned (never removed).
	dup := MetricDescriptor{Type: d.Type, ValueType: 4, MetricKind: 2, Unit: "2", Description: "updated", Labels: []LabelDescriptor{{Key: "new", ValueType: 2}}}
	stored, err := s.CreateMetricDescriptor(ctx, "p1", dup)
	if err != nil {
		t.Fatalf("upsert err = %v", err)
	}
	if stored.Description != "updated" || stored.ValueType != 4 {
		t.Fatalf("upsert fields not overwritten = %+v", stored)
	}
	if len(stored.Labels) != 2 {
		t.Fatalf("upsert labels = %+v, want 2 (old retained)", stored.Labels)
	}

	got, err := s.GetMetricDescriptor(ctx, "p1", d.Type)
	if err != nil || got.Type != d.Type || got.Unit != "2" {
		t.Fatalf("get = %+v, %v", got, err)
	}
	if _, err := s.GetMetricDescriptor(ctx, "p2", d.Type); err != ErrMetricDescriptorNotFound {
		t.Fatalf("get other project err = %v, want ErrMetricDescriptorNotFound", err)
	}

	list, err := s.ListMetricDescriptors(ctx, "p1")
	if err != nil || len(list) != 1 || list[0].Type != d.Type {
		t.Fatalf("list = %+v, %v", list, err)
	}

	if err := s.DeleteMetricDescriptor(ctx, "p1", d.Type); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteMetricDescriptor(ctx, "p1", d.Type); err != ErrMetricDescriptorNotFound {
		t.Fatalf("delete missing err = %v, want ErrMetricDescriptorNotFound", err)
	}
}

func TestMemoryStoreTimeSeriesAppend(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	base := time.Now().UTC()

	ts := TimeSeries{
		MetricType:   "custom.googleapis.com/m",
		MetricLabels: map[string]string{"k": "v"},
		ResourceType: "global",
		Points:       []Point{testPoint(1, base)},
	}
	if err := s.CreateTimeSeries(ctx, "p1", ts); err != nil {
		t.Fatal(err)
	}

	// A second write with the same identity appends points rather than creating
	// a new series.
	ts.Points = []Point{testPoint(2, base.Add(time.Second))}
	if err := s.CreateTimeSeries(ctx, "p1", ts); err != nil {
		t.Fatal(err)
	}

	got, err := s.ListTimeSeries(ctx, "p1")
	if err != nil || len(got) != 1 {
		t.Fatalf("list = %+v, %v", got, err)
	}
	if len(got[0].Points) != 2 {
		t.Fatalf("points = %d, want 2", len(got[0].Points))
	}

	// A different label set is a different series.
	ts.MetricLabels = map[string]string{"k": "other"}
	if err := s.CreateTimeSeries(ctx, "p1", ts); err != nil {
		t.Fatal(err)
	}
	got, _ = s.ListTimeSeries(ctx, "p1")
	if len(got) != 2 {
		t.Fatalf("list after distinct series = %d, want 2", len(got))
	}
}

func TestMemoryStoreAlertPolicyCRUD(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	enabled := true
	p := AlertPolicy{ID: "ap-1", DisplayName: "High CPU", Combiner: 1, Enabled: &enabled}
	if err := s.CreateAlertPolicy(ctx, "p1", p); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateAlertPolicy(ctx, "p1", p); err != ErrAlertPolicyExists {
		t.Fatalf("duplicate policy err = %v, want ErrAlertPolicyExists", err)
	}

	got, err := s.GetAlertPolicy(ctx, "p1", "ap-1")
	if err != nil || got.DisplayName != "High CPU" {
		t.Fatalf("get = %+v, %v", got, err)
	}
	if _, err := s.GetAlertPolicy(ctx, "p1", "missing"); err != ErrAlertPolicyNotFound {
		t.Fatalf("get missing err = %v, want ErrAlertPolicyNotFound", err)
	}

	list, err := s.ListAlertPolicies(ctx, "p1")
	if err != nil || len(list) != 1 {
		t.Fatalf("list = %+v, %v", list, err)
	}

	p.DisplayName = "Updated"
	if err := s.UpdateAlertPolicy(ctx, "p1", p); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetAlertPolicy(ctx, "p1", "ap-1")
	if got.DisplayName != "Updated" {
		t.Fatalf("after update = %+v", got)
	}

	if err := s.DeleteAlertPolicy(ctx, "p1", "ap-1"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteAlertPolicy(ctx, "p1", "ap-1"); err != ErrAlertPolicyNotFound {
		t.Fatalf("delete missing err = %v, want ErrAlertPolicyNotFound", err)
	}
}

func TestMemoryStoreReset(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_, _ = s.CreateMetricDescriptor(ctx, "p1", MetricDescriptor{Type: "t"})
	_ = s.CreateTimeSeries(ctx, "p1", TimeSeries{MetricType: "m", ResourceType: "r", Points: []Point{testPoint(1, time.Now())}})
	_ = s.CreateAlertPolicy(ctx, "p1", AlertPolicy{ID: "a", DisplayName: "x"})

	s.Reset(ctx)

	if got, _ := s.ListMetricDescriptors(ctx, "p1"); len(got) != 0 {
		t.Fatalf("descriptors after reset = %+v", got)
	}
	if got, _ := s.ListTimeSeries(ctx, "p1"); len(got) != 0 {
		t.Fatalf("series after reset = %+v", got)
	}
	if got, _ := s.ListAlertPolicies(ctx, "p1"); len(got) != 0 {
		t.Fatalf("policies after reset = %+v", got)
	}
}

func TestMemoryStoreSnapshot(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_, _ = s.CreateMetricDescriptor(ctx, "p1", MetricDescriptor{Type: "t", ValueType: 3})
	_ = s.CreateTimeSeries(ctx, "p1", TimeSeries{MetricType: "m", ResourceType: "r", Points: []Point{testPoint(1, time.Now().UTC())}})
	_ = s.CreateAlertPolicy(ctx, "p1", AlertPolicy{ID: "a", DisplayName: "x", Combiner: 1})

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatal(err)
	}

	s2 := NewMemoryStore()
	if err := s2.Restore(ctx, &buf); err != nil {
		t.Fatal(err)
	}

	if got, _ := s2.ListMetricDescriptors(ctx, "p1"); len(got) != 1 || got[0].Type != "t" {
		t.Fatalf("restored descriptors = %+v", got)
	}
	if got, _ := s2.ListTimeSeries(ctx, "p1"); len(got) != 1 || len(got[0].Points) != 1 {
		t.Fatalf("restored series = %+v", got)
	}
	if got, _ := s2.ListAlertPolicies(ctx, "p1"); len(got) != 1 || got[0].DisplayName != "x" {
		t.Fatalf("restored policies = %+v", got)
	}
}

func TestSeriesKeyDeterministic(t *testing.T) {
	a := TimeSeries{MetricType: "m", MetricLabels: map[string]string{"x": "1", "y": "2"}, ResourceType: "r", ResourceLabels: map[string]string{"z": "3"}}
	b := TimeSeries{MetricType: "m", MetricLabels: map[string]string{"y": "2", "x": "1"}, ResourceType: "r", ResourceLabels: map[string]string{"z": "3"}}
	if seriesKey(a) != seriesKey(b) {
		t.Fatalf("seriesKey not label-order independent: %q vs %q", seriesKey(a), seriesKey(b))
	}
}
