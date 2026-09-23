package monitoring

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"

	"jaiscloud/internal/clock"
	monitoringstore "jaiscloud/internal/gcp/store/monitoring"
)

// recordingPublisher is a Publisher that records every delivery.
type recordingPublisher struct {
	mu       sync.Mutex
	topics   []string
	payloads [][]byte
	fail     bool
}

func (p *recordingPublisher) Publish(_ context.Context, topic string, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail {
		return errors.New("publish failed")
	}
	p.topics = append(p.topics, topic)
	p.payloads = append(p.payloads, data)
	return nil
}

func (p *recordingPublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.topics)
}

func newEvalFixture(t *testing.T) (monitoringstore.Store, *recordingPublisher, *Evaluator) {
	t.Helper()
	store := monitoringstore.NewMemoryStore()
	pub := &recordingPublisher{}
	ev := NewEvaluator(store, pub, WithProjects(func() []string { return []string{"test"} }))
	return store, pub, ev
}

func putChannel(t *testing.T, store monitoringstore.Store, project, id, typ string, labels map[string]string) {
	t.Helper()
	enabled := true
	if err := store.CreateNotificationChannel(context.Background(), project, monitoringstore.NotificationChannel{
		ID: id, Type: typ, DisplayName: id, Labels: labels, Enabled: &enabled,
	}); err != nil {
		t.Fatalf("create channel: %v", err)
	}
}

func putSeries(t *testing.T, store monitoringstore.Store, project, metricType, resourceType string, labels map[string]string, value float64, end time.Time) {
	t.Helper()
	v := value
	if err := store.CreateTimeSeries(context.Background(), project, monitoringstore.TimeSeries{
		MetricType:   metricType,
		MetricLabels: labels,
		ResourceType: resourceType,
		MetricKind:   1,
		ValueType:    3,
		Points:       []monitoringstore.Point{{EndTime: end, Value: monitoringstore.TypedValue{DoubleValue: &v}}},
	}); err != nil {
		t.Fatalf("create time series: %v", err)
	}
}

func thresholdCondition(name, filter string, cmp monitoringpb.ComparisonType, threshold float64) *monitoringpb.AlertPolicy_Condition {
	return &monitoringpb.AlertPolicy_Condition{
		DisplayName: name,
		Condition: &monitoringpb.AlertPolicy_Condition_ConditionThreshold{ConditionThreshold: &monitoringpb.AlertPolicy_Condition_MetricThreshold{
			Filter:         filter,
			Comparison:     cmp,
			ThresholdValue: threshold,
		}},
	}
}

var policySeq int

func putPolicy(t *testing.T, store monitoringstore.Store, project string, combiner monitoringpb.AlertPolicy_ConditionCombinerType, channels []string, conds ...*monitoringpb.AlertPolicy_Condition) string {
	t.Helper()
	policySeq++
	id := fmt.Sprintf("ap-%d", policySeq)
	raw := make([]json.RawMessage, 0, len(conds))
	for _, c := range conds {
		b, err := protojson.Marshal(c)
		if err != nil {
			t.Fatalf("marshal condition: %v", err)
		}
		raw = append(raw, json.RawMessage(b))
	}
	enabled := true
	p := monitoringstore.AlertPolicy{
		ID: id, DisplayName: "policy " + id, Combiner: int32(combiner), Enabled: &enabled,
		Conditions: raw, NotificationChannels: channels,
	}
	if err := store.CreateAlertPolicy(context.Background(), project, p); err != nil {
		t.Fatalf("create policy: %v", err)
	}
	return id
}

func cpuFilter() string {
	return `metric.type = "custom.googleapis.com/cpu" AND resource.type = "global"`
}

func TestEvaluatorThresholdCrossingOpensIncidentAndDelivers(t *testing.T) {
	store, pub, ev := newEvalFixture(t)
	ctx := context.Background()
	putChannel(t, store, "test", "nc1", "pubsub", map[string]string{"topic": "projects/test/topics/alerts"})
	policyID := putPolicy(t, store, "test", monitoringpb.AlertPolicy_AND,
		[]string{"projects/test/notificationChannels/nc1"},
		thresholdCondition("cpu high", cpuFilter(), monitoringpb.ComparisonType_COMPARISON_GT, 0.9))
	putSeries(t, store, "test", "custom.googleapis.com/cpu", "global", nil, 0.95, clock.Now())

	ev.EvaluateAll(ctx)

	inc, err := store.FindOpenIncident(ctx, "test", policyID)
	if err != nil {
		t.Fatalf("expected an open incident: %v", err)
	}
	if inc.State != monitoringstore.IncidentOpen {
		t.Fatalf("incident state = %q, want OPEN", inc.State)
	}
	if pub.count() != 1 {
		t.Fatalf("published %d notifications, want 1", pub.count())
	}
	if pub.topics[0] != "projects/test/topics/alerts" {
		t.Fatalf("published to %q", pub.topics[0])
	}
	if len(inc.Notifications) != 1 || inc.Notifications[0].Status != "delivered" {
		t.Fatalf("incident notifications = %+v", inc.Notifications)
	}

	// A second evaluation with the same firing state must not re-open or
	// re-notify.
	ev.EvaluateAll(ctx)
	if pub.count() != 1 {
		t.Fatalf("published %d notifications after second tick, want 1 (no duplicate)", pub.count())
	}
}

func TestEvaluatorBelowThresholdDoesNotFire(t *testing.T) {
	store, pub, ev := newEvalFixture(t)
	ctx := context.Background()
	putChannel(t, store, "test", "nc1", "pubsub", map[string]string{"topic": "projects/test/topics/alerts"})
	policyID := putPolicy(t, store, "test", monitoringpb.AlertPolicy_AND,
		[]string{"projects/test/notificationChannels/nc1"},
		thresholdCondition("cpu high", cpuFilter(), monitoringpb.ComparisonType_COMPARISON_GT, 0.9))
	putSeries(t, store, "test", "custom.googleapis.com/cpu", "global", nil, 0.5, clock.Now())

	ev.EvaluateAll(ctx)

	if _, err := store.FindOpenIncident(ctx, "test", policyID); !errors.Is(err, monitoringstore.ErrIncidentNotFound) {
		t.Fatalf("find open incident err = %v, want ErrIncidentNotFound", err)
	}
	if pub.count() != 0 {
		t.Fatalf("published %d notifications, want 0", pub.count())
	}
}

func TestEvaluatorResolveClosesIncidentAndNotifies(t *testing.T) {
	store, pub, ev := newEvalFixture(t)
	ctx := context.Background()
	putChannel(t, store, "test", "nc1", "pubsub", map[string]string{"topic": "projects/test/topics/alerts"})
	policyID := putPolicy(t, store, "test", monitoringpb.AlertPolicy_AND,
		[]string{"projects/test/notificationChannels/nc1"},
		thresholdCondition("cpu high", cpuFilter(), monitoringpb.ComparisonType_COMPARISON_GT, 0.9))
	putSeries(t, store, "test", "custom.googleapis.com/cpu", "global", nil, 0.95, clock.Now().Add(-2*time.Second))

	ev.EvaluateAll(ctx)
	if _, err := store.FindOpenIncident(ctx, "test", policyID); err != nil {
		t.Fatalf("expected open incident: %v", err)
	}

	// A newer point below the threshold resolves the incident.
	putSeries(t, store, "test", "custom.googleapis.com/cpu", "global", nil, 0.1, clock.Now().Add(-1*time.Second))
	ev.EvaluateAll(ctx)

	if _, err := store.FindOpenIncident(ctx, "test", policyID); !errors.Is(err, monitoringstore.ErrIncidentNotFound) {
		t.Fatalf("find open incident after resolve err = %v, want ErrIncidentNotFound", err)
	}
	incidents, err := store.ListIncidents(ctx, "test")
	if err != nil || len(incidents) != 1 {
		t.Fatalf("incidents = %+v, %v", incidents, err)
	}
	if incidents[0].State != monitoringstore.IncidentClosed || incidents[0].EndedAt.IsZero() {
		t.Fatalf("closed incident = %+v", incidents[0])
	}
	if pub.count() != 2 {
		t.Fatalf("published %d notifications, want 2 (open + resolve)", pub.count())
	}
}

func TestEvaluatorCombinerAND(t *testing.T) {
	store, pub, ev := newEvalFixture(t)
	ctx := context.Background()
	// First condition fires, second does not -> AND must not fire.
	putPolicy(t, store, "test", monitoringpb.AlertPolicy_AND, nil,
		thresholdCondition("cpu high", cpuFilter(), monitoringpb.ComparisonType_COMPARISON_GT, 0.9),
		thresholdCondition("mem high", `metric.type = "custom.googleapis.com/mem"`, monitoringpb.ComparisonType_COMPARISON_GT, 0.9))
	putSeries(t, store, "test", "custom.googleapis.com/cpu", "global", nil, 0.95, clock.Now())
	putSeries(t, store, "test", "custom.googleapis.com/mem", "global", nil, 0.1, clock.Now())

	ev.EvaluateAll(ctx)

	if pub.count() != 0 {
		t.Fatalf("AND published %d notifications, want 0", pub.count())
	}
	if got, _ := store.ListIncidents(ctx, "test"); len(got) != 0 {
		t.Fatalf("AND incidents = %+v, want none", got)
	}
}

func TestEvaluatorCombinerOR(t *testing.T) {
	store, pub, ev := newEvalFixture(t)
	ctx := context.Background()
	putChannel(t, store, "test", "nc1", "pubsub", map[string]string{"topic": "projects/test/topics/alerts"})
	putPolicy(t, store, "test", monitoringpb.AlertPolicy_OR,
		[]string{"projects/test/notificationChannels/nc1"},
		thresholdCondition("cpu high", cpuFilter(), monitoringpb.ComparisonType_COMPARISON_GT, 0.9),
		thresholdCondition("mem high", `metric.type = "custom.googleapis.com/mem"`, monitoringpb.ComparisonType_COMPARISON_GT, 0.9))
	putSeries(t, store, "test", "custom.googleapis.com/cpu", "global", nil, 0.95, clock.Now())
	putSeries(t, store, "test", "custom.googleapis.com/mem", "global", nil, 0.1, clock.Now())

	ev.EvaluateAll(ctx)

	if pub.count() != 1 {
		t.Fatalf("OR published %d notifications, want 1", pub.count())
	}
	if got, _ := store.ListIncidents(ctx, "test"); len(got) != 1 || got[0].State != monitoringstore.IncidentOpen {
		t.Fatalf("OR incidents = %+v, want one OPEN", got)
	}
}

func TestEvaluatorNoPointsDoesNotFire(t *testing.T) {
	store, pub, ev := newEvalFixture(t)
	ctx := context.Background()
	policyID := putPolicy(t, store, "test", monitoringpb.AlertPolicy_AND, nil,
		thresholdCondition("cpu high", cpuFilter(), monitoringpb.ComparisonType_COMPARISON_GT, 0.9))

	ev.EvaluateAll(ctx)

	if _, err := store.FindOpenIncident(ctx, "test", policyID); !errors.Is(err, monitoringstore.ErrIncidentNotFound) {
		t.Fatalf("find open incident err = %v, want ErrIncidentNotFound (no points)", err)
	}
	if pub.count() != 0 {
		t.Fatalf("published %d notifications without data, want 0", pub.count())
	}
}

func TestEvaluatorDisabledPolicySkipped(t *testing.T) {
	store, pub, ev := newEvalFixture(t)
	ctx := context.Background()
	b, _ := protojson.Marshal(thresholdCondition("cpu high", cpuFilter(), monitoringpb.ComparisonType_COMPARISON_GT, 0.9))
	disabled := false
	if err := store.CreateAlertPolicy(ctx, "test", monitoringstore.AlertPolicy{
		ID: "ap-disabled", DisplayName: "off", Combiner: int32(monitoringpb.AlertPolicy_AND),
		Enabled: &disabled, Conditions: []json.RawMessage{b},
	}); err != nil {
		t.Fatal(err)
	}
	putSeries(t, store, "test", "custom.googleapis.com/cpu", "global", nil, 0.99, clock.Now())

	ev.EvaluateAll(ctx)

	if pub.count() != 0 {
		t.Fatalf("disabled policy published %d notifications, want 0", pub.count())
	}
	if got, _ := store.ListIncidents(ctx, "test"); len(got) != 0 {
		t.Fatalf("disabled policy incidents = %+v, want none", got)
	}
}

func TestEvaluatorCrossSeriesReducer(t *testing.T) {
	store, _, ev := newEvalFixture(t)
	ctx := context.Background()
	// Two series: 0.95 and 0.85; REDUCE_MAX with GT 0.9 fires, REDUCE_MIN does not.
	aggMax := &monitoringpb.Aggregation{
		AlignmentPeriod:    durationpb.New(60 * time.Second),
		CrossSeriesReducer: monitoringpb.Aggregation_REDUCE_MAX,
	}
	cond := &monitoringpb.AlertPolicy_Condition{
		DisplayName: "max cpu",
		Condition: &monitoringpb.AlertPolicy_Condition_ConditionThreshold{ConditionThreshold: &monitoringpb.AlertPolicy_Condition_MetricThreshold{
			Filter: cpuFilter(), Aggregations: []*monitoringpb.Aggregation{aggMax},
			Comparison: monitoringpb.ComparisonType_COMPARISON_GT, ThresholdValue: 0.9,
		}},
	}
	putPolicy(t, store, "test", monitoringpb.AlertPolicy_AND, nil, cond)
	putSeries(t, store, "test", "custom.googleapis.com/cpu", "global", map[string]string{"instance": "a"}, 0.95, clock.Now())
	putSeries(t, store, "test", "custom.googleapis.com/cpu", "global", map[string]string{"instance": "b"}, 0.85, clock.Now())

	ev.EvaluateAll(ctx)
	if got, _ := store.ListIncidents(ctx, "test"); len(got) != 1 {
		t.Fatalf("REDUCE_MAX incidents = %+v, want one", got)
	}
}
