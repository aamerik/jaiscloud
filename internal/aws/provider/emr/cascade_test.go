package emr

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"jaiscloud/internal/events"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// seedClusterWithSteps creates a cluster containing steps at the given states
// and returns the cluster ID and step IDs in the same order.
func seedClusterWithSteps(t *testing.T, p *EMRProvider, stepStates []string) (clusterID string, stepIDs []string) {
	t.Helper()
	clusterID = "j-TEST" + randID("", 8)
	steps := make([]map[string]any, len(stepStates))
	stepIDs = make([]string, len(stepStates))
	for i, state := range stepStates {
		sid := "s-" + randID("", 8)
		stepIDs[i] = sid
		steps[i] = map[string]any{
			"Id":              sid,
			"Name":            "step-" + sid,
			"ActionOnFailure": "CONTINUE",
			"Status": map[string]any{
				"State":             state,
				"StateChangeReason": map[string]any{},
				"Timeline":          map[string]any{},
			},
		}
	}
	c := emrCluster{
		Id:   clusterID,
		Name: "test-cluster",
		Status: clusterStatus{
			State:             "RUNNING",
			StateChangeReason: map[string]any{},
			Timeline:          map[string]any{},
		},
		Steps: steps,
	}
	data, _ := json.Marshal(c)
	require.NoError(t, p.resources.Create(context.Background(), "000000000000", "us-east-1",
		store.ResourceEntry{Type: rtCluster, ID: clusterID, Data: data}))
	return clusterID, stepIDs
}

func newTestProvider(t *testing.T) (*EMRProvider, *events.EventBus, store.ResourceStore) {
	t.Helper()
	ms := store.NewMemoryResourceStore()
	bus := events.NewEventBus()
	p := New(ms, bus)
	return p, bus, ms
}

// collectBusEvents subscribes to EMR step and cluster state events and
// returns a slice of (type, state, id) triples accumulated during f().
type busEvent struct {
	evType string // "step" or "cluster"
	state  string
	id     string // stepID or clusterID
}

func collectBusEvents(bus *events.EventBus, f func()) []busEvent {
	var collected []busEvent
	doneCh := make(chan struct{})

	bus.Subscribe(events.EventEMRStepState, func(e events.Event) {
		ev := e.Payload.(events.EMRStepStateEvent)
		collected = append(collected, busEvent{evType: "step", state: ev.State, id: ev.StepID})
	})
	bus.Subscribe(events.EventEMRClusterState, func(e events.Event) {
		ev := e.Payload.(events.EMRClusterStateEvent)
		collected = append(collected, busEvent{evType: "cluster", state: ev.State, id: ev.ClusterID})
	})

	f()
	close(doneCh)
	return collected
}

func TestCascade_TerminateCluster(t *testing.T) {
	p, bus, _ := newTestProvider(t)
	h := handlerCtx{cloud: model.CloudAWS, region: "us-east-1", accountID: "000000000000"}

	// step0 = failing (RUNNING→FAILED by caller), step1 = PENDING, step2 = RUNNING
	clusterID, stepIDs := seedClusterWithSteps(t, p, []string{"RUNNING", "PENDING", "RUNNING"})
	failedStep := stepIDs[0]

	evts := collectBusEvents(bus, func() {
		p.cascadeOnStepFailure(context.Background(), h, clusterID, failedStep, "TERMINATE_CLUSTER")
	})

	// Expect INTERRUPTED for step1 and step2
	interruptedIDs := map[string]bool{}
	clusterStates := []string{}
	for _, ev := range evts {
		if ev.evType == "step" && ev.state == "INTERRUPTED" {
			interruptedIDs[ev.id] = true
		}
		if ev.evType == "cluster" {
			clusterStates = append(clusterStates, ev.state)
		}
	}
	assert.True(t, interruptedIDs[stepIDs[1]], "step1 PENDING should become INTERRUPTED")
	assert.True(t, interruptedIDs[stepIDs[2]], "step2 RUNNING should become INTERRUPTED")
	assert.False(t, interruptedIDs[failedStep], "failing step must not be re-emitted")

	assert.Equal(t, []string{"TERMINATING", "TERMINATED_WITH_ERRORS"}, clusterStates)

	// Verify store reflects TERMINATED_WITH_ERRORS and INTERRUPTED step states
	c, err := p.loadCluster(context.Background(), h.accountID, h.region, clusterID)
	require.NoError(t, err)
	assert.Equal(t, "TERMINATED_WITH_ERRORS", c.Status.State, "cluster store state")
	for _, step := range c.Steps {
		sid, _ := step["Id"].(string)
		if sid == failedStep {
			continue
		}
		status, _ := step["Status"].(map[string]any)
		state, _ := status["State"].(string)
		assert.Equal(t, "INTERRUPTED", state, "step %s should be INTERRUPTED in store", sid)
	}
}

func TestCascade_CancelAndWait(t *testing.T) {
	p, bus, _ := newTestProvider(t)
	h := handlerCtx{cloud: model.CloudAWS, region: "us-east-1", accountID: "000000000000"}

	// step0 = failing, step1 = PENDING, step2 = RUNNING
	clusterID, stepIDs := seedClusterWithSteps(t, p, []string{"RUNNING", "PENDING", "RUNNING"})
	failedStep := stepIDs[0]

	evts := collectBusEvents(bus, func() {
		p.cascadeOnStepFailure(context.Background(), h, clusterID, failedStep, "CANCEL_AND_WAIT")
	})

	cancelledIDs := map[string]bool{}
	var clusterStatesSeen []string
	for _, ev := range evts {
		if ev.evType == "step" && ev.state == "CANCELLED" {
			cancelledIDs[ev.id] = true
		}
		if ev.evType == "cluster" {
			clusterStatesSeen = append(clusterStatesSeen, ev.state)
		}
	}
	assert.True(t, cancelledIDs[stepIDs[1]], "PENDING step should become CANCELLED")
	assert.False(t, cancelledIDs[stepIDs[2]], "RUNNING step must not be cancelled")
	assert.Empty(t, clusterStatesSeen, "cluster state must not change for CANCEL_AND_WAIT")

	// Verify store: PENDING step should now be CANCELLED
	c, err := p.loadCluster(context.Background(), h.accountID, h.region, clusterID)
	require.NoError(t, err)
	assert.Equal(t, "RUNNING", c.Status.State, "cluster status unchanged")
	for _, step := range c.Steps {
		sid, _ := step["Id"].(string)
		status, _ := step["Status"].(map[string]any)
		state, _ := status["State"].(string)
		if sid == stepIDs[1] {
			assert.Equal(t, "CANCELLED", state, "PENDING step cancelled in store")
		}
		if sid == stepIDs[2] {
			assert.Equal(t, "RUNNING", state, "RUNNING step unchanged in store")
		}
	}
}

func TestCascade_Continue(t *testing.T) {
	p, bus, _ := newTestProvider(t)
	h := handlerCtx{cloud: model.CloudAWS, region: "us-east-1", accountID: "000000000000"}

	clusterID, stepIDs := seedClusterWithSteps(t, p, []string{"RUNNING", "PENDING"})
	failedStep := stepIDs[0]

	evts := collectBusEvents(bus, func() {
		p.cascadeOnStepFailure(context.Background(), h, clusterID, failedStep, "CONTINUE")
	})

	assert.Empty(t, evts, "CONTINUE must produce no bus events")

	c, err := p.loadCluster(context.Background(), h.accountID, h.region, clusterID)
	require.NoError(t, err)
	for _, step := range c.Steps {
		sid, _ := step["Id"].(string)
		if sid == stepIDs[1] {
			status, _ := step["Status"].(map[string]any)
			state, _ := status["State"].(string)
			assert.Equal(t, "PENDING", state, "PENDING step unchanged for CONTINUE")
		}
	}
	assert.Equal(t, "RUNNING", c.Status.State, "cluster state unchanged")
}

func TestCascade_TerminateCluster_WithTerminalSteps(t *testing.T) {
	p, bus, _ := newTestProvider(t)
	h := handlerCtx{cloud: model.CloudAWS, region: "us-east-1", accountID: "000000000000"}

	// step0 = failing, step1 = COMPLETED (terminal), step2 = CANCELLED (terminal), step3 = PENDING
	clusterID, stepIDs := seedClusterWithSteps(t, p, []string{"RUNNING", "COMPLETED", "CANCELLED", "PENDING"})
	failedStep := stepIDs[0]

	evts := collectBusEvents(bus, func() {
		p.cascadeOnStepFailure(context.Background(), h, clusterID, failedStep, "TERMINATE_CLUSTER")
	})

	interruptedIDs := map[string]bool{}
	for _, ev := range evts {
		if ev.evType == "step" && ev.state == "INTERRUPTED" {
			interruptedIDs[ev.id] = true
		}
	}
	// Only PENDING → INTERRUPTED; terminal steps untouched
	assert.True(t, interruptedIDs[stepIDs[3]], "PENDING step should be INTERRUPTED")
	assert.False(t, interruptedIDs[stepIDs[1]], "COMPLETED step must not be touched")
	assert.False(t, interruptedIDs[stepIDs[2]], "CANCELLED step must not be touched")

	// Verify store
	c, err := p.loadCluster(context.Background(), h.accountID, h.region, clusterID)
	require.NoError(t, err)
	for _, step := range c.Steps {
		sid, _ := step["Id"].(string)
		status, _ := step["Status"].(map[string]any)
		state, _ := status["State"].(string)
		switch sid {
		case stepIDs[1]:
			assert.Equal(t, "COMPLETED", state)
		case stepIDs[2]:
			assert.Equal(t, "CANCELLED", state)
		case stepIDs[3]:
			assert.Equal(t, "INTERRUPTED", state)
		}
	}
}
