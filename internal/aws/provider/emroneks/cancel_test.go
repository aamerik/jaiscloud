package emroneks

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"jaiscloud/internal/events"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newTestProvider(t *testing.T) (*EMRContainersProvider, *events.EventBus, store.ResourceStore) {
	t.Helper()
	ms := store.NewMemoryResourceStore()
	bus := events.NewEventBus()
	p := New(ms, bus)
	return p, bus, ms
}

func seedVC(t *testing.T, p *EMRContainersProvider) virtualCluster {
	t.Helper()
	vc := virtualCluster{
		ID: "vc-test", Name: "test-vc", State: "RUNNING",
		Namespace: "jaiscloud", ServiceAccountName: "test-sa",
		CreatedAt: time.Now().UTC(),
	}
	data, _ := json.Marshal(vc)
	require.NoError(t, p.resources.Create(context.Background(),
		store.ResourceEntry{Type: rtVirtualCluster, ID: vc.ID, Data: data}))
	return vc
}

func seedJobRun(t *testing.T, p *EMRContainersProvider, vcID, state string) jobRun {
	t.Helper()
	jr := jobRun{
		ID: shortID(), Name: "test-job", VirtualClusterID: vcID,
		State: state, CreatedAt: time.Now().UTC(),
	}
	data, _ := json.Marshal(jr)
	require.NoError(t, p.resources.Create(context.Background(),
		store.ResourceEntry{Type: rtJobRun, ID: vcID + "/" + jr.ID, Data: data}))
	return jr
}

// subscribeJobRunStates returns a channel that receives each state string
// emitted on the bus for EventEMRJobRunState events.
func subscribeJobRunStates(bus *events.EventBus) <-chan string {
	ch := make(chan string, 32)
	bus.Subscribe(events.EventEMRJobRunState, func(e events.Event) {
		ev := e.Payload.(events.EMRJobRunStateEvent)
		ch <- ev.State
	})
	return ch
}

func makeNR(vcID, jobID string) *model.NormalizedRequest {
	return &model.NormalizedRequest{
		Cloud:     model.CloudAWS,
		Region:    "us-east-1",
		AccountID: "000000000000",
		Params: map[string]any{
			"_path_virtualClusterId": vcID,
			"_path_jobRunId":         jobID,
		},
		ResourceID: func(_, name string) string { return "arn:aws:emr-containers:::jobruns/" + name },
	}
}

func TestCancelJobRun_FromRunning_DirectTransition(t *testing.T) {
	p, bus, _ := newTestProvider(t)
	seedVC(t, p)
	jr := seedJobRun(t, p, "vc-test", "RUNNING")
	ch := subscribeJobRunStates(bus)

	resp, err := p.CancelJobRun(context.Background(), makeNR("vc-test", jr.ID))
	require.NoError(t, err)
	assert.Equal(t, jr.ID, resp.Data["id"])

	// Exactly one CANCELLED event, no CANCEL_PENDING.
	select {
	case state := <-ch:
		assert.Equal(t, "CANCELLED", state)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for CANCELLED event")
	}
	select {
	case state := <-ch:
		t.Errorf("unexpected extra event: %s", state)
	default:
	}

	loaded, err := p.loadJobRun(context.Background(), "vc-test", jr.ID)
	require.NoError(t, err)
	assert.Equal(t, "CANCELLED", loaded.State)
}

func TestCancelJobRun_FromPending_TwoPhase(t *testing.T) {
	p, bus, _ := newTestProvider(t)
	seedVC(t, p)
	jr := seedJobRun(t, p, "vc-test", "PENDING")
	ch := subscribeJobRunStates(bus)

	resp, err := p.CancelJobRun(context.Background(), makeNR("vc-test", jr.ID))
	require.NoError(t, err)
	assert.Equal(t, jr.ID, resp.Data["id"])

	// Must receive CANCEL_PENDING first, then CANCELLED.
	var states []string
	deadline := time.After(2 * time.Second)
	for len(states) < 2 {
		select {
		case s := <-ch:
			states = append(states, s)
		case <-deadline:
			t.Fatalf("timed out; got states so far: %v", states)
		}
	}
	assert.Equal(t, []string{"CANCEL_PENDING", "CANCELLED"}, states)

	loaded, err := p.loadJobRun(context.Background(), "vc-test", jr.ID)
	require.NoError(t, err)
	assert.Equal(t, "CANCELLED", loaded.State)
}

func TestCancelJobRun_AlreadyTerminal(t *testing.T) {
	for _, state := range []string{"COMPLETED", "FAILED", "CANCELLED"} {
		t.Run(state, func(t *testing.T) {
			p, _, _ := newTestProvider(t)
			seedVC(t, p)
			jr := seedJobRun(t, p, "vc-test", state)
			_, err := p.CancelJobRun(context.Background(), makeNR("vc-test", jr.ID))
			require.Error(t, err)
			var pe *model.ProviderError
			require.ErrorAs(t, err, &pe)
			assert.Equal(t, "ValidationException", pe.Code)
		})
	}
}

func TestCancelJobRun_DuringCancelPending_Idempotent(t *testing.T) {
	p, bus, _ := newTestProvider(t)
	seedVC(t, p)
	jr := seedJobRun(t, p, "vc-test", "CANCEL_PENDING")

	var count atomic.Int32
	bus.Subscribe(events.EventEMRJobRunState, func(e events.Event) {
		count.Add(1)
	})

	resp, err := p.CancelJobRun(context.Background(), makeNR("vc-test", jr.ID))
	require.NoError(t, err)
	assert.Equal(t, jr.ID, resp.Data["id"])
	assert.Equal(t, int32(0), count.Load(), "no new bus events for idempotent cancel")
}

func TestCancelJobRun_NotFound(t *testing.T) {
	p, _, _ := newTestProvider(t)
	seedVC(t, p)
	_, err := p.CancelJobRun(context.Background(), makeNR("vc-test", "no-such-job"))
	require.Error(t, err)
	var pe *model.ProviderError
	require.ErrorAs(t, err, &pe)
	assert.Equal(t, "ResourceNotFoundException", pe.Code)
}

func TestCancelJobRun_ConcurrentCancels_SingleTerminal(t *testing.T) {
	p, bus, _ := newTestProvider(t)
	seedVC(t, p)
	jr := seedJobRun(t, p, "vc-test", "PENDING")

	var cancelPendingCount, cancelledCount atomic.Int32
	bus.Subscribe(events.EventEMRJobRunState, func(e events.Event) {
		ev := e.Payload.(events.EMRJobRunStateEvent)
		switch ev.State {
		case "CANCEL_PENDING":
			cancelPendingCount.Add(1)
		case "CANCELLED":
			cancelledCount.Add(1)
		}
	})

	const N = 10
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			p.CancelJobRun(context.Background(), makeNR("vc-test", jr.ID)) //nolint:errcheck
		}()
	}
	wg.Wait()

	// Drain async goroutines spawned by PENDING-path cancels.
	time.Sleep(200 * time.Millisecond)

	// At most one CANCEL_PENDING and one CANCELLED (subsequent calls hit
	// CANCEL_PENDING idempotent path or terminal ValidationException).
	assert.LessOrEqual(t, cancelPendingCount.Load(), int32(1))
	assert.LessOrEqual(t, cancelledCount.Load(), int32(1))
}
