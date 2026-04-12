package spark_test

import (
	"context"
	"testing"
	"time"

	"github.com/jaiscloud/plugin-aws-emr-spark/internal/executor/spark"
)

func TestMockExecutor_Submit_Status_Complete(t *testing.T) {
	m := spark.NewMockExecutor() // immediate completion

	job := spark.SparkJob{JobID: "j1", JarURI: "s3://bucket/app.jar"}
	if err := m.Submit(context.Background(), job); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	status, err := m.Status(context.Background(), "j1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.State != spark.StateCompleted {
		t.Errorf("expected COMPLETED, got %s", status.State)
	}
}

func TestMockExecutor_Status_UnknownJob(t *testing.T) {
	m := spark.NewMockExecutor()
	_, err := m.Status(context.Background(), "unknown")
	if err == nil {
		t.Fatal("expected error for unknown job")
	}
}

func TestMockExecutor_Cancel(t *testing.T) {
	m := spark.NewMockExecutorWithDelay(10 * time.Minute) // never completes on its own
	m.Submit(context.Background(), spark.SparkJob{JobID: "j1"})

	if err := m.Cancel(context.Background(), "j1"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	status, _ := m.Status(context.Background(), "j1")
	if status.State != spark.StateCancelled {
		t.Errorf("expected CANCELLED, got %s", status.State)
	}
}

func TestMockExecutor_Cancel_NonExistent_NoOp(t *testing.T) {
	m := spark.NewMockExecutor()
	if err := m.Cancel(context.Background(), "does-not-exist"); err != nil {
		t.Fatalf("Cancel on nonexistent should be no-op: %v", err)
	}
}

func TestMockExecutor_Delay(t *testing.T) {
	m := spark.NewMockExecutorWithDelay(50 * time.Millisecond)
	m.Submit(context.Background(), spark.SparkJob{JobID: "j1"})

	// Should still be RUNNING before delay expires
	status, _ := m.Status(context.Background(), "j1")
	if status.State != spark.StateRunning {
		t.Errorf("expected RUNNING before delay, got %s", status.State)
	}

	time.Sleep(100 * time.Millisecond)

	status, _ = m.Status(context.Background(), "j1")
	if status.State != spark.StateCompleted {
		t.Errorf("expected COMPLETED after delay, got %s", status.State)
	}
}

func TestMockExecutor_Reset(t *testing.T) {
	m := spark.NewMockExecutorWithDelay(10 * time.Minute)
	m.Submit(context.Background(), spark.SparkJob{JobID: "j1"})
	m.Reset()

	_, err := m.Status(context.Background(), "j1")
	if err == nil {
		t.Fatal("expected error after Reset (job cleared)")
	}
}

func TestMockExecutor_ForceState(t *testing.T) {
	m := spark.NewMockExecutorWithDelay(10 * time.Minute)
	m.Submit(context.Background(), spark.SparkJob{JobID: "j1"})
	m.ForceState("j1", spark.StateFailed)

	status, _ := m.Status(context.Background(), "j1")
	if status.State != spark.StateFailed {
		t.Errorf("expected FAILED, got %s", status.State)
	}
}

func TestMockExecutor_Close(t *testing.T) {
	m := spark.NewMockExecutor()
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestSparkState_IsTerminal(t *testing.T) {
	terminal := []spark.SparkState{spark.StateCompleted, spark.StateFailed, spark.StateCancelled}
	for _, s := range terminal {
		if !s.IsTerminal() {
			t.Errorf("%s should be terminal", s)
		}
	}
	nonTerminal := []spark.SparkState{spark.StatePending, spark.StateRunning}
	for _, s := range nonTerminal {
		if s.IsTerminal() {
			t.Errorf("%s should not be terminal", s)
		}
	}
}
