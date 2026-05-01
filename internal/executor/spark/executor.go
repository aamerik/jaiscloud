// Package spark defines the SparkExecutor interface and related types for
// running Spark jobs in Kubernetes or mock environments.
package spark

import (
	"context"
	"fmt"

	"jaiscloud/internal/k8stypes"
)

// SparkState mirrors EMR step/job-run states.
type SparkState string

const (
	StatePending   SparkState = "PENDING"
	StateRunning   SparkState = "RUNNING"
	StateCompleted SparkState = "COMPLETED"
	StateFailed    SparkState = "FAILED"
	StateCancelled SparkState = "CANCELLED"
)

// IsTerminal returns true if the state is a terminal (non-polling) state.
func (s SparkState) IsTerminal() bool {
	switch s {
	case StateCompleted, StateFailed, StateCancelled:
		return true
	}
	return false
}

// SparkJob is the input to SparkExecutor.Submit.
type SparkJob struct {
	// JobID is the caller-assigned identifier (e.g. EMR step ID, job run ID).
	JobID string

	// MainClass is the fully-qualified Spark application main class.
	// Mutually exclusive with JarURI for Python apps.
	MainClass string

	// JarURI is the path/URI to the application JAR or Python script.
	JarURI string

	// Args are positional arguments passed to the Spark application.
	Args []string

	// SparkConf contains Spark configuration key/value pairs (--conf key=value).
	SparkConf map[string]string

	// Config is the resolved executor configuration for this job.
	Config SparkConfig

	// Bootstrap fragments — set by the EMR provider after resolving bootstrap
	// actions. Executor reads these and injects them into the batch Job manifest.
	// All nil when no bootstrap actions are configured.
	ExtraInitContainers []k8stypes.Container
	ExtraVolumes        []k8stypes.Volume
	ExtraMainMounts     []k8stypes.VolumeMount

	// AllowClusterMode enables Spark cluster-deploy-mode (driver runs as K8s Pod).
	// When false (default), spark-submit args are left as-is for mock mode.
	AllowClusterMode bool
}

// SparkStatus is the current status of a submitted job.
type SparkStatus struct {
	JobID   string
	State   SparkState
	Message string // human-readable detail (error message, exit code, etc.)
}

// SparkExecutor submits and queries Spark jobs in one specific environment.
type SparkExecutor interface {
	// Submit launches a Spark job. Returns an error if submission fails.
	// The job may still fail asynchronously; poll via Status.
	Submit(ctx context.Context, job SparkJob) error

	// Status returns the current state of a previously submitted job.
	// Returns an error if the job ID is unknown to this executor.
	Status(ctx context.Context, jobID string) (SparkStatus, error)

	// Cancel attempts to terminate a running job.
	// Returns nil if the job is already in a terminal state.
	Cancel(ctx context.Context, jobID string) error

	// Close releases any resources held by the executor (connections, goroutines).
	Close() error
}

// NewExecutor creates a SparkExecutor for the given mode.
// Supported modes: "" / "mock" → MockExecutor; "k8s" → K8sExecutor.
// Any other mode returns an error — Docker, local, and remote Spark executor
// modes have been removed; only k8s and mock are supported.
func NewExecutor(mode string, cfg SparkConfig) (SparkExecutor, error) {
	switch mode {
	case "", "mock":
		return NewMockExecutor(), nil
	case "k8s":
		return NewK8sExecutor(cfg, nil), nil
	default:
		return nil, fmt.Errorf("unknown Spark executor mode %q (allowed: mock, k8s)", mode)
	}
}
