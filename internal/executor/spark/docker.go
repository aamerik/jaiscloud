package spark

import (
	"bytes"
	"context"
	"os/exec"
	"sync"
)

// dockerJob tracks a running Docker container's state.
type dockerJob struct {
	mu     sync.Mutex
	state  SparkState
	msg    string
	cancel context.CancelFunc
}

// DockerExecutor runs spark-submit inside a Docker container (docker run --rm).
// It requires the Docker daemon to be running and the specified image to be
// available locally or pullable.
//
// The image is resolved in this order:
//  1. job.Config.Image (per-job override)
//  2. cfg.Image passed to NewDockerExecutor (executor-level default)
//  3. DefaultImage
//
// When job.Args is non-empty, it is passed directly as the container command
// (EMR command-runner.jar style where Args = ["spark-submit", ...]). Otherwise
// SparkSubmitArgs is used.
type DockerExecutor struct {
	image string
	jobs  sync.Map // jobID → *dockerJob
}

// NewDockerExecutor creates a DockerExecutor. cfg.Image is the default Spark image.
func NewDockerExecutor(cfg SparkConfig) *DockerExecutor {
	image := cfg.Image
	if image == "" {
		image = DefaultImage
	}
	return &DockerExecutor{image: image}
}

// Submit launches a Docker container running spark-submit asynchronously.
func (e *DockerExecutor) Submit(ctx context.Context, job SparkJob) error {
	image := job.Config.Image
	if image == "" {
		image = e.image
	}

	dj := &dockerJob{state: StateRunning}
	runCtx, cancel := context.WithCancel(context.Background())
	dj.cancel = cancel
	e.jobs.Store(job.JobID, dj)

	go func() {
		defer cancel()
		dockerArgs := append([]string{"run", "--rm", image}, e.containerArgs(job)...)
		var stderr bytes.Buffer
		cmd := exec.CommandContext(runCtx, "docker", dockerArgs...)
		cmd.Stderr = &stderr

		err := cmd.Run()

		dj.mu.Lock()
		defer dj.mu.Unlock()
		if dj.state == StateCancelled {
			return // Cancel() already set terminal state
		}
		if err != nil {
			dj.state = StateFailed
			dj.msg = stderr.String()
			if dj.msg == "" {
				dj.msg = err.Error()
			}
		} else {
			dj.state = StateCompleted
		}
	}()
	return nil
}

// containerArgs returns the arguments to pass inside the container.
// EMR uses command-runner.jar where job.Args = ["spark-submit", ...] — use directly.
// For jobs built via BuildSparkJob with a real JarURI, delegate to SparkSubmitArgs.
func (e *DockerExecutor) containerArgs(job SparkJob) []string {
	if len(job.Args) > 0 {
		return job.Args
	}
	return append([]string{"/opt/spark/bin/spark-submit"}, SparkSubmitArgs(job)...)
}

// Status returns the current state of a submitted job.
func (e *DockerExecutor) Status(_ context.Context, jobID string) (SparkStatus, error) {
	v, ok := e.jobs.Load(jobID)
	if !ok {
		return SparkStatus{JobID: jobID, State: StatePending}, nil
	}
	dj := v.(*dockerJob)
	dj.mu.Lock()
	defer dj.mu.Unlock()
	return SparkStatus{JobID: jobID, State: dj.state, Message: dj.msg}, nil
}

// Cancel stops the running Docker container by cancelling its context.
func (e *DockerExecutor) Cancel(_ context.Context, jobID string) error {
	v, ok := e.jobs.Load(jobID)
	if !ok {
		return nil
	}
	dj := v.(*dockerJob)
	dj.mu.Lock()
	if !dj.state.IsTerminal() {
		dj.state = StateCancelled
	}
	dj.mu.Unlock()
	dj.cancel()
	return nil
}

// Close is a no-op; Docker containers are self-contained processes.
func (e *DockerExecutor) Close() error { return nil }

// Reset cancels all running containers and clears tracked state.
// Already-terminal jobs are not affected beyond being removed from the map.
func (e *DockerExecutor) Reset() {
	e.jobs.Range(func(k, v any) bool {
		dj := v.(*dockerJob)
		dj.mu.Lock()
		if !dj.state.IsTerminal() {
			dj.state = StateCancelled
		}
		dj.mu.Unlock()
		dj.cancel()
		e.jobs.Delete(k)
		return true
	})
}
