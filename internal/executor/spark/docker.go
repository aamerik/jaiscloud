package spark

import (
	"bytes"
	"context"
	"log/slog"
	"os/exec"
	"sync"

	"jaiscloud/internal/platform"
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
type DockerExecutor struct {
	cfg      SparkConfig
	platform *platform.PlatformConfig
	jobs     sync.Map // jobID → *dockerJob
}

// NewDockerExecutor creates a DockerExecutor. plat may be nil.
func NewDockerExecutor(cfg SparkConfig, plat *platform.PlatformConfig) *DockerExecutor {
	if cfg.Image == "" {
		cfg.Image = DefaultImage
	}
	return &DockerExecutor{cfg: cfg, platform: plat}
}

// Submit launches a Docker container running spark-submit asynchronously.
func (e *DockerExecutor) Submit(ctx context.Context, job SparkJob) error {
	transform, err := selectTransform(e.cfg.Cloud)
	if err != nil {
		return err
	}
	resolved, err := transform.ResolveCommand(job, e.cfg)
	if err != nil {
		return err
	}

	dj := &dockerJob{state: StateRunning}
	runCtx, cancel := context.WithCancel(context.Background())
	dj.cancel = cancel
	e.jobs.Store(job.JobID, dj)

	go func() {
		defer cancel()

		dockerArgs := []string{"run", "--rm", "--network", "host"}

		// Cloud-specific env vars.
		for _, ev := range transform.PodEnv(e.cfg) {
			dockerArgs = append(dockerArgs, "-e", ev.Name+"="+ev.Value)
		}

		// Platform layer: TLS PEM bundle + extra volumes + extra env.
		if e.platform != nil {
			volArgs, envArgs, err := platform.ApplyDocker(e.platform)
			if err != nil {
				slog.Warn("spark docker: platform apply failed", "err", err)
			}
			dockerArgs = append(dockerArgs, volArgs...)
			dockerArgs = append(dockerArgs, envArgs...)
		}

		dockerArgs = append(dockerArgs, resolved.Image, resolved.Binary)
		dockerArgs = append(dockerArgs, resolved.Args...)

		var stderr bytes.Buffer
		cmd := exec.CommandContext(runCtx, "docker", dockerArgs...)
		cmd.Stderr = &stderr

		err := cmd.Run()

		dj.mu.Lock()
		defer dj.mu.Unlock()
		if dj.state == StateCancelled {
			return
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
