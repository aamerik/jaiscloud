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
//  1. job.SparkConf["spark.kubernetes.container.image"] (per-job override)
//  2. cfg.Image passed to NewDockerExecutor (executor-level default)
//  3. DefaultImage
//
// When S3Endpoint is set, AWS credential env vars and --network host are injected
// so spark-submit can reach JaisCloud's S3 endpoint from inside the container.
type DockerExecutor struct {
	cfg  SparkConfig
	jobs sync.Map // jobID → *dockerJob
}

// NewDockerExecutor creates a DockerExecutor. cfg.Image is the default Spark image.
func NewDockerExecutor(cfg SparkConfig) *DockerExecutor {
	if cfg.Image == "" {
		cfg.Image = DefaultImage
	}
	return &DockerExecutor{cfg: cfg}
}

// Submit launches a Docker container running spark-submit asynchronously.
func (e *DockerExecutor) Submit(ctx context.Context, job SparkJob) error {
	resolved := AWSResolveSparkCommand(job, e.cfg)

	dj := &dockerJob{state: StateRunning}
	runCtx, cancel := context.WithCancel(context.Background())
	dj.cancel = cancel
	e.jobs.Store(job.JobID, dj)

	go func() {
		defer cancel()

		dockerArgs := []string{"run", "--rm", "--network", "host"}
		if e.cfg.S3Endpoint != "" {
			dockerArgs = append(dockerArgs,
				"-e", "AWS_ENDPOINT_URL="+e.cfg.S3Endpoint,
				"-e", "AWS_REGION="+e.cfg.Region,
				"-e", "AWS_ACCESS_KEY_ID="+e.cfg.AWSAccessKey,
				"-e", "AWS_SECRET_ACCESS_KEY="+e.cfg.AWSSecretKey,
			)
		}
		// Pass binary + args after image name (overrides CMD in container).
		dockerArgs = append(dockerArgs, resolved.Image, resolved.Binary)
		dockerArgs = append(dockerArgs, resolved.Args...)

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
