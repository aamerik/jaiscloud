package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"jaiscloud/internal/clock"
)

const (
	asyncQueueSize     = 1024
	asyncWorkerCount   = 4
	defaultMaxAttempts = 2
)

// asyncInvokeJob is a single async Lambda invocation queued for processing.
type asyncInvokeJob struct {
	funcARN       string
	payload       []byte
	attempts      int
	maxAttempts   int
	dlqARN        string
	createdAt     time.Time
	maxAgeSeconds int64
}

// asyncQueueSQSSend is the internal SQS send function type used for DLQ delivery.
type asyncQueueSQSSend func(ctx context.Context, arn string, body string) error

// asyncInvoker is the narrow invoker interface used by the async queue.
type asyncInvoker interface {
	InvokeInternal(ctx context.Context, functionName string, payload []byte) ([]byte, error)
}

// AsyncQueue processes async Lambda invocations with retry and DLQ support.
type AsyncQueue struct {
	jobs    chan asyncInvokeJob
	invoker asyncInvoker
	sqsSend asyncQueueSQSSend
	logger  *slog.Logger
	wg      sync.WaitGroup
}

// NewAsyncQueue creates a new AsyncQueue with the given invoker and optional SQS sender for DLQ.
// sqsSend may be nil — if nil, DLQ delivery is skipped.
func NewAsyncQueue(invoker asyncInvoker, sqsSend asyncQueueSQSSend) *AsyncQueue {
	return &AsyncQueue{
		jobs:    make(chan asyncInvokeJob, asyncQueueSize),
		invoker: invoker,
		sqsSend: sqsSend,
		logger:  slog.Default(),
	}
}

// Enqueue adds a job to the queue. If the queue is full the job is dropped.
func (q *AsyncQueue) Enqueue(job asyncInvokeJob) {
	if job.maxAttempts <= 0 {
		job.maxAttempts = defaultMaxAttempts
	}
	if job.createdAt.IsZero() {
		job.createdAt = clock.Now()
	}
	select {
	case q.jobs <- job:
	default:
		q.logger.Warn("lambda: async queue full; dropping invocation", "function", job.funcARN)
	}
}

// Run starts the worker goroutines and blocks until ctx is cancelled.
// Implements the workers.Worker interface (no return value).
func (q *AsyncQueue) Run(ctx context.Context) {
	for i := 0; i < asyncWorkerCount; i++ {
		q.wg.Add(1)
		go q.worker(ctx)
	}
	<-ctx.Done()
	q.wg.Wait()
}

func (q *AsyncQueue) worker(ctx context.Context) {
	defer q.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-q.jobs:
			q.processJob(ctx, job)
		}
	}
}

func (q *AsyncQueue) processJob(ctx context.Context, job asyncInvokeJob) {
	// Check max age.
	if job.maxAgeSeconds > 0 && time.Since(job.createdAt).Seconds() > float64(job.maxAgeSeconds) {
		q.logger.Warn("lambda: async invoke max age exceeded; sending to DLQ", "function", job.funcARN)
		q.sendDLQ(ctx, job, errors.New("MaximumEventAgeInSeconds exceeded"))
		return
	}

	_, err := q.invoker.InvokeInternal(ctx, job.funcARN, job.payload)
	if err == nil {
		return
	}

	job.attempts++
	q.logger.Warn("lambda: async invoke failed", "function", job.funcARN, "attempt", job.attempts, "err", err)

	if job.attempts < job.maxAttempts {
		// Re-enqueue with exponential backoff.
		delay := time.Duration(job.attempts) * time.Second
		go func() {
			select {
			case <-ctx.Done():
			case <-time.After(delay):
				q.Enqueue(job)
			}
		}()
		return
	}

	q.sendDLQ(ctx, job, err)
}

// asyncDLQRecord is the envelope sent to the DLQ.
type asyncDLQRecord struct {
	Version         string                 `json:"version"`
	Timestamp       string                 `json:"timestamp"`
	RequestContext  asyncDLQRequestContext `json:"requestContext"`
	ResponseContext map[string]any         `json:"responseContext"`
	Payload         string                 `json:"payload,omitempty"`
}

type asyncDLQRequestContext struct {
	FunctionARN      string `json:"functionArn"`
	Condition        string `json:"condition"`
	MaxRetryAttempts int    `json:"maxRetryAttempts"`
}

func (q *AsyncQueue) sendDLQ(ctx context.Context, job asyncInvokeJob, invokeErr error) {
	if job.dlqARN == "" || q.sqsSend == nil {
		if invokeErr != nil {
			q.logger.Warn("lambda: async invoke failed, no DLQ configured", "function", job.funcARN, "err", invokeErr)
		}
		return
	}
	rec := asyncDLQRecord{
		Version:   "1.0",
		Timestamp: clock.Now().Format(time.RFC3339),
		RequestContext: asyncDLQRequestContext{
			FunctionARN:      job.funcARN,
			Condition:        "RetriesExhausted",
			MaxRetryAttempts: job.maxAttempts,
		},
		ResponseContext: map[string]any{"statusCode": 0},
		Payload:         string(job.payload),
	}
	body, err := json.Marshal(rec)
	if err != nil {
		q.logger.Error("lambda: failed to marshal async DLQ record", "err", err)
		return
	}
	if err := q.sqsSend(ctx, job.dlqARN, string(body)); err != nil {
		q.logger.Warn("lambda: failed to send async DLQ message", "destination", job.dlqARN, "err", err)
	}
}
