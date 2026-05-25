package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	sqsstore "jaiscloud/internal/aws/store/sqs"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// messageMoveTask tracks a single in-progress or completed move task.
type messageMoveTask struct {
	taskHandle         string
	sourceArn          string
	destinationArn     string
	maxNumberPerSecond int
	startedAt          time.Time
	status             string // RUNNING, COMPLETED, CANCELLING, CANCELLED, FAILED
	messagesMoved      int
	approxMessageCount int
	cancelFn           context.CancelFunc
}

// moveTasks holds all message move tasks indexed by task handle.
type moveTasks struct {
	mu    sync.Mutex
	tasks map[string]*messageMoveTask
}

func newMoveTasks() *moveTasks {
	return &moveTasks{tasks: make(map[string]*messageMoveTask)}
}

// getOrInit lazily initialises the move tasks registry on QueueProvider.
// Because QueueProvider was constructed before this file existed, we store the
// registry in the provider's existing sync.Map via a well-known key in
// resources, or fall back to a package-level map protected by a mutex.
// For simplicity we keep a package-level registry; it is reset on provider Reset().
var (
	globalMoveTasks   = newMoveTasks()
	globalMoveTasksMu sync.Mutex
)

func resetMoveTasks() {
	globalMoveTasksMu.Lock()
	defer globalMoveTasksMu.Unlock()
	for _, t := range globalMoveTasks.tasks {
		if t.cancelFn != nil {
			t.cancelFn()
		}
	}
	globalMoveTasks = newMoveTasks()
}

// StartMessageMoveTask implements Queue.StartMessageMoveTask.
func (p *QueueProvider) StartMessageMoveTask(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	sourceArn, _ := nr.Params["SourceArn"].(string)
	if sourceArn == "" {
		return nil, model.NewProviderError("InvalidParameterValue", "SourceArn is required", 400)
	}
	destinationArn, _ := nr.Params["DestinationArn"].(string)
	maxPerSec := 300
	if v, ok := nr.Params["MaxNumberOfMessagesPerSecond"].(float64); ok && v > 0 {
		maxPerSec = int(v)
	}

	// Validate source queue exists and resolve its scope
	sourceURL, sourceAccount, sourceRegion, err := p.resolveQueueURLWithScope(ctx, sourceArn)
	if err != nil {
		return nil, model.NewProviderError("QueueDoesNotExist", "The specified source queue does not exist", 400)
	}

	// Check that no task is already running for this source
	globalMoveTasks.mu.Lock()
	for _, t := range globalMoveTasks.tasks {
		if t.sourceArn == sourceArn && t.status == "RUNNING" {
			globalMoveTasks.mu.Unlock()
			return nil, model.NewProviderError("InvalidParameterValue",
				"A message move task is already running for this source queue", 400)
		}
	}

	taskHandle := fmt.Sprintf("mmt-%s", newMessageID())
	taskCtx, cancel := context.WithCancel(context.Background())
	task := &messageMoveTask{
		taskHandle:         taskHandle,
		sourceArn:          sourceArn,
		destinationArn:     destinationArn,
		maxNumberPerSecond: maxPerSec,
		startedAt:          clock.Now(),
		status:             "RUNNING",
		cancelFn:           cancel,
	}
	globalMoveTasks.tasks[taskHandle] = task
	globalMoveTasks.mu.Unlock()

	// Resolve destination URL and scope
	var destURL, destAccount, destRegion string
	if destinationArn != "" {
		destURL, destAccount, destRegion, err = p.resolveQueueURLWithScope(ctx, destinationArn)
		if err != nil {
			return nil, model.NewProviderError("QueueDoesNotExist", "The specified destination queue does not exist", 400)
		}
	}

	go p.runMoveTask(taskCtx, task, sourceURL, sourceAccount, sourceRegion, destURL, destAccount, destRegion)

	return provider.OK(map[string]any{
		"TaskHandle": taskHandle,
	}), nil
}

func (p *QueueProvider) runMoveTask(ctx context.Context, task *messageMoveTask, sourceURL, sourceAccount, sourceRegion, destURL, destAccount, destRegion string) {
	interval := time.Second / time.Duration(task.maxNumberPerSecond)
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			globalMoveTasks.mu.Lock()
			task.status = "CANCELLED"
			task.cancelFn = nil
			globalMoveTasks.mu.Unlock()
			return
		case <-ticker.C:
			msgs, err := p.messages.Receive(ctx, sourceAccount, sourceRegion, sourceURL, 1, clock.Now())
			if err != nil || len(msgs) == 0 {
				globalMoveTasks.mu.Lock()
				task.status = "COMPLETED"
				task.cancelFn = nil
				globalMoveTasks.mu.Unlock()
				return
			}
			msg := msgs[0]
			dst := destURL
			dstAccount := destAccount
			dstRegion := destRegion
			if dst == "" {
				dst = sourceURL
				dstAccount = sourceAccount
				dstRegion = sourceRegion
			}
			moved := sqsstore.SQSMessage{
				MessageID:         msg.MessageID,
				QueueURL:          dst,
				Body:              msg.Body,
				Attributes:        msg.Attributes,
				MessageAttributes: msg.MessageAttributes,
				ReceiptHandle:     "",
				VisibleAt:         time.Time{},
				DelayUntil:        time.Time{},
			}
			p.messages.Send(ctx, dstAccount, dstRegion, moved)                                //nolint:errcheck
			p.messages.Delete(ctx, sourceAccount, sourceRegion, sourceURL, msg.ReceiptHandle) //nolint:errcheck
			globalMoveTasks.mu.Lock()
			task.messagesMoved++
			globalMoveTasks.mu.Unlock()
		}
	}
}

// CancelMessageMoveTask implements Queue.CancelMessageMoveTask.
func (p *QueueProvider) CancelMessageMoveTask(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	taskHandle, _ := nr.Params["TaskHandle"].(string)
	if taskHandle == "" {
		return nil, model.NewProviderError("InvalidParameterValue", "TaskHandle is required", 400)
	}

	globalMoveTasks.mu.Lock()
	task, ok := globalMoveTasks.tasks[taskHandle]
	if !ok {
		globalMoveTasks.mu.Unlock()
		return nil, model.NewProviderError("ResourceNotFoundException",
			"The specified task does not exist", 400)
	}
	if task.status != "RUNNING" {
		globalMoveTasks.mu.Unlock()
		return nil, model.NewProviderError("InvalidParameterValue",
			fmt.Sprintf("Task is in %s state and cannot be cancelled", task.status), 400)
	}
	task.status = "CANCELLING"
	cancelFn := task.cancelFn
	globalMoveTasks.mu.Unlock()

	if cancelFn != nil {
		cancelFn()
	}

	return provider.OK(map[string]any{
		"ApproximateNumberOfMessagesMoved": task.messagesMoved,
	}), nil
}

// ListMessageMoveTasks implements Queue.ListMessageMoveTasks.
func (p *QueueProvider) ListMessageMoveTasks(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	sourceArn, _ := nr.Params["SourceArn"].(string)

	globalMoveTasks.mu.Lock()
	defer globalMoveTasks.mu.Unlock()

	var results []map[string]any
	for _, t := range globalMoveTasks.tasks {
		if sourceArn != "" && t.sourceArn != sourceArn {
			continue
		}
		entry := map[string]any{
			"TaskHandle":                       t.taskHandle,
			"SourceArn":                        t.sourceArn,
			"Status":                           t.status,
			"StartedTimestamp":                 t.startedAt.Unix(),
			"ApproximateNumberOfMessagesMoved": t.messagesMoved,
		}
		if t.destinationArn != "" {
			entry["DestinationArn"] = t.destinationArn
		}
		if t.maxNumberPerSecond > 0 {
			entry["MaxNumberOfMessagesPerSecond"] = t.maxNumberPerSecond
		}
		results = append(results, entry)
	}
	if results == nil {
		results = []map[string]any{}
	}
	return provider.OK(map[string]any{
		"Results": results,
	}), nil
}
