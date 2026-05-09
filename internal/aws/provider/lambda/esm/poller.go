package esm

import (
	"context"
	"time"
)

const (
	sqsPollWaitTimeSec  = 20
	sqsEmptyPollDelay   = 1 * time.Second
	maxConsecutiveErrors = 3
)

// runSQSPoller polls an SQS queue and invokes the Lambda function with batches of messages.
func (p *Provider) runSQSPoller(poller *esmPoller, esm EventSourceMapping) {
	logger := p.logger.With("esm_uuid", esm.UUID, "queue", esm.QueueName, "function", esm.FunctionName)
	logger.Info("esm: SQS poller started")

	for {
		// Check if poller has been cancelled
		select {
		case <-poller.ctx.Done():
			logger.Info("esm: SQS poller stopped")
			return
		default:
		}

		// Reload ESM to check if still enabled
		current, err := p.loadESM(poller.ctx, esm.UUID)
		if err != nil {
			logger.Warn("esm: failed to load ESM state", "err", err)
			if !sleepCtx(poller.ctx, sqsEmptyPollDelay) {
				return
			}
			continue
		}
		if current.State == ESMStateDisabled || current.State == ESMStateDeleting {
			logger.Info("esm: SQS poller stopping - ESM disabled or deleted")
			return
		}

		batchSize := current.BatchSize
		if batchSize <= 0 {
			batchSize = 10
		}

		// Receive messages from SQS
		messages, err := p.queueAPI.InternalReceive(poller.ctx, esm.QueueName, batchSize, sqsPollWaitTimeSec)
		if err != nil {
			select {
			case <-poller.ctx.Done():
				return
			default:
			}
			logger.Warn("esm: SQS receive error", "err", err)
			current.ConsecutiveErrors++
			if current.ConsecutiveErrors >= maxConsecutiveErrors {
				logger.Error("esm: too many consecutive SQS errors, disabling ESM", "errors", current.ConsecutiveErrors)
				current.State = ESMStateDisabled
				current.StateTransitionReason = "PROBLEM"
				current.LastProcessingResult = "PROBLEM: " + err.Error()
				_ = p.persistESM(context.Background(), current)
				return
			}
			_ = p.persistESM(context.Background(), current)
			if !sleepCtx(poller.ctx, sqsEmptyPollDelay) {
				return
			}
			continue
		}

		if len(messages) == 0 {
			if !sleepCtx(poller.ctx, sqsEmptyPollDelay) {
				return
			}
			continue
		}

		// Build payload and invoke
		payload := buildSQSEventPayload(messages, esm.EventSourceArn, esm.Cloud, esm.Region)

		_, invokeErr := p.invoker.InvokeInternal(poller.ctx, esm.FunctionName, payload)
		if invokeErr != nil {
			select {
			case <-poller.ctx.Done():
				return
			default:
			}
			logger.Warn("esm: Lambda invocation failed", "err", invokeErr)
			current.ConsecutiveErrors++
			current.LastProcessingResult = "PROBLEM: " + invokeErr.Error()

			if current.ConsecutiveErrors >= maxConsecutiveErrors {
				logger.Error("esm: too many consecutive Lambda errors, disabling ESM", "errors", current.ConsecutiveErrors)
				current.State = ESMStateDisabled
				current.StateTransitionReason = "PROBLEM"
				_ = p.persistESM(context.Background(), current)
				return
			}
			_ = p.persistESM(context.Background(), current)
			// Messages are not deleted on failure — they become visible again after visibility timeout
			continue
		}

		// Success: delete messages from queue
		var handles []string
		for _, m := range messages {
			handles = append(handles, m.ReceiptHandle)
		}
		if delErr := p.queueAPI.InternalDeleteBatch(poller.ctx, esm.QueueName, handles); delErr != nil {
			logger.Warn("esm: failed to delete SQS messages after successful invocation", "err", delErr)
		}

		// Reset error counter and update last processing result
		current.ConsecutiveErrors = 0
		current.LastProcessingResult = "OK"
		_ = p.persistESM(context.Background(), current)
	}
}

// runDynamoDBStreamsPoller polls DynamoDB Streams and invokes Lambda with batches of records.
func (p *Provider) runDynamoDBStreamsPoller(poller *esmPoller, esm EventSourceMapping) {
	logger := p.logger.With("esm_uuid", esm.UUID, "table", esm.TableName, "function", esm.FunctionName)
	logger.Info("esm: DynamoDB Streams poller started")

	cursor := esm.LastSequenceNumber

	for {
		// Check if poller has been cancelled
		select {
		case <-poller.ctx.Done():
			logger.Info("esm: DynamoDB Streams poller stopped")
			return
		default:
		}

		// Reload ESM to check if still enabled
		current, err := p.loadESM(poller.ctx, esm.UUID)
		if err != nil {
			logger.Warn("esm: failed to load ESM state", "err", err)
			if !sleepCtx(poller.ctx, sqsEmptyPollDelay) {
				return
			}
			continue
		}
		if current.State == ESMStateDisabled || current.State == ESMStateDeleting {
			logger.Info("esm: DynamoDB Streams poller stopping - ESM disabled or deleted")
			return
		}

		// Check if stream is still enabled
		streamInfo, ok := p.streamStore.GetStreamInfo(esm.TableName)
		if !ok || !streamInfo.Enabled {
			if !sleepCtx(poller.ctx, sqsEmptyPollDelay) {
				return
			}
			continue
		}

		batchSize := current.BatchSize
		if batchSize <= 0 {
			batchSize = 100
		}

		// Get records from the stream
		records, _ := p.streamStore.GetRecords(esm.TableName, cursor)
		if len(records) == 0 {
			if !sleepCtx(poller.ctx, sqsEmptyPollDelay) {
				return
			}
			continue
		}

		// Batch records
		batch := records
		if len(batch) > batchSize {
			batch = batch[:batchSize]
		}

		// Build payload and invoke
		payload := buildDynamoDBStreamsEventPayload(batch, esm.EventSourceArn, esm.Cloud, esm.Region)

		_, invokeErr := p.invoker.InvokeInternal(poller.ctx, esm.FunctionName, payload)
		if invokeErr != nil {
			select {
			case <-poller.ctx.Done():
				return
			default:
			}
			logger.Warn("esm: Lambda invocation failed for DDB stream", "err", invokeErr)
			current.ConsecutiveErrors++
			current.LastProcessingResult = "PROBLEM: " + invokeErr.Error()

			if current.ConsecutiveErrors >= maxConsecutiveErrors {
				logger.Error("esm: too many consecutive Lambda errors, disabling DDB Streams ESM", "errors", current.ConsecutiveErrors)
				current.State = ESMStateDisabled
				current.StateTransitionReason = "PROBLEM"
				_ = p.persistESM(context.Background(), current)
				return
			}
			_ = p.persistESM(context.Background(), current)
			// Retry same batch on error (do NOT advance cursor)
			continue
		}

		// Success: advance cursor to last processed record
		lastRecord := batch[len(batch)-1]
		cursor = lastRecord.SequenceNumber
		current.LastSequenceNumber = cursor
		current.ConsecutiveErrors = 0
		current.LastProcessingResult = "OK"
		_ = p.persistESM(context.Background(), current)
	}
}
