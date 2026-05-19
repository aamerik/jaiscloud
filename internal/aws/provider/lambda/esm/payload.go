package esm

import (
	"encoding/json"
	"fmt"

	streamstore "jaiscloud/internal/aws/store/stream"
)

func buildSQSEventPayload(messages []InternalMessage, sourceArn, cloud, region string) []byte {
	records := make([]map[string]any, len(messages))
	for i, m := range messages {
		records[i] = map[string]any{
			"messageId":         m.MessageID,
			"receiptHandle":     m.ReceiptHandle,
			"body":              m.Body,
			"attributes":        m.Attributes,
			"messageAttributes": m.MessageAttributes,
			"md5OfBody":         m.MD5OfBody,
			"eventSource":       cloud + ":sqs",
			"eventSourceARN":    sourceArn,
			"awsRegion":         region,
		}
	}
	payload, _ := json.Marshal(map[string]any{"Records": records})
	return payload
}

func buildDynamoDBStreamsEventPayload(records []streamstore.Record, sourceArn, cloud, region string) []byte {
	out := make([]map[string]any, len(records))
	for i, r := range records {
		out[i] = map[string]any{
			"eventID":        r.EventID,
			"eventName":      r.EventName,
			"eventVersion":   "1.1",
			"eventSource":    cloud + ":dynamodb",
			"eventSourceARN": sourceArn,
			"awsRegion":      region,
			"dynamodb": map[string]any{
				"Keys":                        r.Keys,
				"NewImage":                    r.NewImage,
				"OldImage":                    r.OldImage,
				"SequenceNumber":              fmt.Sprintf("%d", r.SequenceNumber),
				"SizeBytes":                   estimateRecordSize(r),
				"StreamViewType":              "NEW_AND_OLD_IMAGES",
				"ApproximateCreationDateTime": r.ApproximateCreationDateTime.Unix(),
			},
		}
	}
	payload, _ := json.Marshal(map[string]any{"Records": out})
	return payload
}

func estimateRecordSize(r streamstore.Record) int {
	b, _ := json.Marshal(r)
	return len(b)
}
