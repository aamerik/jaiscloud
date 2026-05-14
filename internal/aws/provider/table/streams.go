package table

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	streamstore "jaiscloud/internal/store/stream"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// ─── Stream-aware TableProvider extension ────────────────────────────────────
// These methods are added to the existing TableProvider. The provider's
// Streams field holds the stream store; it is set by NewWithStreams.

// StreamRoutes returns the DynamoDB Streams routes to register under "Streams.*".
func (p *TableProvider) StreamRoutes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Streams.ListStreams":        p.ListStreams,
		"Streams.DescribeStream":     p.DescribeStream,
		"Streams.GetShardIterator":   p.GetShardIterator,
		"Streams.GetRecords":         p.GetRecords,
	}
}

// ListStreams lists all enabled streams.
func (p *TableProvider) ListStreams(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if p.streams == nil {
		return provider.OK(map[string]any{"Streams": []any{}}), nil
	}
	tableFilter, _ := nr.Params["TableName"].(string)
	infos := p.streams.ListStreams()
	var streams []map[string]any
	for _, info := range infos {
		if tableFilter != "" && info.TableName != tableFilter {
			continue
		}
		streams = append(streams, map[string]any{
			"StreamArn":   info.StreamArn,
			"TableName":   info.TableName,
			"StreamLabel": info.StreamLabel,
		})
	}
	if streams == nil {
		streams = []map[string]any{}
	}
	return provider.OK(map[string]any{"Streams": streams}), nil
}

// DescribeStream returns shard metadata for a stream.
func (p *TableProvider) DescribeStream(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	streamArn, _ := nr.Params["StreamArn"].(string)
	tableName := tableNameFromStreamArn(streamArn)
	if p.streams == nil || !p.streams.IsEnabled(tableName) {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "Stream not found", HTTPStatus: http.StatusBadRequest}
	}
	info, _ := p.streams.GetStreamInfo(tableName)
	shardId := "shardId-00000000000000000001-" + tableName
	return provider.OK(map[string]any{
		"StreamDescription": map[string]any{
			"StreamArn":         info.StreamArn,
			"StreamLabel":       info.StreamLabel,
			"StreamStatus":      "ENABLED",
			"StreamViewType":    "NEW_AND_OLD_IMAGES",
			"TableName":         tableName,
			"CreationRequestDateTime": time.Now().Unix(),
			"Shards": []map[string]any{
				{
					"ShardId": shardId,
					"SequenceNumberRange": map[string]any{
						"StartingSequenceNumber": "0",
					},
				},
			},
		},
	}), nil
}

// GetShardIterator returns an opaque iterator token encoding table+position.
func (p *TableProvider) GetShardIterator(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	streamArn, _ := nr.Params["StreamArn"].(string)
	iterType, _ := nr.Params["ShardIteratorType"].(string)
	seqNum, _ := nr.Params["SequenceNumber"].(string)

	tableName := tableNameFromStreamArn(streamArn)
	if p.streams == nil || !p.streams.IsEnabled(tableName) {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "Stream not found", HTTPStatus: http.StatusBadRequest}
	}

	pos := -1 // TRIM_HORIZON / LATEST with no records
	switch iterType {
	case "AT_SEQUENCE_NUMBER":
		if n, err := strconv.Atoi(seqNum); err == nil {
			pos = n - 1
		}
	case "AFTER_SEQUENCE_NUMBER":
		if n, err := strconv.Atoi(seqNum); err == nil {
			pos = n
		}
	case "LATEST":
		_, nextSeq := p.streams.GetRecords(tableName, -1)
		pos = nextSeq - 1
	default: // TRIM_HORIZON
		pos = -1
	}

	tok := encodeIterator(tableName, pos)
	return provider.OK(map[string]any{"ShardIterator": tok}), nil
}

// GetRecords fetches up to 1000 records from the shard iterator position.
func (p *TableProvider) GetRecords(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	iterToken, _ := nr.Params["ShardIterator"].(string)
	limit := 1000
	if l, ok := nr.Params["Limit"].(float64); ok && l > 0 {
		limit = int(l)
	}

	tableName, pos := decodeIterator(iterToken)
	if tableName == "" || p.streams == nil {
		return nil, &model.ProviderError{Code: "ExpiredIteratorException", Message: "Iterator expired", HTTPStatus: http.StatusBadRequest}
	}

	records, nextSeq := p.streams.GetRecords(tableName, pos)
	if len(records) > limit {
		records = records[:limit]
	}

	wireRecords := make([]map[string]any, 0, len(records))
	for _, r := range records {
		rec := map[string]any{
			"eventID":      r.EventID,
			"eventVersion": "1.1",
			"eventSource":  "aws:dynamodb",
			"eventName":    r.EventName,
			"dynamodb": map[string]any{
				"SequenceNumber":              fmt.Sprintf("%021d", r.SequenceNumber),
				"ApproximateCreationDateTime": r.ApproximateCreationDateTime.Unix(),
				"StreamViewType":              "NEW_AND_OLD_IMAGES",
				"Keys":                        r.Keys,
				"NewImage":                    r.NewImage,
				"OldImage":                    r.OldImage,
			},
		}
		wireRecords = append(wireRecords, rec)
	}

	newIterPos := nextSeq - 1
	if len(records) > 0 {
		newIterPos = records[len(records)-1].SequenceNumber
	}
	nextIter := encodeIterator(tableName, newIterPos)

	return provider.OK(map[string]any{
		"Records":           wireRecords,
		"NextShardIterator": nextIter,
	}), nil
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func encodeIterator(tableName string, pos int) string {
	raw := fmt.Sprintf("%s:%d", tableName, pos)
	return base64.StdEncoding.EncodeToString([]byte(raw))
}

func decodeIterator(tok string) (string, int) {
	b, err := base64.StdEncoding.DecodeString(tok)
	if err != nil {
		return "", 0
	}
	parts := strings.SplitN(string(b), ":", 2)
	if len(parts) != 2 {
		return "", 0
	}
	pos, _ := strconv.Atoi(parts[1])
	return parts[0], pos
}

// tableNameFromStreamArn extracts tableName from a stream ARN.
// Format: arn:aws:dynamodb:region:accountID:table/<tableName>/stream/<label>
// The resource segment uses ":table/" not "/table/".
func tableNameFromStreamArn(arn string) string {
	for _, sep := range []string{":table/", "/table/"} {
		if idx := strings.Index(arn, sep); idx >= 0 {
			rest := arn[idx+len(sep):]
			if end := strings.Index(rest, "/"); end >= 0 {
				return rest[:end]
			}
			return rest
		}
	}
	return arn
}

// appendStreamRecord writes a record to the stream store if streams are enabled.
// The record images are filtered according to the table's StreamViewType.
func (p *TableProvider) appendStreamRecord(tableName, eventName, eventID string, keys, newImg, oldImg map[string]any) {
	if p.streams == nil || !p.streams.IsEnabled(tableName) {
		return
	}
	// Look up StreamViewType; default to NEW_AND_OLD_IMAGES.
	ts, err := p.loadTable(context.Background(), tableName)
	viewType := "NEW_AND_OLD_IMAGES"
	if err == nil && ts.StreamViewType != "" {
		viewType = ts.StreamViewType
	}

	var recNew, recOld map[string]any
	switch viewType {
	case "KEYS_ONLY":
		// No image data — only keys are included.
	case "NEW_IMAGE":
		recNew = newImg
	case "OLD_IMAGE":
		recOld = oldImg
	default: // NEW_AND_OLD_IMAGES
		recNew = newImg
		recOld = oldImg
	}

	p.streams.Append(tableName, streamstore.Record{
		EventID:   eventID,
		EventName: eventName,
		Keys:      keys,
		NewImage:  recNew,
		OldImage:  recOld,
	})
}
