// Package firehose implements the Kinesis Firehose delivery stream provider.
package firehose

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
	objectstore "jaiscloud/internal/store/object"
)

const rtDeliveryStream = "firehose_delivery_stream"

// maxBufferBytes is the per-stream buffer size threshold that triggers an early flush.
const maxBufferBytes = 5 * 1024 * 1024

// flushInterval is how often the background goroutine flushes all buffers.
const flushInterval = 60 * time.Second

// BucketChecker can check whether an S3 bucket exists.
type BucketChecker interface {
	GetBucket(ctx context.Context, bucket string) (map[string]any, error)
}

// S3Writer can write objects directly to S3.
type S3Writer interface {
	InternalPutObject(ctx context.Context, bucket, key, contentType string, data []byte) error
}

type Provider struct {
	resources store.ResourceStore
	s3meta    BucketChecker
	s3writer  S3Writer

	mu      sync.Mutex
	buffers map[string][]string // streamName → buffered records (base64-decoded)

	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

func New(resources store.ResourceStore) *Provider {
	p := &Provider{
		resources: resources,
		buffers:   make(map[string][]string),
	}
	return p
}

// Start launches the background flush goroutine.
func (p *Provider) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		ticker := time.NewTicker(flushInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				p.FlushAll(ctx)
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Shutdown stops the background flusher.
func (p *Provider) Shutdown(ctx context.Context) error {
	if p.cancel != nil {
		p.cancel()
	}
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WithS3Meta wires an S3 object-metadata store so CreateDeliveryStream can
// validate that the target bucket exists.
func (p *Provider) WithS3Meta(s3 BucketChecker) *Provider {
	p.s3meta = s3
	return p
}

// WithS3Writer wires an S3 writer for delivery stream flushing.
func (p *Provider) WithS3Writer(w S3Writer) *Provider {
	p.s3writer = w
	return p
}

// bucketNameFromARN extracts the bucket name from an S3 ARN like
// arn:aws:s3:::bucket-name
func bucketNameFromARN(arn string) string {
	// arn:aws:s3:::bucket-name  — last colon-segment is the bucket
	parts := strings.Split(arn, ":")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return ""
}

// ensure the interface is satisfied at compile time
var _ BucketChecker = (objectstore.ObjectMetaStore)(nil)

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Firehose.CreateDeliveryStream":           p.CreateDeliveryStream,
		"Firehose.DeleteDeliveryStream":           p.DeleteDeliveryStream,
		"Firehose.DescribeDeliveryStream":         p.DescribeDeliveryStream,
		"Firehose.ListDeliveryStreams":             p.ListDeliveryStreams,
		"Firehose.UpdateDestination":              p.UpdateDestination,
		"Firehose.PutRecord":                      p.PutRecord,
		"Firehose.PutRecordBatch":                 p.PutRecordBatch,
		"Firehose.StartDeliveryStreamEncryption":  p.StartDeliveryStreamEncryption,
		"Firehose.StopDeliveryStreamEncryption":   p.StopDeliveryStreamEncryption,
		"Firehose.TagDeliveryStream":              p.TagDeliveryStream,
		"Firehose.UntagDeliveryStream":            p.UntagDeliveryStream,
		"Firehose.ListTagsForDeliveryStream":      p.ListTagsForDeliveryStream,
	}
}

type deliveryStream struct {
	Name            string            `json:"DeliveryStreamName"`
	ARN             string            `json:"DeliveryStreamARN"`
	Status          string            `json:"DeliveryStreamStatus"`
	Type            string            `json:"DeliveryStreamType"`
	VersionID       string            `json:"VersionId"`
	Tags            map[string]string `json:"Tags"`
	Encrypted       bool              `json:"Encrypted"`
	CreateTimestamp time.Time         `json:"CreateTimestamp"`
	Destinations    []map[string]any  `json:"Destinations"`
	RecordCount     int               `json:"RecordCount"`
	S3Bucket        string            `json:"S3Bucket,omitempty"`
	S3Prefix        string            `json:"S3Prefix,omitempty"`
}

func randHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func str(params map[string]any, key string) string {
	v, _ := params[key].(string)
	return v
}

func fhErr(code, msg string, status int) error {
	return model.NewProviderError(code, msg, status)
}

func streamToWire(s deliveryStream) map[string]any {
	return map[string]any{
		"DeliveryStreamName":   s.Name,
		"DeliveryStreamARN":    s.ARN,
		"DeliveryStreamStatus": s.Status,
		"DeliveryStreamType":   s.Type,
		"VersionId":            s.VersionID,
		"HasMoreDestinations":  false,
		"Destinations":         s.Destinations,
		"CreateTimestamp":      s.CreateTimestamp.Unix(),
		"LastUpdateTimestamp":  s.CreateTimestamp.Unix(),
		"DeliveryStreamEncryptionConfiguration": map[string]any{
			"Status": func() string {
				if s.Encrypted {
					return "ENABLED"
				}
				return "DISABLED"
			}(),
		},
	}
}

func (p *Provider) loadStream(ctx context.Context, name string) (deliveryStream, error) {
	e, err := p.resources.Get(ctx, rtDeliveryStream, name)
	if err != nil {
		return deliveryStream{}, fhErr("ResourceNotFoundException", "Delivery stream not found: "+name, http.StatusBadRequest)
	}
	var s deliveryStream
	_ = json.Unmarshal(e.Data, &s)
	return s, nil
}

func (p *Provider) saveStream(ctx context.Context, s deliveryStream) {
	data, _ := json.Marshal(s)
	entry := store.ResourceEntry{Type: rtDeliveryStream, ID: s.Name, Data: data}
	if err := p.resources.Create(ctx, entry); err == store.ErrAlreadyExists {
		p.resources.Update(ctx, entry)
	}
}

func extractDestinations(params map[string]any) []map[string]any {
	var dests []map[string]any
	for _, key := range []string{"S3DestinationConfiguration", "ExtendedS3DestinationConfiguration", "RedshiftDestinationConfiguration", "HttpEndpointDestinationConfiguration"} {
		if v, ok := params[key]; ok {
			dests = append(dests, map[string]any{
				"DestinationId": randHex(4),
				key:             v,
			})
		}
	}
	if len(dests) == 0 {
		dests = []map[string]any{{"DestinationId": randHex(4)}}
	}
	return dests
}

func extractS3Config(params map[string]any) (bucket, prefix string) {
	for _, key := range []string{"S3DestinationConfiguration", "ExtendedS3DestinationConfiguration"} {
		if cfg, ok := params[key].(map[string]any); ok {
			bucketARN, _ := cfg["BucketARN"].(string)
			bucket = bucketNameFromARN(bucketARN)
			prefix, _ = cfg["Prefix"].(string)
			return
		}
	}
	return "", ""
}

func (p *Provider) CreateDeliveryStream(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := str(nr.Params, "DeliveryStreamName")
	if name == "" {
		return nil, fhErr("InvalidArgumentException", "DeliveryStreamName is required", http.StatusBadRequest)
	}
	if len(name) > 64 {
		return nil, fhErr("InvalidArgumentException", "DeliveryStreamName must be <= 64 chars", http.StatusBadRequest)
	}

	if _, err := p.resources.Get(ctx, rtDeliveryStream, name); err == nil {
		return nil, fhErr("ResourceInUseException", "Delivery stream "+name+" already exists", http.StatusBadRequest)
	}

	// Check 50-stream limit
	if entries, _ := p.resources.List(ctx, rtDeliveryStream, ""); len(entries) >= 50 {
		return nil, fhErr("LimitExceededException", "Maximum number of delivery streams reached (50)", http.StatusBadRequest)
	}

	// Validate S3 destination bucket if configured
	if s3Cfg, ok := nr.Params["S3DestinationConfiguration"].(map[string]any); ok {
		if bucketARN, _ := s3Cfg["BucketARN"].(string); bucketARN != "" {
			bucketName := bucketNameFromARN(bucketARN)
			if p.s3meta != nil && bucketName != "" {
				if _, err := p.s3meta.GetBucket(ctx, bucketName); err != nil {
					return nil, fhErr("InvalidArgumentException", "S3 bucket does not exist: "+bucketName, http.StatusBadRequest)
				}
			}
		}
	}

	streamType := str(nr.Params, "DeliveryStreamType")
	if streamType == "" {
		streamType = "DirectPut"
	}
	bucket, prefix := extractS3Config(nr.Params)
	s := deliveryStream{
		Name:            name,
		ARN:             nr.ResourceID("firehose-stream", name),
		Status:          "ACTIVE",
		Type:            streamType,
		VersionID:       "1",
		Tags:            map[string]string{},
		CreateTimestamp: time.Now().UTC(),
		Destinations:    extractDestinations(nr.Params),
		S3Bucket:        bucket,
		S3Prefix:        prefix,
	}
	p.saveStream(ctx, s)
	return provider.OK(map[string]any{"DeliveryStreamARN": s.ARN}), nil
}

func (p *Provider) DeleteDeliveryStream(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := str(nr.Params, "DeliveryStreamName")
	if _, err := p.loadStream(ctx, name); err != nil {
		return nil, err
	}
	_ = p.resources.Delete(ctx, rtDeliveryStream, name)
	// Remove buffer
	p.mu.Lock()
	delete(p.buffers, name)
	p.mu.Unlock()
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) DescribeDeliveryStream(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := str(nr.Params, "DeliveryStreamName")
	s, err := p.loadStream(ctx, name)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"DeliveryStreamDescription": streamToWire(s)}), nil
}

func (p *Provider) ListDeliveryStreams(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, rtDeliveryStream, "")
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		var s deliveryStream
		if json.Unmarshal(e.Data, &s) == nil {
			names = append(names, s.Name)
		}
	}
	return provider.OK(map[string]any{"DeliveryStreamNames": names, "HasMoreDeliveryStreams": false}), nil
}

func (p *Provider) UpdateDestination(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := str(nr.Params, "DeliveryStreamName")
	s, err := p.loadStream(ctx, name)
	if err != nil {
		return nil, err
	}
	s.VersionID = randHex(4)
	p.saveStream(ctx, s)
	return provider.OK(map[string]any{}), nil
}

// bufferRecord appends a decoded record to the in-memory buffer for a stream.
// If the buffer exceeds maxBufferBytes, it triggers an async flush.
func (p *Provider) bufferRecord(ctx context.Context, streamName string, data []byte) {
	p.mu.Lock()
	p.buffers[streamName] = append(p.buffers[streamName], string(data))
	totalBytes := 0
	for _, r := range p.buffers[streamName] {
		totalBytes += len(r)
	}
	shouldFlush := totalBytes >= maxBufferBytes
	p.mu.Unlock()

	if shouldFlush {
		go p.flushStream(ctx, streamName)
	}
}

// s3KeyForStream builds a time-stamped S3 key with optional prefix.
func s3KeyForStream(prefix string) string {
	now := time.Now().UTC()
	datePart := fmt.Sprintf("%04d/%02d/%02d/%02d", now.Year(), now.Month(), now.Day(), now.Hour())
	uuidBytes := make([]byte, 16)
	rand.Read(uuidBytes)
	uuid := fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uuidBytes[0:4], uuidBytes[4:6], uuidBytes[6:8], uuidBytes[8:10], uuidBytes[10:16])
	if prefix == "" {
		return datePart + "/" + uuid
	}
	return strings.TrimRight(prefix, "/") + "/" + datePart + "/" + uuid
}

// flushStream writes buffered records for a stream to S3.
func (p *Provider) flushStream(ctx context.Context, streamName string) {
	p.mu.Lock()
	records := p.buffers[streamName]
	if len(records) == 0 {
		p.mu.Unlock()
		return
	}
	p.buffers[streamName] = nil
	p.mu.Unlock()

	// Load stream config to find the S3 destination
	s, err := p.loadStream(ctx, streamName)
	if err != nil || s.S3Bucket == "" || p.s3writer == nil {
		return
	}

	// Concatenate records
	var buf bytes.Buffer
	for _, r := range records {
		buf.WriteString(r)
		buf.WriteByte('\n')
	}

	key := s3KeyForStream(s.S3Prefix)
	_ = p.s3writer.InternalPutObject(ctx, s.S3Bucket, key, "application/octet-stream", buf.Bytes())
}

// FlushAll flushes all stream buffers immediately. Used by the admin flush endpoint.
func (p *Provider) FlushAll(ctx context.Context) {
	p.mu.Lock()
	names := make([]string, 0, len(p.buffers))
	for name := range p.buffers {
		names = append(names, name)
	}
	p.mu.Unlock()
	for _, name := range names {
		p.flushStream(ctx, name)
	}
}

func (p *Provider) PutRecord(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := str(nr.Params, "DeliveryStreamName")
	s, err := p.loadStream(ctx, name)
	if err != nil {
		return nil, err
	}
	s.RecordCount++
	p.saveStream(ctx, s)

	// Buffer the record for S3 delivery
	if s.S3Bucket != "" {
		if rec, ok := nr.Params["Record"].(map[string]any); ok {
			if dataStr, ok := rec["Data"].(string); ok {
				decoded, derr := base64.StdEncoding.DecodeString(dataStr)
				if derr == nil {
					p.bufferRecord(ctx, name, decoded)
				}
			}
		}
	}

	return provider.OK(map[string]any{"RecordId": randHex(16)}), nil
}

func (p *Provider) PutRecordBatch(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := str(nr.Params, "DeliveryStreamName")
	s, err := p.loadStream(ctx, name)
	if err != nil {
		return nil, err
	}
	records, _ := nr.Params["Records"].([]any)
	count := len(records)
	if count == 0 {
		count = 1
	}
	s.RecordCount += count
	p.saveStream(ctx, s)

	// Buffer records for S3 delivery
	if s.S3Bucket != "" {
		for _, item := range records {
			if rec, ok := item.(map[string]any); ok {
				if dataStr, ok := rec["Data"].(string); ok {
					decoded, derr := base64.StdEncoding.DecodeString(dataStr)
					if derr == nil {
						p.bufferRecord(ctx, name, decoded)
					}
				}
			}
		}
	}

	requestResponses := make([]map[string]any, count)
	for i := range requestResponses {
		requestResponses[i] = map[string]any{"RecordId": randHex(16)}
	}
	return provider.OK(map[string]any{
		"FailedPutCount":   0,
		"RequestResponses": requestResponses,
	}), nil
}

func (p *Provider) StartDeliveryStreamEncryption(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := str(nr.Params, "DeliveryStreamName")
	s, err := p.loadStream(ctx, name)
	if err != nil {
		return nil, err
	}
	s.Encrypted = true
	p.saveStream(ctx, s)
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) StopDeliveryStreamEncryption(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := str(nr.Params, "DeliveryStreamName")
	s, err := p.loadStream(ctx, name)
	if err != nil {
		return nil, err
	}
	s.Encrypted = false
	p.saveStream(ctx, s)
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) TagDeliveryStream(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := str(nr.Params, "DeliveryStreamName")
	s, err := p.loadStream(ctx, name)
	if err != nil {
		return nil, err
	}
	if raw, ok := nr.Params["Tags"].([]any); ok {
		for _, item := range raw {
			if m, ok := item.(map[string]any); ok {
				k, _ := m["Key"].(string)
				v, _ := m["Value"].(string)
				if k != "" {
					s.Tags[k] = v
				}
			}
		}
	}
	p.saveStream(ctx, s)
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) UntagDeliveryStream(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := str(nr.Params, "DeliveryStreamName")
	s, err := p.loadStream(ctx, name)
	if err != nil {
		return nil, err
	}
	if raw, ok := nr.Params["TagKeys"].([]any); ok {
		for _, k := range raw {
			if key, ok := k.(string); ok {
				delete(s.Tags, key)
			}
		}
	}
	p.saveStream(ctx, s)
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) ListTagsForDeliveryStream(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := str(nr.Params, "DeliveryStreamName")
	s, err := p.loadStream(ctx, name)
	if err != nil {
		return nil, err
	}
	tags := make([]map[string]any, 0, len(s.Tags))
	for k, v := range s.Tags {
		tags = append(tags, map[string]any{"Key": k, "Value": v})
	}
	return provider.OK(map[string]any{"Tags": tags, "HasMoreTags": false}), nil
}
