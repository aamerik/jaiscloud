// Package firehose implements the Kinesis Firehose delivery stream provider.
package firehose

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const rtDeliveryStream = "firehose_delivery_stream"

type Provider struct {
	resources store.ResourceStore
}

func New(resources store.ResourceStore) *Provider {
	return &Provider{resources: resources}
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Firehose.CreateDeliveryStream":                p.CreateDeliveryStream,
		"Firehose.DeleteDeliveryStream":                p.DeleteDeliveryStream,
		"Firehose.DescribeDeliveryStream":              p.DescribeDeliveryStream,
		"Firehose.ListDeliveryStreams":                 p.ListDeliveryStreams,
		"Firehose.UpdateDestination":                  p.UpdateDestination,
		"Firehose.PutRecord":                          p.PutRecord,
		"Firehose.PutRecordBatch":                     p.PutRecordBatch,
		"Firehose.StartDeliveryStreamEncryption":      p.StartDeliveryStreamEncryption,
		"Firehose.StopDeliveryStreamEncryption":       p.StopDeliveryStreamEncryption,
		"Firehose.TagDeliveryStream":                  p.TagDeliveryStream,
		"Firehose.UntagDeliveryStream":                p.UntagDeliveryStream,
		"Firehose.ListTagsForDeliveryStream":          p.ListTagsForDeliveryStream,
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
		"DeliveryStreamName":             s.Name,
		"DeliveryStreamARN":              s.ARN,
		"DeliveryStreamStatus":           s.Status,
		"DeliveryStreamType":             s.Type,
		"VersionId":                      s.VersionID,
		"HasMoreDestinations":            false,
		"Destinations":                   s.Destinations,
		"CreateTimestamp":                s.CreateTimestamp.Unix(),
		"LastUpdateTimestamp":            s.CreateTimestamp.Unix(),
		"DeliveryStreamEncryptionConfiguration": map[string]any{
			"Status": map[string]any{"DISABLED": !s.Encrypted, "ENABLED": s.Encrypted}["DISABLED"],
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

	streamType := str(nr.Params, "DeliveryStreamType")
	if streamType == "" {
		streamType = "DirectPut"
	}
	s := deliveryStream{
		Name:            name,
		ARN:             nr.ResourceID("firehose-stream", name),
		Status:          "ACTIVE",
		Type:            streamType,
		VersionID:       "1",
		Tags:            map[string]string{},
		CreateTimestamp: time.Now().UTC(),
		Destinations:    extractDestinations(nr.Params),
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

func (p *Provider) PutRecord(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := str(nr.Params, "DeliveryStreamName")
	s, err := p.loadStream(ctx, name)
	if err != nil {
		return nil, err
	}
	s.RecordCount++
	p.saveStream(ctx, s)
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
	// Prefix filter
	prefix := str(nr.Params, "ExclusiveStartTagKey")
	_ = prefix
	_ = strings.HasPrefix
	return provider.OK(map[string]any{"Tags": tags, "HasMoreTags": false}), nil
}
