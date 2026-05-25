// Package awsconfig implements the AWS Config service provider (metadata-only).
package awsconfig

import (
	"context"
	"encoding/json"
	"net/http"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const (
	rtRecorder        = "config_recorder"
	rtRecorderStatus  = "config_recorder_status"
	rtDeliveryChannel = "config_delivery_channel"
	rtConfigRule      = "config_rule"
)

// Provider handles AWS Config operations.
type Provider struct {
	resources store.ResourceStore
}

// New creates a new Config provider.
func New(resources store.ResourceStore) *Provider {
	return &Provider{resources: resources}
}

// Routes returns all Config handler registrations.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		// Configuration Recorders
		"Config.PutConfigurationRecorder":            p.PutConfigurationRecorder,
		"Config.DescribeConfigurationRecorders":      p.DescribeConfigurationRecorders,
		"Config.StartConfigurationRecorder":          p.StartConfigurationRecorder,
		"Config.StopConfigurationRecorder":           p.StopConfigurationRecorder,
		"Config.DescribeConfigurationRecorderStatus": p.DescribeConfigurationRecorderStatus,
		// Delivery Channels
		"Config.PutDeliveryChannel":       p.PutDeliveryChannel,
		"Config.DescribeDeliveryChannels": p.DescribeDeliveryChannels,
		// Config Rules
		"Config.PutConfigRule":                      p.PutConfigRule,
		"Config.DescribeConfigRules":                p.DescribeConfigRules,
		"Config.DeleteConfigRule":                   p.DeleteConfigRule,
		"Config.GetComplianceDetailsByConfigRule":   p.GetComplianceDetailsByConfigRule,
		"Config.DescribeConfigRuleEvaluationStatus": p.DescribeConfigRuleEvaluationStatus,
		// Delivery Channel status
		"Config.DescribeDeliveryChannelStatus": p.DescribeDeliveryChannelStatus,
	}
}

// ─── Types ────────────────────────────────────────────────────────────────────

type configurationRecorder struct {
	Name           string         `json:"name"`
	RoleARN        string         `json:"roleARN"`
	RecordingGroup map[string]any `json:"recordingGroup"`
}

type recorderStatus struct {
	Name          string  `json:"name"`
	Recording     bool    `json:"recording"`
	LastStatus    string  `json:"lastStatus"`
	LastStartTime float64 `json:"lastStartTime"`
	LastStopTime  float64 `json:"lastStopTime"`
}

type deliveryChannel struct {
	Name         string `json:"name"`
	S3BucketName string `json:"s3BucketName"`
	SNSTopicARN  string `json:"snsTopicARN"`
}

type configRule struct {
	ConfigRuleName string         `json:"ConfigRuleName"`
	ConfigRuleARN  string         `json:"ConfigRuleARN"`
	Source         map[string]any `json:"Source"`
	Scope          map[string]any `json:"Scope"`
}

// ─── Configuration Recorders ──────────────────────────────────────────────────

func (p *Provider) PutConfigurationRecorder(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	recParams, _ := nr.Params["ConfigurationRecorder"].(map[string]any)
	if recParams == nil {
		recParams = nr.Params
	}

	name := strParam(recParams, "name")
	if name == "" {
		name = "default"
	}

	rec := configurationRecorder{
		Name:    name,
		RoleARN: strParam(recParams, "roleARN"),
	}
	if rg, ok := recParams["recordingGroup"].(map[string]any); ok {
		rec.RecordingGroup = rg
	}

	data, _ := json.Marshal(rec)
	_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, rtRecorder, name)
	if err := p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtRecorder, ID: name, Data: data}); err != nil {
		return nil, err
	}

	// Initialize status if it doesn't exist
	if _, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtRecorderStatus, name); err == store.ErrNotFound {
		st := recorderStatus{Name: name, Recording: false, LastStatus: "Pending"}
		stData, _ := json.Marshal(st)
		_ = p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtRecorderStatus, ID: name, Data: stData})
	}

	return provider.OK(map[string]any{}), nil
}

func (p *Provider) DescribeConfigurationRecorders(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	nameFilter := strParam(nr.Params, "ConfigurationRecorderNames.member.1")

	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, rtRecorder, "")
	if err != nil {
		return nil, err
	}

	var recorders []any
	for _, e := range entries {
		var rec configurationRecorder
		json.Unmarshal(e.Data, &rec)
		if nameFilter != "" && rec.Name != nameFilter {
			continue
		}
		recorders = append(recorders, recorderToWire(rec))
	}
	if recorders == nil {
		recorders = []any{}
	}
	return provider.OK(map[string]any{"ConfigurationRecorders": recorders}), nil
}

func (p *Provider) StartConfigurationRecorder(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "ConfigurationRecorderName")
	if name == "" {
		name = "default"
	}
	return p.setRecorderStatus(ctx, nr.AccountID, nr.Region, name, true)
}

func (p *Provider) StopConfigurationRecorder(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "ConfigurationRecorderName")
	if name == "" {
		name = "default"
	}
	return p.setRecorderStatus(ctx, nr.AccountID, nr.Region, name, false)
}

func (p *Provider) setRecorderStatus(ctx context.Context, account, region, name string, recording bool) (*model.ProviderResponse, error) {
	st := recorderStatus{
		Name:       name,
		Recording:  recording,
		LastStatus: "SUCCESS",
	}
	if recording {
		st.LastStartTime = float64(clock.Now().Unix())
	} else {
		st.LastStopTime = float64(clock.Now().Unix())
	}
	data, _ := json.Marshal(st)
	_ = p.resources.Delete(ctx, account, region, rtRecorderStatus, name)
	_ = p.resources.Create(ctx, account, region, store.ResourceEntry{Type: rtRecorderStatus, ID: name, Data: data})
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) DescribeConfigurationRecorderStatus(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	nameFilter := strParam(nr.Params, "ConfigurationRecorderNames.member.1")

	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, rtRecorderStatus, "")
	if err != nil {
		return nil, err
	}

	var statuses []any
	for _, e := range entries {
		var st recorderStatus
		json.Unmarshal(e.Data, &st)
		if nameFilter != "" && st.Name != nameFilter {
			continue
		}
		statuses = append(statuses, recorderStatusToWire(st))
	}
	if statuses == nil {
		statuses = []any{}
	}
	return provider.OK(map[string]any{"ConfigurationRecordersStatus": statuses}), nil
}

// ─── Delivery Channels ────────────────────────────────────────────────────────

func (p *Provider) PutDeliveryChannel(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	chParams, _ := nr.Params["DeliveryChannel"].(map[string]any)
	if chParams == nil {
		chParams = nr.Params
	}

	name := strParam(chParams, "name")
	if name == "" {
		name = "default"
	}

	ch := deliveryChannel{
		Name:         name,
		S3BucketName: strParam(chParams, "s3BucketName"),
		SNSTopicARN:  strParam(chParams, "snsTopicARN"),
	}

	data, _ := json.Marshal(ch)
	_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, rtDeliveryChannel, name)
	if err := p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtDeliveryChannel, ID: name, Data: data}); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) DescribeDeliveryChannels(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, rtDeliveryChannel, "")
	if err != nil {
		return nil, err
	}

	var channels []any
	for _, e := range entries {
		var ch deliveryChannel
		json.Unmarshal(e.Data, &ch)
		channels = append(channels, channelToWire(ch))
	}
	if channels == nil {
		channels = []any{}
	}
	return provider.OK(map[string]any{"DeliveryChannels": channels}), nil
}

// ─── Config Rules ─────────────────────────────────────────────────────────────

func (p *Provider) PutConfigRule(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ruleParams, _ := nr.Params["ConfigRule"].(map[string]any)
	if ruleParams == nil {
		ruleParams = nr.Params
	}

	name := strParam(ruleParams, "ConfigRuleName")
	if name == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "ConfigRuleName is required", HTTPStatus: http.StatusBadRequest}
	}

	rule := configRule{
		ConfigRuleName: name,
		ConfigRuleARN:  nr.ResourceID("config-rule", name),
	}
	if src, ok := ruleParams["Source"].(map[string]any); ok {
		rule.Source = src
	}
	if scope, ok := ruleParams["Scope"].(map[string]any); ok {
		rule.Scope = scope
	}

	data, _ := json.Marshal(rule)
	_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, rtConfigRule, name)
	if err := p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtConfigRule, ID: name, Data: data}); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) DescribeConfigRules(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, rtConfigRule, "")
	if err != nil {
		return nil, err
	}

	var rules []any
	for _, e := range entries {
		var rule configRule
		json.Unmarshal(e.Data, &rule)
		rules = append(rules, ruleToWire(rule))
	}
	if rules == nil {
		rules = []any{}
	}
	return provider.OK(map[string]any{"ConfigRules": rules}), nil
}

func (p *Provider) DeleteConfigRule(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "ConfigRuleName")
	if name == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "ConfigRuleName is required", HTTPStatus: http.StatusBadRequest}
	}
	if _, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtConfigRule, name); err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "NoSuchConfigRuleException", Message: "Config rule not found", HTTPStatus: http.StatusBadRequest}
	}
	p.resources.Delete(ctx, nr.AccountID, nr.Region, rtConfigRule, name)
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) DescribeDeliveryChannelStatus(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, rtDeliveryChannel, "")
	if err != nil {
		return nil, err
	}

	var statuses []any
	for _, e := range entries {
		var ch deliveryChannel
		json.Unmarshal(e.Data, &ch)
		statuses = append(statuses, map[string]any{
			"name":                       ch.Name,
			"configHistoryDeliveryInfo":  map[string]any{"lastStatus": "SUCCESS"},
			"configSnapshotDeliveryInfo": map[string]any{"lastStatus": "SUCCESS"},
		})
	}
	if statuses == nil {
		statuses = []any{}
	}
	return provider.OK(map[string]any{"DeliveryChannelsStatus": statuses}), nil
}

func (p *Provider) GetComplianceDetailsByConfigRule(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// Metadata-only: return empty evaluation results
	return provider.OK(map[string]any{
		"EvaluationResults": []any{},
	}), nil
}

func (p *Provider) DescribeConfigRuleEvaluationStatus(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// Metadata-only: return empty status
	return provider.OK(map[string]any{
		"ConfigRulesEvaluationStatus": []any{},
	}), nil
}

// ─── Wire helpers ─────────────────────────────────────────────────────────────

func recorderToWire(rec configurationRecorder) map[string]any {
	w := map[string]any{
		"name":    rec.Name,
		"roleARN": rec.RoleARN,
	}
	if rec.RecordingGroup != nil {
		w["recordingGroup"] = rec.RecordingGroup
	}
	return w
}

func recorderStatusToWire(st recorderStatus) map[string]any {
	return map[string]any{
		"name":          st.Name,
		"recording":     st.Recording,
		"lastStatus":    st.LastStatus,
		"lastStartTime": st.LastStartTime,
		"lastStopTime":  st.LastStopTime,
	}
}

func channelToWire(ch deliveryChannel) map[string]any {
	return map[string]any{
		"name":         ch.Name,
		"s3BucketName": ch.S3BucketName,
		"snsTopicARN":  ch.SNSTopicARN,
	}
}

func ruleToWire(rule configRule) map[string]any {
	return map[string]any{
		"ConfigRuleName": rule.ConfigRuleName,
		"ConfigRuleArn":  rule.ConfigRuleARN,
		"Source":         rule.Source,
		"Scope":          rule.Scope,
	}
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
