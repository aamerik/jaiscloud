// Package elbv2 implements a metadata-only ELBv2 (Elastic Load Balancing v2) provider.
package elbv2

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const (
	rtLoadBalancer    = "elbv2_loadbalancer"
	rtTargetGroup     = "elbv2_targetgroup"
	rtListener        = "elbv2_listener"
	rtTargets         = "elbv2_targets"
	rtLBAttributes    = "elbv2_lb_attributes"
	rtTags            = "elbv2_tags"
)

// ELBv2Provider handles metadata-only ELBv2 operations.
type ELBv2Provider struct {
	resources store.ResourceStore
}

// New creates a new ELBv2Provider.
func New(resources store.ResourceStore) *ELBv2Provider {
	return &ELBv2Provider{resources: resources}
}

// Routes returns all ELBv2 handler registrations.
func (p *ELBv2Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		// Load Balancers
		"ELBv2.CreateLoadBalancer":            p.CreateLoadBalancer,
		"ELBv2.DeleteLoadBalancer":            p.DeleteLoadBalancer,
		"ELBv2.DescribeLoadBalancers":         p.DescribeLoadBalancers,
		"ELBv2.ModifyLoadBalancerAttributes":  p.ModifyLoadBalancerAttributes,
		"ELBv2.DescribeLoadBalancerAttributes": p.DescribeLoadBalancerAttributes,
		// Target Groups
		"ELBv2.CreateTargetGroup":   p.CreateTargetGroup,
		"ELBv2.DeleteTargetGroup":   p.DeleteTargetGroup,
		"ELBv2.DescribeTargetGroups": p.DescribeTargetGroups,
		// Targets
		"ELBv2.RegisterTargets":     p.RegisterTargets,
		"ELBv2.DeregisterTargets":   p.DeregisterTargets,
		"ELBv2.DescribeTargetHealth": p.DescribeTargetHealth,
		// Listeners
		"ELBv2.CreateListener":    p.CreateListener,
		"ELBv2.DeleteListener":    p.DeleteListener,
		"ELBv2.DescribeListeners": p.DescribeListeners,
		// Tags
		"ELBv2.AddTags":      p.AddTags,
		"ELBv2.RemoveTags":   p.RemoveTags,
		"ELBv2.DescribeTags": p.DescribeTags,
	}
}

// ─── Types ────────────────────────────────────────────────────────────────────

type loadBalancer struct {
	LoadBalancerArn  string   `json:"LoadBalancerArn"`
	LoadBalancerName string   `json:"LoadBalancerName"`
	DNSName          string   `json:"DNSName"`
	Scheme           string   `json:"Scheme"`
	Type             string   `json:"Type"`
	Subnets          []string `json:"Subnets"`
	SecurityGroups   []string `json:"SecurityGroups"`
	StateCode        string   `json:"StateCode"`
}

type targetGroup struct {
	TargetGroupArn  string `json:"TargetGroupArn"`
	TargetGroupName string `json:"TargetGroupName"`
	Protocol        string `json:"Protocol"`
	Port            string `json:"Port"`
	VpcId           string `json:"VpcId"`
	TargetType      string `json:"TargetType"`
	LoadBalancerArn string `json:"LoadBalancerArn"`
}

type listener struct {
	ListenerArn     string   `json:"ListenerArn"`
	LoadBalancerArn string   `json:"LoadBalancerArn"`
	Protocol        string   `json:"Protocol"`
	Port            string   `json:"Port"`
	DefaultActions  []any    `json:"DefaultActions"`
}

type targetEntry struct {
	TargetGroupArn string   `json:"TargetGroupArn"`
	TargetIDs      []string `json:"TargetIDs"`
}

// ─── Load Balancers ───────────────────────────────────────────────────────────

func (p *ELBv2Provider) CreateLoadBalancer(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	if name == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}

	arn := nr.ResourceID("elb-loadbalancer", name)
	lbType := strParam(nr.Params, "Type")
	if lbType == "" {
		lbType = "application"
	}
	scheme := strParam(nr.Params, "Scheme")
	if scheme == "" {
		scheme = "internet-facing"
	}

	subnets := extractMemberList(nr.Params, "Subnets.member")
	sgs := extractMemberList(nr.Params, "SecurityGroups.member")

	lb := loadBalancer{
		LoadBalancerArn:  arn,
		LoadBalancerName: name,
		DNSName:          name + ".us-east-1.elb.amazonaws.com",
		Scheme:           scheme,
		Type:             lbType,
		Subnets:          subnets,
		SecurityGroups:   sgs,
		StateCode:        "active",
	}

	data, _ := json.Marshal(lb)
	if err := p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtLoadBalancer, ID: arn, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, &model.ProviderError{Code: "DuplicateLoadBalancerName", Message: "Load balancer name already in use", HTTPStatus: http.StatusBadRequest}
		}
		return nil, err
	}

	return provider.OK(map[string]any{"LoadBalancers": []any{lbToWire(lb)}}), nil
}

func (p *ELBv2Provider) DeleteLoadBalancer(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "LoadBalancerArn")
	if arn == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "LoadBalancerArn is required", HTTPStatus: http.StatusBadRequest}
	}
	if err := p.resources.Delete(ctx, nr.AccountID, nr.Region, rtLoadBalancer, arn); err != nil {
		if err == store.ErrNotFound {
			return nil, &model.ProviderError{Code: "LoadBalancerNotFound", Message: "Load balancer not found", HTTPStatus: http.StatusBadRequest}
		}
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *ELBv2Provider) DescribeLoadBalancers(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arnFilter := extractMemberList(nr.Params, "LoadBalancerArns.member")
	nameFilter := extractMemberList(nr.Params, "Names.member")

	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, rtLoadBalancer, "")
	if err != nil {
		return nil, err
	}

	var result []any
	for _, e := range entries {
		var lb loadBalancer
		json.Unmarshal(e.Data, &lb)

		if len(arnFilter) > 0 && !contains(arnFilter, lb.LoadBalancerArn) {
			continue
		}
		if len(nameFilter) > 0 && !contains(nameFilter, lb.LoadBalancerName) {
			continue
		}
		result = append(result, lbToWire(lb))
	}
	if result == nil {
		result = []any{}
	}
	return provider.OK(map[string]any{"LoadBalancers": result}), nil
}

func (p *ELBv2Provider) ModifyLoadBalancerAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "LoadBalancerArn")
	if arn == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "LoadBalancerArn is required", HTTPStatus: http.StatusBadRequest}
	}

	// Load existing or start fresh for attributes
	attrs := extractKeyValueList(nr.Params, "Attributes.member")

	attrData, _ := json.Marshal(attrs)
	attrID := "attrs:" + arn
	_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, rtLBAttributes, attrID) // nolint: errcheck
	_ = p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtLBAttributes, ID: attrID, Data: attrData})

	return provider.OK(map[string]any{"Attributes": attrsToWire(attrs)}), nil
}

func (p *ELBv2Provider) DescribeLoadBalancerAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "LoadBalancerArn")
	if arn == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "LoadBalancerArn is required", HTTPStatus: http.StatusBadRequest}
	}

	attrID := "attrs:" + arn
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtLBAttributes, attrID)
	if err != nil {
		// Return empty attributes if not set
		return provider.OK(map[string]any{"Attributes": []any{}}), nil
	}
	var attrs map[string]string
	json.Unmarshal(e.Data, &attrs)
	return provider.OK(map[string]any{"Attributes": attrsToWire(attrs)}), nil
}

// ─── Target Groups ────────────────────────────────────────────────────────────

func (p *ELBv2Provider) CreateTargetGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	if name == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}

	arn := nr.ResourceID("elb-targetgroup", name)
	tg := targetGroup{
		TargetGroupArn:  arn,
		TargetGroupName: name,
		Protocol:        strParam(nr.Params, "Protocol"),
		Port:            strParam(nr.Params, "Port"),
		VpcId:           strParam(nr.Params, "VpcId"),
		TargetType:      strParam(nr.Params, "TargetType"),
	}

	data, _ := json.Marshal(tg)
	if err := p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtTargetGroup, ID: arn, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, &model.ProviderError{Code: "DuplicateTargetGroupName", Message: "Target group name already in use", HTTPStatus: http.StatusBadRequest}
		}
		return nil, err
	}

	return provider.OK(map[string]any{"TargetGroups": []any{tgToWire(tg)}}), nil
}

func (p *ELBv2Provider) DeleteTargetGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "TargetGroupArn")
	if arn == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "TargetGroupArn is required", HTTPStatus: http.StatusBadRequest}
	}
	if err := p.resources.Delete(ctx, nr.AccountID, nr.Region, rtTargetGroup, arn); err != nil {
		if err == store.ErrNotFound {
			return nil, &model.ProviderError{Code: "TargetGroupNotFound", Message: "Target group not found", HTTPStatus: http.StatusBadRequest}
		}
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *ELBv2Provider) DescribeTargetGroups(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arnFilter := extractMemberList(nr.Params, "TargetGroupArns.member")
	lbArnFilter := strParam(nr.Params, "LoadBalancerArn")

	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, rtTargetGroup, "")
	if err != nil {
		return nil, err
	}

	var result []any
	for _, e := range entries {
		var tg targetGroup
		json.Unmarshal(e.Data, &tg)

		if len(arnFilter) > 0 && !contains(arnFilter, tg.TargetGroupArn) {
			continue
		}
		if lbArnFilter != "" && tg.LoadBalancerArn != lbArnFilter {
			continue
		}
		result = append(result, tgToWire(tg))
	}
	if result == nil {
		result = []any{}
	}
	return provider.OK(map[string]any{"TargetGroups": result}), nil
}

// ─── Targets ──────────────────────────────────────────────────────────────────

func (p *ELBv2Provider) RegisterTargets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	tgArn := strParam(nr.Params, "TargetGroupArn")
	if tgArn == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "TargetGroupArn is required", HTTPStatus: http.StatusBadRequest}
	}

	newIDs := extractTargetIDs(nr.Params, "Targets.member")

	// Load existing
	existing := &targetEntry{TargetGroupArn: tgArn}
	if e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtTargets, tgArn); err == nil {
		json.Unmarshal(e.Data, existing)
	}

	// Merge
	existing.TargetIDs = mergeUnique(existing.TargetIDs, newIDs)
	data, _ := json.Marshal(existing)

	_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, rtTargets, tgArn)
	_ = p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtTargets, ID: tgArn, Data: data})

	return provider.OK(map[string]any{}), nil
}

func (p *ELBv2Provider) DeregisterTargets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	tgArn := strParam(nr.Params, "TargetGroupArn")
	if tgArn == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "TargetGroupArn is required", HTTPStatus: http.StatusBadRequest}
	}

	removeIDs := extractTargetIDs(nr.Params, "Targets.member")

	existing := &targetEntry{TargetGroupArn: tgArn}
	if e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtTargets, tgArn); err == nil {
		json.Unmarshal(e.Data, existing)
	}

	// Remove
	var remaining []string
	for _, id := range existing.TargetIDs {
		if !contains(removeIDs, id) {
			remaining = append(remaining, id)
		}
	}
	existing.TargetIDs = remaining

	data, _ := json.Marshal(existing)
	_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, rtTargets, tgArn)
	_ = p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtTargets, ID: tgArn, Data: data})

	return provider.OK(map[string]any{}), nil
}

func (p *ELBv2Provider) DescribeTargetHealth(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	tgArn := strParam(nr.Params, "TargetGroupArn")
	if tgArn == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "TargetGroupArn is required", HTTPStatus: http.StatusBadRequest}
	}

	existing := &targetEntry{TargetGroupArn: tgArn}
	if e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtTargets, tgArn); err == nil {
		json.Unmarshal(e.Data, existing)
	}

	var healthDescriptions []any
	for _, id := range existing.TargetIDs {
		healthDescriptions = append(healthDescriptions, map[string]any{
			"Target":            map[string]any{"Id": id},
			"HealthCheckPort":   "traffic-port",
			"TargetHealth":      map[string]any{"State": "healthy"},
		})
	}
	if healthDescriptions == nil {
		healthDescriptions = []any{}
	}

	return provider.OK(map[string]any{"TargetHealthDescriptions": healthDescriptions}), nil
}

// ─── Listeners ────────────────────────────────────────────────────────────────

func (p *ELBv2Provider) CreateListener(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	lbArn := strParam(nr.Params, "LoadBalancerArn")
	if lbArn == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "LoadBalancerArn is required", HTTPStatus: http.StatusBadRequest}
	}

	// Derive a unique ID for the listener
	listenerID := fmt.Sprintf("%s-%s", lbArn, strParam(nr.Params, "Port"))
	listenerArn := nr.ResourceID("elb-listener", listenerID)

	l := listener{
		ListenerArn:     listenerArn,
		LoadBalancerArn: lbArn,
		Protocol:        strParam(nr.Params, "Protocol"),
		Port:            strParam(nr.Params, "Port"),
		DefaultActions:  extractDefaultActions(nr.Params),
	}

	data, _ := json.Marshal(l)
	if err := p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtListener, ID: listenerArn, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, &model.ProviderError{Code: "DuplicateListener", Message: "Listener already exists", HTTPStatus: http.StatusBadRequest}
		}
		return nil, err
	}

	return provider.OK(map[string]any{"Listeners": []any{listenerToWire(l)}}), nil
}

func (p *ELBv2Provider) DeleteListener(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ListenerArn")
	if arn == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "ListenerArn is required", HTTPStatus: http.StatusBadRequest}
	}
	if err := p.resources.Delete(ctx, nr.AccountID, nr.Region, rtListener, arn); err != nil {
		if err == store.ErrNotFound {
			return nil, &model.ProviderError{Code: "ListenerNotFound", Message: "Listener not found", HTTPStatus: http.StatusBadRequest}
		}
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *ELBv2Provider) DescribeListeners(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	lbArn := strParam(nr.Params, "LoadBalancerArn")
	listenerArns := extractMemberList(nr.Params, "ListenerArns.member")

	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, rtListener, "")
	if err != nil {
		return nil, err
	}

	var result []any
	for _, e := range entries {
		var l listener
		json.Unmarshal(e.Data, &l)

		if lbArn != "" && l.LoadBalancerArn != lbArn {
			continue
		}
		if len(listenerArns) > 0 && !contains(listenerArns, l.ListenerArn) {
			continue
		}
		result = append(result, listenerToWire(l))
	}
	if result == nil {
		result = []any{}
	}
	return provider.OK(map[string]any{"Listeners": result}), nil
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func (p *ELBv2Provider) AddTags(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	resourceArns := extractMemberList(nr.Params, "ResourceArns.member")
	tags := extractTagList(nr.Params, "Tags.member")

	for _, arn := range resourceArns {
		tagID := "tags:" + arn
		existing := map[string]string{}
		if e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtTags, tagID); err == nil {
			json.Unmarshal(e.Data, &existing)
		}
		for k, v := range tags {
			existing[k] = v
		}
		data, _ := json.Marshal(existing)
		_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, rtTags, tagID)
		_ = p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtTags, ID: tagID, Data: data})
	}
	return provider.OK(map[string]any{}), nil
}

func (p *ELBv2Provider) RemoveTags(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	resourceArns := extractMemberList(nr.Params, "ResourceArns.member")
	tagKeys := extractMemberList(nr.Params, "TagKeys.member")

	for _, arn := range resourceArns {
		tagID := "tags:" + arn
		existing := map[string]string{}
		if e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtTags, tagID); err == nil {
			json.Unmarshal(e.Data, &existing)
		}
		for _, k := range tagKeys {
			delete(existing, k)
		}
		data, _ := json.Marshal(existing)
		_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, rtTags, tagID)
		_ = p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtTags, ID: tagID, Data: data})
	}
	return provider.OK(map[string]any{}), nil
}

func (p *ELBv2Provider) DescribeTags(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	resourceArns := extractMemberList(nr.Params, "ResourceArns.member")

	var tagDescriptions []any
	for _, arn := range resourceArns {
		tagID := "tags:" + arn
		existing := map[string]string{}
		if e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtTags, tagID); err == nil {
			json.Unmarshal(e.Data, &existing)
		}
		var tags []any
		for k, v := range existing {
			tags = append(tags, map[string]any{"Key": k, "Value": v})
		}
		if tags == nil {
			tags = []any{}
		}
		tagDescriptions = append(tagDescriptions, map[string]any{
			"ResourceArn": arn,
			"Tags":        tags,
		})
	}
	if tagDescriptions == nil {
		tagDescriptions = []any{}
	}
	return provider.OK(map[string]any{"TagDescriptions": tagDescriptions}), nil
}

// ─── Wire helpers ─────────────────────────────────────────────────────────────

func lbToWire(lb loadBalancer) map[string]any {
	return map[string]any{
		"LoadBalancerArn":  lb.LoadBalancerArn,
		"LoadBalancerName": lb.LoadBalancerName,
		"DNSName":          lb.DNSName,
		"Scheme":           lb.Scheme,
		"Type":             lb.Type,
		"State":            map[string]any{"Code": lb.StateCode},
	}
}

func tgToWire(tg targetGroup) map[string]any {
	return map[string]any{
		"TargetGroupArn":  tg.TargetGroupArn,
		"TargetGroupName": tg.TargetGroupName,
		"Protocol":        tg.Protocol,
		"Port":            tg.Port,
		"VpcId":           tg.VpcId,
		"TargetType":      tg.TargetType,
	}
}

func listenerToWire(l listener) map[string]any {
	return map[string]any{
		"ListenerArn":     l.ListenerArn,
		"LoadBalancerArn": l.LoadBalancerArn,
		"Protocol":        l.Protocol,
		"Port":            l.Port,
		"DefaultActions":  l.DefaultActions,
	}
}

func attrsToWire(attrs map[string]string) []any {
	result := make([]any, 0, len(attrs))
	for k, v := range attrs {
		result = append(result, map[string]any{"Key": k, "Value": v})
	}
	return result
}

// ─── Parameter extraction helpers ────────────────────────────────────────────

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// extractMemberList extracts AWS Query protocol lists like "Subnets.member.1", "Subnets.member.2".
func extractMemberList(params map[string]any, prefix string) []string {
	var result []string
	for i := 1; ; i++ {
		key := fmt.Sprintf("%s.%d", prefix, i)
		v, ok := params[key]
		if !ok {
			break
		}
		if s, ok := v.(string); ok && s != "" {
			result = append(result, s)
		}
	}
	return result
}

// extractTargetIDs extracts target IDs from Targets.member.N.Id
func extractTargetIDs(params map[string]any, prefix string) []string {
	var result []string
	for i := 1; ; i++ {
		idKey := fmt.Sprintf("%s.%d.Id", prefix, i)
		v, ok := params[idKey]
		if !ok {
			break
		}
		if s, ok := v.(string); ok && s != "" {
			result = append(result, s)
		}
	}
	return result
}

// extractKeyValueList extracts key=value pairs from Attributes.member.N.Key / .Value
func extractKeyValueList(params map[string]any, prefix string) map[string]string {
	result := map[string]string{}
	for i := 1; ; i++ {
		keyParam := fmt.Sprintf("%s.%d.Key", prefix, i)
		valParam := fmt.Sprintf("%s.%d.Value", prefix, i)
		k, ok := params[keyParam]
		if !ok {
			break
		}
		if ks, ok := k.(string); ok {
			vs, _ := params[valParam]
			result[ks] = str(vs)
		}
	}
	return result
}

// extractTagList extracts tags from Tags.member.N.Key / .Value
func extractTagList(params map[string]any, prefix string) map[string]string {
	return extractKeyValueList(params, prefix)
}

// extractDefaultActions extracts DefaultActions.member.N structure.
func extractDefaultActions(params map[string]any) []any {
	var actions []any
	for i := 1; ; i++ {
		typeKey := fmt.Sprintf("DefaultActions.member.%d.Type", i)
		v, ok := params[typeKey]
		if !ok {
			break
		}
		action := map[string]any{"Type": str(v)}
		// TargetGroupArn
		if tgKey := fmt.Sprintf("DefaultActions.member.%d.TargetGroupArn", i); params[tgKey] != nil {
			action["TargetGroupArn"] = str(params[tgKey])
		}
		actions = append(actions, action)
	}
	return actions
}

// contains checks if a string slice contains a value.
func contains(slice []string, val string) bool {
	for _, s := range slice {
		if s == val {
			return true
		}
	}
	return false
}

// mergeUnique merges two string slices removing duplicates.
func mergeUnique(base, add []string) []string {
	seen := map[string]bool{}
	for _, s := range base {
		seen[s] = true
	}
	result := append([]string{}, base...)
	for _, s := range add {
		if !seen[s] {
			seen[s] = true
			result = append(result, s)
		}
	}
	return result
}

func str(v any) string {
	if v == nil {
		return ""
	}
	switch s := v.(type) {
	case string:
		return s
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", v))
	}
}
