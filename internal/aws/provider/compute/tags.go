package compute

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const rtEC2Tags = "ec2_tags"

// extractTags parses EC2 Query-protocol Tag.N.Key / Tag.N.Value pairs from params.
func extractTags(params map[string]any) map[string]string {
	tags := map[string]string{}
	for i := 1; ; i++ {
		key := strParam(params, fmt.Sprintf("Tag.%d.Key", i))
		if key == "" {
			break
		}
		val := strParam(params, fmt.Sprintf("Tag.%d.Value", i))
		tags[key] = val
	}
	return tags
}

// extractTagKeys parses EC2 Tag.N.Key (without values) used by DeleteTags.
func extractTagKeys(params map[string]any) []string {
	var keys []string
	for i := 1; ; i++ {
		key := strParam(params, fmt.Sprintf("Tag.%d.Key", i))
		if key == "" {
			break
		}
		keys = append(keys, key)
	}
	return keys
}

func loadResourceTags(ctx context.Context, res store.ResourceStore, resourceID string) map[string]string {
	e, err := res.Get(ctx, rtEC2Tags, resourceID)
	if err != nil {
		return map[string]string{}
	}
	var m map[string]string
	if json.Unmarshal(e.Data, &m) != nil {
		return map[string]string{}
	}
	return m
}

func saveResourceTags(ctx context.Context, res store.ResourceStore, resourceID string, tags map[string]string) {
	data, _ := json.Marshal(tags)
	entry := store.ResourceEntry{Type: rtEC2Tags, ID: resourceID, Data: data}
	if err := res.Create(ctx, entry); err == store.ErrAlreadyExists {
		res.Update(ctx, entry)
	}
}

// resourceTypeFromID guesses the EC2 resource type from the ID prefix.
func resourceTypeFromID(id string) string {
	switch {
	case strings.HasPrefix(id, "i-"):
		return "instance"
	case strings.HasPrefix(id, "vpc-"):
		return "vpc"
	case strings.HasPrefix(id, "subnet-"):
		return "subnet"
	case strings.HasPrefix(id, "sg-"):
		return "security-group"
	case strings.HasPrefix(id, "igw-"):
		return "internet-gateway"
	case strings.HasPrefix(id, "rtb-"):
		return "route-table"
	case strings.HasPrefix(id, "nat-"):
		return "natgateway"
	case strings.HasPrefix(id, "kp-"):
		return "key-pair"
	case strings.HasPrefix(id, "ami-"):
		return "image"
	default:
		return "unknown"
	}
}

func (p *ComputeProvider) CreateTags(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	resourceIDs := extractIndexedParam(nr.Params, "ResourceId")
	newTags := extractTags(nr.Params)
	for _, rid := range resourceIDs {
		existing := loadResourceTags(ctx, p.resources, rid)
		for k, v := range newTags {
			existing[k] = v
		}
		saveResourceTags(ctx, p.resources, rid, existing)
	}
	return provider.OK(map[string]any{"return": "true"}), nil
}

func (p *ComputeProvider) DeleteTags(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	resourceIDs := extractIndexedParam(nr.Params, "ResourceId")
	tagKeys := extractTagKeys(nr.Params)
	for _, rid := range resourceIDs {
		existing := loadResourceTags(ctx, p.resources, rid)
		for _, k := range tagKeys {
			delete(existing, k)
		}
		saveResourceTags(ctx, p.resources, rid, existing)
	}
	return provider.OK(map[string]any{"return": "true"}), nil
}

func (p *ComputeProvider) DescribeTags(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// Parse filters: Filter.N.Name + Filter.N.Value.M
	var filters []tagFilter
	for i := 1; ; i++ {
		name := strParam(nr.Params, fmt.Sprintf("Filter.%d.Name", i))
		if name == "" {
			break
		}
		var vals []string
		for j := 1; ; j++ {
			v := strParam(nr.Params, fmt.Sprintf("Filter.%d.Value.%d", i, j))
			if v == "" {
				break
			}
			vals = append(vals, v)
		}
		filters = append(filters, tagFilter{name: name, values: vals})
	}

	entries, _ := p.resources.List(ctx, rtEC2Tags, "")
	var tagItems []map[string]any
	for _, e := range entries {
		var tags map[string]string
		if json.Unmarshal(e.Data, &tags) != nil {
			continue
		}
		rid := e.ID
		rtype := resourceTypeFromID(rid)
		for k, v := range tags {
			item := map[string]any{
				"resourceId":   rid,
				"resourceType": rtype,
				"key":          k,
				"value":        v,
			}
			if !matchesTagFilters(item, filters) {
				continue
			}
			tagItems = append(tagItems, item)
		}
	}
	if tagItems == nil {
		tagItems = []map[string]any{}
	}
	return provider.OK(map[string]any{"tagSet": tagItems}), nil
}

type tagFilter struct {
	name   string
	values []string
}

func matchesTagFilters(item map[string]any, filters []tagFilter) bool {
	for _, f := range filters {
		var actual string
		switch f.name {
		case "resource-id":
			actual = item["resourceId"].(string)
		case "resource-type":
			actual = item["resourceType"].(string)
		case "key":
			actual = item["key"].(string)
		case "value":
			actual = item["value"].(string)
		default:
			continue
		}
		if !containsStr(f.values, actual) {
			return false
		}
	}
	return true
}
