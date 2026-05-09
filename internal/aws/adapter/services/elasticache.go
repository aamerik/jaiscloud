package services

import (
	"fmt"
	"net/http"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
)

// ElastiCacheCodec handles the ElastiCache Query/XML wire protocol.
type ElastiCacheCodec struct{}

var _ adapter.Codec = (*ElastiCacheCodec)(nil)

func (c *ElastiCacheCodec) ServiceName() string { return "elasticache" }

func (c *ElastiCacheCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	values := mergeQueryAndForm(r, body)
	action := values.Get("Action")
	if action == "" {
		return nil, fmt.Errorf("missing Action parameter for ElastiCache request")
	}
	return &model.NormalizedRequest{
		Service: "elasticache",
		Action:  action,
		Params:  flattenQueryValues(values),
		Raw:     r,
	}, nil
}

func (c *ElastiCacheCodec) Encode(_ *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "text/xml")
	body := buildElastiCacheXML(resp.Data)
	return resp.HTTPStatus, h, []byte(body)
}

func (c *ElastiCacheCodec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "text/xml")
	body := fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<ErrorResponse xmlns="http://elasticache.amazonaws.com/doc/2015-02-02/">`+
			`<Error><Code>%s</Code><Message>%s</Message></Error>`+
			`<RequestId>jaiscloud-ec</RequestId>`+
			`</ErrorResponse>`,
		xmlEscape(perr.Code), xmlEscape(perr.Message),
	)
	return perr.HTTPStatus, h, []byte(body)
}

const ecNS = `xmlns="http://elasticache.amazonaws.com/doc/2015-02-02/"`

func buildElastiCacheXML(data map[string]any) string {
	if data == nil {
		return `<?xml version="1.0" encoding="UTF-8"?><Response/>`
	}

	wrap := func(action, inner string) string {
		return `<?xml version="1.0" encoding="UTF-8"?>` +
			`<` + action + `Response ` + ecNS + `>` +
			`<` + action + `Result>` + inner + `</` + action + `Result>` +
			`<ResponseMetadata><RequestId>jaiscloud-ec</RequestId></ResponseMetadata>` +
			`</` + action + `Response>`
	}

	if v, ok := data["CacheCluster"]; ok {
		return wrap("CreateCacheCluster", encodeCacheCluster(v))
	}
	if v, ok := data["CacheClusterModified"]; ok {
		return wrap("ModifyCacheCluster", encodeCacheCluster(v))
	}
	if v, ok := data["CacheClusterDeleted"]; ok {
		return wrap("DeleteCacheCluster", encodeCacheCluster(v))
	}
	if list, ok := data["CacheClusters"]; ok {
		var sb strings.Builder
		sb.WriteString(`<CacheClusters>`)
		if items, ok := list.([]map[string]any); ok {
			for _, item := range items {
				sb.WriteString(encodeCacheCluster(item))
			}
		}
		sb.WriteString(`</CacheClusters>`)
		return wrap("DescribeCacheClusters", sb.String())
	}
	if v, ok := data["ReplicationGroup"]; ok {
		return wrap("CreateReplicationGroup", encodeReplicationGroup(v))
	}
	if v, ok := data["ReplicationGroupModified"]; ok {
		return wrap("ModifyReplicationGroup", encodeReplicationGroup(v))
	}
	if v, ok := data["ReplicationGroupDeleted"]; ok {
		return wrap("DeleteReplicationGroup", encodeReplicationGroup(v))
	}
	if list, ok := data["ReplicationGroups"]; ok {
		var sb strings.Builder
		sb.WriteString(`<ReplicationGroups>`)
		if items, ok := list.([]map[string]any); ok {
			for _, item := range items {
				sb.WriteString(encodeReplicationGroup(item))
			}
		}
		sb.WriteString(`</ReplicationGroups>`)
		return wrap("DescribeReplicationGroups", sb.String())
	}
	return `<?xml version="1.0" encoding="UTF-8"?><Response/>`
}

func encodeCacheCluster(v any) string {
	m, ok := v.(map[string]any)
	if !ok {
		return ""
	}
	var sb strings.Builder
	sb.WriteString(`<CacheCluster>`)
	sb.WriteString(xmlTag("CacheClusterId", str(m["CacheClusterId"])))
	sb.WriteString(xmlTag("CacheClusterStatus", str(m["CacheClusterStatus"])))
	sb.WriteString(xmlTag("CacheNodeType", str(m["CacheNodeType"])))
	sb.WriteString(xmlTag("Engine", str(m["Engine"])))
	sb.WriteString(xmlTag("EngineVersion", str(m["EngineVersion"])))
	sb.WriteString(xmlTag("NumCacheNodes", str(m["NumCacheNodes"])))
	if ep, ok := m["ConfigurationEndpoint"]; ok {
		sb.WriteString(`<ConfigurationEndpoint>`)
		if epm, ok := ep.(map[string]any); ok {
			sb.WriteString(xmlTag("Address", str(epm["Address"])))
			sb.WriteString(xmlTag("Port", str(epm["Port"])))
		}
		sb.WriteString(`</ConfigurationEndpoint>`)
	}
	sb.WriteString(`</CacheCluster>`)
	return sb.String()
}

func encodeReplicationGroup(v any) string {
	m, ok := v.(map[string]any)
	if !ok {
		return ""
	}
	var sb strings.Builder
	sb.WriteString(`<ReplicationGroup>`)
	sb.WriteString(xmlTag("ReplicationGroupId", str(m["ReplicationGroupId"])))
	sb.WriteString(xmlTag("Description", str(m["Description"])))
	sb.WriteString(xmlTag("Status", str(m["Status"])))
	if ep, ok := m["ConfigurationEndpoint"]; ok {
		sb.WriteString(`<ConfigurationEndpoint>`)
		if epm, ok := ep.(map[string]any); ok {
			sb.WriteString(xmlTag("Address", str(epm["Address"])))
			sb.WriteString(xmlTag("Port", str(epm["Port"])))
		}
		sb.WriteString(`</ConfigurationEndpoint>`)
	}
	sb.WriteString(`</ReplicationGroup>`)
	return sb.String()
}
