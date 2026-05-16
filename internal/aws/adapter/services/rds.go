package services

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
)

// RDSCodec handles the RDS Query/XML wire protocol.
type RDSCodec struct{}

var _ adapter.Codec = (*RDSCodec)(nil)

func (c *RDSCodec) ServiceName() string { return "rds" }

func (c *RDSCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	values := mergeQueryAndForm(r, body)
	action := values.Get("Action")
	if action == "" {
		return nil, fmt.Errorf("missing Action parameter for RDS request")
	}
	params := flattenQueryValues(values)
	return &model.NormalizedRequest{
		Service: "rds",
		Action:  action,
		Params:  params,
		Raw:     r,
	}, nil
}

func (c *RDSCodec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "text/xml")
	action := ""
	if nr != nil {
		action = nr.Action
	}
	body := buildRDSXML(action, resp.Data)
	return resp.HTTPStatus, h, []byte(body)
}

func (c *RDSCodec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "text/xml")
	body := fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<ErrorResponse xmlns="http://rds.amazonaws.com/doc/2014-10-31/">`+
			`<Error><Code>%s</Code><Message>%s</Message></Error>`+
			`<RequestId>jaiscloud-rds</RequestId>`+
			`</ErrorResponse>`,
		xmlEscape(perr.Code), xmlEscape(perr.Message),
	)
	return perr.HTTPStatus, h, []byte(body)
}

const rdsNS = `xmlns="http://rds.amazonaws.com/doc/2014-10-31/"`

func buildRDSXML(action string, data map[string]any) string {
	if data == nil {
		return `<?xml version="1.0" encoding="UTF-8"?><Response/>`
	}

	wrap := func(action, inner string) string {
		return `<?xml version="1.0" encoding="UTF-8"?>` +
			`<` + action + `Response ` + rdsNS + `>` +
			`<` + action + `Result>` + inner + `</` + action + `Result>` +
			`<ResponseMetadata><RequestId>jaiscloud-rds</RequestId></ResponseMetadata>` +
			`</` + action + `Response>`
	}

	// Lifecycle ops all use their own result wrapper.
	for _, op := range []struct{ key, action string }{
		{"DBInstance", "CreateDBInstance"},
		{"DBInstanceModified", "ModifyDBInstance"},
		{"DBInstanceDeleted", "DeleteDBInstance"},
		{"DBInstanceRebooted", "RebootDBInstance"},
		{"DBInstanceStarted", "StartDBInstance"},
		{"DBInstanceStopped", "StopDBInstance"},
		{"DBInstancePromoted", "PromoteReadReplica"},
		{"DBInstanceReadReplica", "CreateDBInstanceReadReplica"},
	} {
		if v, ok := data[op.key]; ok {
			return wrap(op.action, encodeDBInstance(v))
		}
	}
	if list, ok := data["DBInstances"]; ok {
		var sb strings.Builder
		sb.WriteString(`<DBInstances>`)
		if items, ok := list.([]map[string]any); ok {
			for _, item := range items {
				sb.WriteString(encodeDBInstance(item))
			}
		}
		sb.WriteString(`</DBInstances>`)
		return wrap("DescribeDBInstances", sb.String())
	}
	if v, ok := data["DBCluster"]; ok {
		return wrap("CreateDBCluster", encodeDBCluster(v))
	}
	if v, ok := data["DBClusterModified"]; ok {
		return wrap("ModifyDBCluster", encodeDBCluster(v))
	}
	if v, ok := data["DBClusterDeleted"]; ok {
		return wrap("DeleteDBCluster", encodeDBCluster(v))
	}
	if list, ok := data["DBClusters"]; ok {
		var sb strings.Builder
		sb.WriteString(`<DBClusters>`)
		if items, ok := list.([]map[string]any); ok {
			for _, item := range items {
				sb.WriteString(encodeDBCluster(item))
			}
		}
		sb.WriteString(`</DBClusters>`)
		return wrap("DescribeDBClusters", sb.String())
	}
	if v, ok := data["DBSubnetGroup"]; ok {
		return wrap("CreateDBSubnetGroup", encodeDBSubnetGroup(v))
	}
	if list, ok := data["DBSubnetGroups"]; ok {
		var sb strings.Builder
		sb.WriteString(`<DBSubnetGroups>`)
		if items, ok := list.([]map[string]any); ok {
			for _, item := range items {
				sb.WriteString(encodeDBSubnetGroup(item))
			}
		}
		sb.WriteString(`</DBSubnetGroups>`)
		return wrap("DescribeDBSubnetGroups", sb.String())
	}
	if v, ok := data["DBParameterGroup"]; ok {
		a := action
		if a == "" {
			a = "CreateDBParameterGroup"
		}
		return wrap(a, encodeDBParameterGroup(v))
	}
	if list, ok := data["DBParameterGroups"]; ok {
		var sb strings.Builder
		sb.WriteString(`<DBParameterGroups>`)
		if items, ok := list.([]any); ok {
			for _, item := range items {
				sb.WriteString(encodeDBParameterGroup(item))
			}
		}
		sb.WriteString(`</DBParameterGroups>`)
		return wrap("DescribeDBParameterGroups", sb.String())
	}
	// TagList check comes last so action-specific shapes take priority.
	if _, ok := data["TagList"]; ok {
		a := action
		if a == "" {
			a = "ListTagsForResource"
		}
		return wrap(a, encodeRDSTagList(data["TagList"]))
	}
	// Any action with unknown or empty data: emit a proper wrapper, never bare <Response/>.
	if action != "" {
		var inner strings.Builder
		for k, v := range data {
			inner.WriteString(encodeRDSValue(k, v))
		}
		return wrap(action, inner.String())
	}
	return `<?xml version="1.0" encoding="UTF-8"?>` +
		`<Response ` + rdsNS + `>` +
		`<ResponseMetadata><RequestId>jaiscloud-rds</RequestId></ResponseMetadata>` +
		`</Response>`
}

func encodeRDSValue(k string, v any) string {
	switch val := v.(type) {
	case map[string]any:
		var sb strings.Builder
		sb.WriteString(`<` + k + `>`)
		for ck, cv := range val {
			sb.WriteString(encodeRDSValue(ck, cv))
		}
		sb.WriteString(`</` + k + `>`)
		return sb.String()
	case []any:
		var sb strings.Builder
		for _, item := range val {
			sb.WriteString(encodeRDSValue(k, item))
		}
		return sb.String()
	default:
		_ = val
		return xmlTag(k, str(v))
	}
}

func encodeDBParameterGroup(v any) string {
	m, ok := v.(map[string]any)
	if !ok {
		return ""
	}
	var sb strings.Builder
	sb.WriteString(`<DBParameterGroup>`)
	sb.WriteString(xmlTag("DBParameterGroupName", str(m["DBParameterGroupName"])))
	sb.WriteString(xmlTag("DBParameterGroupFamily", str(m["DBParameterGroupFamily"])))
	sb.WriteString(xmlTag("Description", str(m["Description"])))
	sb.WriteString(xmlTag("DBParameterGroupArn", str(m["DBParameterGroupArn"])))
	sb.WriteString(`</DBParameterGroup>`)
	return sb.String()
}

func encodeRDSTagList(v any) string {
	var sb strings.Builder
	sb.WriteString(`<TagList>`)
	if tags, ok := v.([]map[string]any); ok {
		for _, t := range tags {
			sb.WriteString(`<Tag>`)
			sb.WriteString(xmlTag("Key", str(t["Key"])))
			sb.WriteString(xmlTag("Value", str(t["Value"])))
			sb.WriteString(`</Tag>`)
		}
	}
	sb.WriteString(`</TagList>`)
	return sb.String()
}

func encodeDBInstance(v any) string {
	m, ok := v.(map[string]any)
	if !ok {
		return ""
	}
	var sb strings.Builder
	sb.WriteString(`<DBInstance>`)
	sb.WriteString(xmlTag("DBInstanceIdentifier", str(m["DBInstanceIdentifier"])))
	sb.WriteString(xmlTag("DBInstanceClass", str(m["DBInstanceClass"])))
	sb.WriteString(xmlTag("Engine", str(m["Engine"])))
	sb.WriteString(xmlTag("DBInstanceStatus", str(m["DBInstanceStatus"])))
	sb.WriteString(xmlTag("MasterUsername", str(m["MasterUsername"])))
	sb.WriteString(xmlTag("DBName", str(m["DBName"])))
	sb.WriteString(xmlTag("Endpoint", encodeEndpoint(m["Endpoint"])))
	sb.WriteString(xmlTag("AllocatedStorage", str(m["AllocatedStorage"])))
	sb.WriteString(xmlTag("MultiAZ", str(m["MultiAZ"])))
	sb.WriteString(xmlTag("EngineVersion", str(m["EngineVersion"])))
	sb.WriteString(xmlTag("PubliclyAccessible", str(m["PubliclyAccessible"])))
	sb.WriteString(xmlTag("DBInstanceArn", str(m["DBInstanceArn"])))
	if sg, ok := m["DBSubnetGroup"]; ok {
		sb.WriteString(xmlTag("DBSubnetGroup", encodeDBSubnetGroup(sg)))
	}
	sb.WriteString(`</DBInstance>`)
	return sb.String()
}

func encodeEndpoint(v any) string {
	if v == nil {
		return ""
	}
	m, ok := v.(map[string]any)
	if !ok {
		return ""
	}
	return xmlTag("Address", str(m["Address"])) + xmlTag("Port", str(m["Port"]))
}

func encodeDBCluster(v any) string {
	m, ok := v.(map[string]any)
	if !ok {
		return ""
	}
	var sb strings.Builder
	sb.WriteString(`<DBCluster>`)
	sb.WriteString(xmlTag("DBClusterIdentifier", str(m["DBClusterIdentifier"])))
	sb.WriteString(xmlTag("DBClusterArn", str(m["DBClusterArn"])))
	sb.WriteString(xmlTag("Status", str(m["Status"])))
	sb.WriteString(xmlTag("Engine", str(m["Engine"])))
	sb.WriteString(xmlTag("EngineVersion", str(m["EngineVersion"])))
	sb.WriteString(xmlTag("MasterUsername", str(m["MasterUsername"])))
	sb.WriteString(xmlTag("DatabaseName", str(m["DatabaseName"])))
	sb.WriteString(xmlTag("Endpoint", str(m["Endpoint"])))
	sb.WriteString(xmlTag("ReaderEndpoint", str(m["ReaderEndpoint"])))
	sb.WriteString(xmlTag("Port", str(m["Port"])))
	sb.WriteString(`</DBCluster>`)
	return sb.String()
}

func encodeDBSubnetGroup(v any) string {
	m, ok := v.(map[string]any)
	if !ok {
		return ""
	}
	var sb strings.Builder
	sb.WriteString(`<DBSubnetGroup>`)
	sb.WriteString(xmlTag("DBSubnetGroupName", str(m["DBSubnetGroupName"])))
	sb.WriteString(xmlTag("DBSubnetGroupDescription", str(m["DBSubnetGroupDescription"])))
	sb.WriteString(xmlTag("VpcId", str(m["VpcId"])))
	sb.WriteString(xmlTag("SubnetGroupStatus", str(m["SubnetGroupStatus"])))
	sb.WriteString(`</DBSubnetGroup>`)
	return sb.String()
}

// flattenQueryValues converts url.Values to a flat map[string]any.
func flattenQueryValues(values url.Values) map[string]any {
	params := make(map[string]any, len(values))
	for k, vs := range values {
		if len(vs) > 0 {
			params[k] = vs[0]
		}
	}
	return params
}
