package services

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
)

// Route53Codec handles the Route53 REST/XML protocol.
// Requests: REST paths like /2013-04-01/hostedzone
// Responses: XML bodies
type Route53Codec struct{}

var _ adapter.Codec = (*Route53Codec)(nil)

func (c *Route53Codec) ServiceName() string { return "route53" }

func (c *Route53Codec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	action, params := route53ActionFromRequest(r, body)
	if action == "" {
		return nil, fmt.Errorf("unrecognised Route53 request: %s %s", r.Method, r.URL.Path)
	}
	return &model.NormalizedRequest{
		Service: "route53",
		Action:  action,
		Params:  params,
		Raw:     r,
	}, nil
}

func (c *Route53Codec) Encode(_ *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/xml")
	body := buildRoute53XML(resp.Data)
	return resp.HTTPStatus, h, []byte(body)
}

func (c *Route53Codec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/xml")
	body := fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<ErrorResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">`+
			`<Error><Type>Sender</Type><Code>%s</Code><Message>%s</Message></Error>`+
			`</ErrorResponse>`,
		xmlEscape(perr.Code), xmlEscape(perr.Message),
	)
	return perr.HTTPStatus, h, []byte(body)
}

// route53ActionFromRequest maps REST method + path to an action name and extracts params.
//
// /2013-04-01/hostedzone                        POST → CreateHostedZone  GET → ListHostedZones
// /2013-04-01/hostedzone/{id}                   GET  → GetHostedZone     DELETE → DeleteHostedZone
// /2013-04-01/hostedzone/{id}/rrset             POST → ChangeResourceRecordSets  GET → ListResourceRecordSets
// /2013-04-01/healthcheck                       POST → CreateHealthCheck  GET → ListHealthChecks
// /2013-04-01/healthcheck/{id}                  GET  → GetHealthCheck    DELETE → DeleteHealthCheck
func route53ActionFromRequest(r *http.Request, body []byte) (string, map[string]any) {
	path := strings.TrimPrefix(r.URL.Path, "/2013-04-01")
	path = strings.TrimSuffix(path, "/")
	method := r.Method
	params := map[string]any{}

	switch {
	// Hosted Zones
	case path == "/hostedzone" && method == "POST":
		parseXMLBody(body, params)
		return "CreateHostedZone", params
	case path == "/hostedzone" && method == "GET":
		if v := r.URL.Query().Get("maxitems"); v != "" {
			params["MaxItems"] = v
		}
		if v := r.URL.Query().Get("marker"); v != "" {
			params["Marker"] = v
		}
		return "ListHostedZones", params
	case strings.HasPrefix(path, "/hostedzone/") && !strings.Contains(path[len("/hostedzone/"):], "/"):
		id := strings.TrimPrefix(path, "/hostedzone/")
		params["Id"] = id
		if method == "GET" {
			return "GetHostedZone", params
		}
		if method == "DELETE" {
			return "DeleteHostedZone", params
		}
	case strings.HasSuffix(path, "/rrset"):
		parts := strings.Split(path, "/")
		// /hostedzone/{id}/rrset
		if len(parts) >= 3 {
			params["HostedZoneId"] = parts[2]
		}
		if method == "POST" {
			parseXMLBody(body, params)
			return "ChangeResourceRecordSets", params
		}
		if method == "GET" {
			return "ListResourceRecordSets", params
		}
	// Health Checks
	case path == "/healthcheck" && method == "POST":
		parseXMLBody(body, params)
		return "CreateHealthCheck", params
	case path == "/healthcheck" && method == "GET":
		return "ListHealthChecks", params
	case strings.HasPrefix(path, "/healthcheck/") && !strings.Contains(path[len("/healthcheck/"):], "/"):
		id := strings.TrimPrefix(path, "/healthcheck/")
		params["Id"] = id
		if method == "GET" {
			return "GetHealthCheck", params
		}
		if method == "DELETE" {
			return "DeleteHealthCheck", params
		}
	// Change info
	case strings.HasPrefix(path, "/change/"):
		id := strings.TrimPrefix(path, "/change/")
		params["Id"] = id
		return "GetChange", params
	// Tags — /tags/{resourcetype}/{id}
	case strings.HasPrefix(path, "/tags/"):
		rest := strings.TrimPrefix(path, "/tags/")
		parts := strings.SplitN(rest, "/", 2)
		if len(parts) == 2 {
			params["ResourceType"] = parts[0]
			params["ResourceId"] = parts[1]
		}
		if method == "POST" {
			parseR53TagsBody(body, params)
			return "ChangeTagsForResource", params
		}
		if method == "GET" {
			return "ListTagsForResource", params
		}
	}
	return "", nil
}

// parseR53TagsBody parses the ChangeTagsForResource XML body into structured params.
// It produces params["AddTags"] = []any{{"Key":k,"Value":v},...}
// and params["RemoveTagKeys"] = []any{"key1",...}.
func parseR53TagsBody(body []byte, params map[string]any) {
	if len(body) == 0 {
		return
	}
	dec := xml.NewDecoder(strings.NewReader(string(body)))
	var stack []string
	var curKey, curVal string
	var addTags []any
	var removeKeys []any
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
		switch t := tok.(type) {
		case xml.StartElement:
			stack = append(stack, t.Name.Local)
			if t.Name.Local == "Tag" {
				curKey = ""
				curVal = ""
			}
		case xml.CharData:
			text := strings.TrimSpace(string(t))
			if len(stack) == 0 {
				break
			}
			top := stack[len(stack)-1]
			parent := ""
			if len(stack) >= 2 {
				parent = stack[len(stack)-2]
			}
			if top == "Key" && parent == "Tag" {
				curKey = text
			} else if top == "Value" && parent == "Tag" {
				curVal = text
			} else if top == "Key" && parent == "RemoveTagKeys" {
				removeKeys = append(removeKeys, text)
			}
		case xml.EndElement:
			if len(stack) == 0 {
				break
			}
			top := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if top == "Tag" && curKey != "" {
				addTags = append(addTags, map[string]any{"Key": curKey, "Value": curVal})
				curKey = ""
				curVal = ""
			}
		}
	}
	if len(addTags) > 0 {
		params["AddTags"] = addTags
	}
	if len(removeKeys) > 0 {
		params["RemoveTagKeys"] = removeKeys
	}
}

// parseXMLBody does a best-effort parse of the XML request body into the params map.
func parseXMLBody(body []byte, params map[string]any) {
	if len(body) == 0 {
		return
	}
	dec := xml.NewDecoder(strings.NewReader(string(body)))
	var stack []string
	var current string
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
		switch t := tok.(type) {
		case xml.StartElement:
			stack = append(stack, t.Name.Local)
			current = ""
		case xml.CharData:
			current = strings.TrimSpace(string(t))
		case xml.EndElement:
			if len(stack) > 0 {
				key := stack[len(stack)-1]
				if current != "" {
					params[key] = current
				}
				stack = stack[:len(stack)-1]
				current = ""
			}
		}
	}
}

// buildRoute53XML serialises the provider response data as Route53 XML.
func buildRoute53XML(data map[string]any) string {
	if data == nil {
		return `<?xml version="1.0" encoding="UTF-8"?><Response/>`
	}
	ns := `xmlns="https://route53.amazonaws.com/doc/2013-04-01/"`

	if hz, ok := data["HostedZone"]; ok {
		inner := encodeHostedZone(hz)
		return `<?xml version="1.0" encoding="UTF-8"?>` +
			`<GetHostedZoneResponse ` + ns + `>` + inner + `</GetHostedZoneResponse>`
	}
	if _, ok := data["CreateHostedZoneResponse"]; ok {
		inner := encodeHostedZone(data["HostedZoneCreated"])
		loc := str(data["Location"])
		return `<?xml version="1.0" encoding="UTF-8"?>` +
			`<CreateHostedZoneResponse ` + ns + `>` + inner +
			`<ChangeInfo><Id>/change/C1</Id><Status>INSYNC</Status></ChangeInfo>` +
			`<Location>` + xmlEscape(loc) + `</Location>` +
			`</CreateHostedZoneResponse>`
	}
	if list, ok := data["HostedZones"]; ok {
		var sb strings.Builder
		sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
		sb.WriteString(`<ListHostedZonesResponse ` + ns + `>`)
		sb.WriteString(`<HostedZones>`)
		if zones, ok := list.([]map[string]any); ok {
			for _, z := range zones {
				sb.WriteString(encodeHostedZone(z))
			}
		}
		sb.WriteString(`</HostedZones>`)
		sb.WriteString(`<IsTruncated>false</IsTruncated>`)
		sb.WriteString(`<MaxItems>100</MaxItems>`)
		sb.WriteString(`</ListHostedZonesResponse>`)
		return sb.String()
	}
	if rrsets, ok := data["ResourceRecordSets"]; ok {
		var sb strings.Builder
		sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
		sb.WriteString(`<ListResourceRecordSetsResponse ` + ns + `>`)
		sb.WriteString(`<ResourceRecordSets>`)
		if sets, ok := rrsets.([]map[string]any); ok {
			for _, rr := range sets {
				sb.WriteString(encodeRRSet(rr))
			}
		}
		sb.WriteString(`</ResourceRecordSets>`)
		sb.WriteString(`<IsTruncated>false</IsTruncated>`)
		sb.WriteString(`<MaxRRSets>300</MaxRRSets>`)
		sb.WriteString(`</ListResourceRecordSetsResponse>`)
		return sb.String()
	}
	if _, ok := data["DeleteHostedZoneResponse"]; ok {
		return `<?xml version="1.0" encoding="UTF-8"?>` +
			`<DeleteHostedZoneResponse ` + ns + `>` +
			`<ChangeInfo><Id>/change/C1</Id><Status>INSYNC</Status><SubmittedAt>` +
			xmlEscape(str(data["SubmittedAt"])) + `</SubmittedAt></ChangeInfo>` +
			`</DeleteHostedZoneResponse>`
	}
	if _, ok := data["ChangeInfo"]; ok {
		return `<?xml version="1.0" encoding="UTF-8"?>` +
			`<ChangeResourceRecordSetsResponse ` + ns + `>` +
			`<ChangeInfo><Id>/change/C1</Id><Status>INSYNC</Status><SubmittedAt>` +
			str(data["SubmittedAt"]) + `</SubmittedAt></ChangeInfo>` +
			`</ChangeResourceRecordSetsResponse>`
	}
	if hc, ok := data["HealthCheck"]; ok {
		return `<?xml version="1.0" encoding="UTF-8"?>` +
			`<CreateHealthCheckResponse ` + ns + `>` +
			encodeHealthCheck(hc) +
			`</CreateHealthCheckResponse>`
	}
	if list, ok := data["HealthChecks"]; ok {
		var sb strings.Builder
		sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
		sb.WriteString(`<ListHealthChecksResponse ` + ns + `>`)
		sb.WriteString(`<HealthChecks>`)
		if hcs, ok := list.([]map[string]any); ok {
			for _, hc := range hcs {
				sb.WriteString(encodeHealthCheck(hc))
			}
		}
		sb.WriteString(`</HealthChecks>`)
		sb.WriteString(`<IsTruncated>false</IsTruncated>`)
		sb.WriteString(`</ListHealthChecksResponse>`)
		return sb.String()
	}
	// GetChange
	if status, ok := data["Status"]; ok {
		return `<?xml version="1.0" encoding="UTF-8"?>` +
			`<GetChangeResponse ` + ns + `>` +
			`<ChangeInfo><Id>/change/C1</Id><Status>` + xmlEscape(str(status)) + `</Status></ChangeInfo>` +
			`</GetChangeResponse>`
	}
	// ChangeTagsForResource — no body needed
	if _, ok := data["ChangeTagsForResourceResponse"]; ok {
		return `<?xml version="1.0" encoding="UTF-8"?>` +
			`<ChangeTagsForResourceResponse ` + ns + `></ChangeTagsForResourceResponse>`
	}
	// ListTagsForResource
	if rts, ok := data["ResourceTagSet"]; ok {
		inner := encodeR53TagSet(rts)
		return `<?xml version="1.0" encoding="UTF-8"?>` +
			`<ListTagsForResourceResponse ` + ns + `>` +
			`<ResourceTagSet>` + inner + `</ResourceTagSet>` +
			`</ListTagsForResourceResponse>`
	}
	// ListTagsForResources
	if sets, ok := data["ResourceTagSets"]; ok {
		var sb strings.Builder
		sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
		sb.WriteString(`<ListTagsForResourcesResponse ` + ns + `>`)
		sb.WriteString(`<ResourceTagSets>`)
		if items, ok := sets.([]map[string]any); ok {
			for _, item := range items {
				sb.WriteString(`<ResourceTagSet>`)
				sb.WriteString(encodeR53TagSet(item))
				sb.WriteString(`</ResourceTagSet>`)
			}
		}
		sb.WriteString(`</ResourceTagSets>`)
		sb.WriteString(`</ListTagsForResourcesResponse>`)
		return sb.String()
	}
	return `<?xml version="1.0" encoding="UTF-8"?><Response/>`
}

func encodeR53TagSet(v any) string {
	m, ok := v.(map[string]any)
	if !ok {
		return ""
	}
	var sb strings.Builder
	sb.WriteString(xmlTag("ResourceType", str(m["ResourceType"])))
	sb.WriteString(xmlTag("ResourceId", str(m["ResourceId"])))
	sb.WriteString(`<Tags>`)
	if tags, ok := m["Tags"].([]map[string]any); ok {
		for _, t := range tags {
			sb.WriteString(`<Tag>`)
			sb.WriteString(xmlTag("Key", str(t["Key"])))
			sb.WriteString(xmlTag("Value", str(t["Value"])))
			sb.WriteString(`</Tag>`)
		}
	}
	sb.WriteString(`</Tags>`)
	return sb.String()
}

func encodeHostedZone(v any) string {
	hz, ok := v.(map[string]any)
	if !ok {
		return ""
	}
	return `<HostedZone>` +
		xmlTag("Id", str(hz["Id"])) +
		xmlTag("Name", str(hz["Name"])) +
		`<Config>` + xmlTag("Comment", str(hz["Comment"])) + xmlTag("PrivateZone", str(hz["PrivateZone"])) + `</Config>` +
		`<ResourceRecordSetCount>` + str(hz["ResourceRecordSetCount"]) + `</ResourceRecordSetCount>` +
		`</HostedZone>`
}

func encodeRRSet(rr map[string]any) string {
	var sb strings.Builder
	sb.WriteString(`<ResourceRecordSet>`)
	sb.WriteString(xmlTag("Name", str(rr["Name"])))
	sb.WriteString(xmlTag("Type", str(rr["Type"])))
	sb.WriteString(xmlTag("TTL", str(rr["TTL"])))
	if records, ok := rr["ResourceRecords"].([]string); ok {
		sb.WriteString(`<ResourceRecords>`)
		for _, r := range records {
			sb.WriteString(`<ResourceRecord>` + xmlTag("Value", r) + `</ResourceRecord>`)
		}
		sb.WriteString(`</ResourceRecords>`)
	}
	sb.WriteString(`</ResourceRecordSet>`)
	return sb.String()
}

func encodeHealthCheck(v any) string {
	hc, ok := v.(map[string]any)
	if !ok {
		return ""
	}
	return `<HealthCheck>` +
		xmlTag("Id", str(hc["Id"])) +
		`<HealthCheckConfig>` +
		xmlTag("Type", str(hc["Type"])) +
		xmlTag("FullyQualifiedDomainName", str(hc["FullyQualifiedDomainName"])) +
		xmlTag("Port", str(hc["Port"])) +
		`</HealthCheckConfig>` +
		`<HealthCheckVersion>1</HealthCheckVersion>` +
		`</HealthCheck>`
}
