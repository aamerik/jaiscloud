package services

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
)

// CloudFrontCodec handles the CloudFront REST/XML protocol.
// Routes are path+method-based under /2020-05-31/...
type CloudFrontCodec struct{}

var _ adapter.Codec = (*CloudFrontCodec)(nil)

func (c *CloudFrontCodec) ServiceName() string { return "cloudfront" }

func (c *CloudFrontCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	action, params := cloudfrontActionFromRequest(r, body)
	if action == "" {
		return nil, fmt.Errorf("unrecognised CloudFront request: %s %s", r.Method, r.URL.Path)
	}
	return &model.NormalizedRequest{
		Service: "cloudfront",
		Action:  action,
		Params:  params,
		Raw:     r,
	}, nil
}

func (c *CloudFrontCodec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/xml")
	if etag, ok := resp.Data["_etag"].(string); ok {
		h.Set("ETag", etag)
	}
	body := buildCloudfrontXML(nr.Action, resp.Data)
	return resp.HTTPStatus, h, []byte(body)
}

func (c *CloudFrontCodec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/xml")
	body := fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<ErrorResponse>`+
			`<Error><Code>%s</Code><Message>%s</Message></Error>`+
			`</ErrorResponse>`,
		xmlEscape(perr.Code), xmlEscape(perr.Message),
	)
	return perr.HTTPStatus, h, []byte(body)
}

const cfBase = "/2020-05-31"

func cloudfrontActionFromRequest(r *http.Request, body []byte) (string, map[string]any) {
	path := strings.TrimPrefix(r.URL.Path, cfBase)
	path = strings.TrimSuffix(path, "/")
	method := r.Method
	params := map[string]any{}

	switch {
	// Distributions
	case path == "/distribution" && method == "POST":
		parseCloudfrontBody(body, params)
		return "CreateDistribution", params

	case path == "/distribution" && method == "GET":
		for k, v := range r.URL.Query() {
			if len(v) > 0 {
				params[k] = v[0]
			}
		}
		return "ListDistributions", params

	case strings.HasPrefix(path, "/distribution/") && !strings.Contains(path[len("/distribution/"):], "/"):
		id := strings.TrimPrefix(path, "/distribution/")
		params["Id"] = id
		if method == "GET" {
			return "GetDistribution", params
		}
		if method == "PUT" {
			params["IfMatch"] = r.Header.Get("If-Match")
			parseCloudfrontBody(body, params)
			return "UpdateDistribution", params
		}
		if method == "DELETE" {
			params["IfMatch"] = r.Header.Get("If-Match")
			return "DeleteDistribution", params
		}

	// Tags
	case path == "/tagging" && method == "POST":
		params["Resource"] = r.URL.Query().Get("Resource")
		parseCloudfrontBody(body, params)
		return "TagResource", params

	case path == "/tagging" && method == "PUT":
		params["Resource"] = r.URL.Query().Get("Resource")
		parseCloudfrontBody(body, params)
		return "UntagResource", params

	case path == "/tagging" && method == "GET":
		params["Resource"] = r.URL.Query().Get("Resource")
		return "ListTagsForResource", params
	}

	return "", nil
}

// parseCloudfrontBody loosely parses CloudFront XML bodies into a flat params map.
// Full XML unmarshalling is complex; we store the raw XML and do lazy parsing in the provider.
func parseCloudfrontBody(body []byte, params map[string]any) {
	if len(body) == 0 {
		return
	}
	params["_rawXML"] = string(body)

	// Extract key fields: Enabled, Comment, CallerReference
	var top struct {
		CallerReference string `xml:"CallerReference"`
		Comment         string `xml:"Comment"`
		Enabled         string `xml:"Enabled"`
		PriceClass      string `xml:"PriceClass"`
		HttpVersion     string `xml:"HttpVersion"`
		IsIPV6Enabled   string `xml:"IsIPV6Enabled"`
	}
	_ = xml.Unmarshal(body, &top)
	if top.CallerReference != "" {
		params["CallerReference"] = top.CallerReference
	}
	if top.Comment != "" {
		params["Comment"] = top.Comment
	}
	if top.Enabled != "" {
		params["Enabled"] = top.Enabled
	}
	if top.PriceClass != "" {
		params["PriceClass"] = top.PriceClass
	}
	if top.HttpVersion != "" {
		params["HttpVersion"] = top.HttpVersion
	}

	// Tags: <Tags><Items><Tag><Key>k</Key><Value>v</Value></Tag></Items></Tags>
	var tagDoc struct {
		Items []struct {
			Key   string `xml:"Key"`
			Value string `xml:"Value"`
		} `xml:"Items>Tag"`
	}
	if xml.Unmarshal(body, &tagDoc) == nil && len(tagDoc.Items) > 0 {
		tags := map[string]string{}
		for _, t := range tagDoc.Items {
			tags[t.Key] = t.Value
		}
		params["Tags"] = tags
	}
}

// buildCloudfrontXML wraps response data in a minimal CloudFront XML envelope.
func buildCloudfrontXML(action string, data map[string]any) string {
	// Remove internal metadata fields
	for _, k := range []string{"_etag"} {
		delete(data, k)
	}
	inner := encodeEC2Result(action, data) // reuse generic XML encoder
	return `<?xml version="1.0" encoding="UTF-8"?>` +
		`<` + action + `Response xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">` +
		inner +
		`</` + action + `Response>`
}
