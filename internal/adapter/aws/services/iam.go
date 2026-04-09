package services

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
)

// IAMCodec handles IAM and STS wire format (Query protocol + XML responses).
// It is registered under both "iam" and "sts" keys in the adapter codec map.
type IAMCodec struct{}

var _ adapter.Codec = (*IAMCodec)(nil)

func (c *IAMCodec) ServiceName() string { return "iam" }

// ─── Decode ───────────────────────────────────────────────────────────────────

func (c *IAMCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	values := mergeQueryAndForm(r, body)
	action := values.Get("Action")
	if action == "" {
		return nil, fmt.Errorf("missing Action parameter")
	}

	// Determine whether this is IAM or STS.
	service := "iam"
	if isSTSAction(action) {
		service = "sts"
	}

	params := flattenIAMParams(values)

	nr := &model.NormalizedRequest{
		Service: service,
		Action:  action,
		Params:  params,
		Raw:     r,
	}
	return nr, nil
}

// ─── Encode ───────────────────────────────────────────────────────────────────

func (c *IAMCodec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "text/xml")
	body := buildIAMXML(nr.Action, nr.Service, resp.Data)
	return resp.HTTPStatus, h, []byte(body)
}

// ─── EncodeError ──────────────────────────────────────────────────────────────

func (c *IAMCodec) EncodeError(nr *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "text/xml")
	code := iamErrorCodeMap[perr.Code]
	if code == "" {
		code = perr.Code
	}
	body := fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<ErrorResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">`+
			`<Error><Type>Sender</Type><Code>%s</Code><Message>%s</Message></Error>`+
			`<RequestId>00000000-0000-0000-0000-000000000000</RequestId>`+
			`</ErrorResponse>`,
		xmlEscape(code), xmlEscape(perr.Message),
	)
	return perr.HTTPStatus, h, []byte(body)
}

var iamErrorCodeMap = map[string]string{
	"NoSuchEntity":     "NoSuchEntity",
	"EntityAlreadyExists": "EntityAlreadyExists",
	"ValidationError":  "ValidationError",
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func isSTSAction(action string) bool {
	switch action {
	case "AssumeRole", "AssumeRoleWithSAML", "AssumeRoleWithWebIdentity",
		"GetCallerIdentity", "GetSessionToken", "GetFederationToken",
		"DecodeAuthorizationMessage":
		return true
	}
	return false
}

func flattenIAMParams(values interface{ Get(string) string }) map[string]any {
	type getter interface{ Get(string) string }
	v := values.(getter)
	params := map[string]any{}
	for _, k := range []string{
		"RoleName", "PolicyArn", "PolicyName", "PolicyDocument",
		"AssumeRolePolicyDocument", "Description", "Path",
		"UserName", "AccessKeyId", "GroupName",
		"RoleArn", "RoleSessionName", "DurationSeconds",
		"SerialNumber", "TokenCode",
	} {
		if val := v.Get(k); val != "" {
			params[k] = val
		}
	}
	return params
}

// buildIAMXML produces the AWS IAM/STS XML response envelope.
func buildIAMXML(action, service string, data map[string]any) string {
	xmlns := "https://iam.amazonaws.com/doc/2010-05-08/"
	if service == "sts" {
		xmlns = "https://sts.amazonaws.com/doc/2011-06-15/"
	}

	inner := buildIAMResult(action, data)

	// For actions that return nothing, omit the result element.
	resultXML := ""
	if inner != "" {
		resultXML = "<" + action + "Result>" + inner + "</" + action + "Result>"
	}

	return `<?xml version="1.0" encoding="UTF-8"?>` +
		`<` + action + `Response xmlns="` + xmlns + `">` +
		resultXML +
		`<ResponseMetadata><RequestId>00000000-0000-0000-0000-000000000000</RequestId></ResponseMetadata>` +
		`</` + action + `Response>`
}

func buildIAMResult(action string, data map[string]any) string {
	if data == nil {
		return ""
	}
	var sb strings.Builder
	switch action {
	case "CreateRole", "GetRole":
		if r, ok := data["Role"].(map[string]any); ok {
			sb.WriteString(encodeRole(r))
		}
	case "ListRoles":
		sb.WriteString("<Roles>")
		if roles, ok := data["Roles"].([]map[string]any); ok {
			for _, r := range roles {
				sb.WriteString("<member>")
				sb.WriteString(encodeRole(r))
				sb.WriteString("</member>")
			}
		}
		sb.WriteString("</Roles>")
		sb.WriteString(xmlTag("IsTruncated", "false"))
	case "CreatePolicy", "GetPolicy":
		if p, ok := data["Policy"].(map[string]any); ok {
			sb.WriteString(encodePolicy(p))
		}
	case "ListPolicies":
		sb.WriteString("<Policies>")
		if policies, ok := data["Policies"].([]map[string]any); ok {
			for _, p := range policies {
				sb.WriteString("<member>")
				sb.WriteString(encodePolicy(p))
				sb.WriteString("</member>")
			}
		}
		sb.WriteString("</Policies>")
		sb.WriteString(xmlTag("IsTruncated", "false"))
	case "ListAttachedRolePolicies":
		sb.WriteString("<AttachedPolicies>")
		if attached, ok := data["AttachedPolicies"].([]map[string]any); ok {
			for _, p := range attached {
				sb.WriteString("<member>")
				sb.WriteString(xmlTag("PolicyName", str(p["PolicyName"])))
				sb.WriteString(xmlTag("PolicyArn", str(p["PolicyArn"])))
				sb.WriteString("</member>")
			}
		}
		sb.WriteString("</AttachedPolicies>")
		sb.WriteString(xmlTag("IsTruncated", "false"))
	case "ListRolePolicies":
		sb.WriteString("<PolicyNames>")
		if names, ok := data["PolicyNames"].([]string); ok {
			for _, n := range names {
				sb.WriteString(xmlTag("member", n))
			}
		}
		sb.WriteString("</PolicyNames>")
		sb.WriteString(xmlTag("IsTruncated", "false"))
	case "GetRolePolicy":
		sb.WriteString(xmlTag("RoleName", str(data["RoleName"])))
		sb.WriteString(xmlTag("PolicyName", str(data["PolicyName"])))
		sb.WriteString(xmlTag("PolicyDocument", str(data["PolicyDocument"])))
	case "CreateUser", "GetUser":
		if u, ok := data["User"].(map[string]any); ok {
			sb.WriteString(encodeUser(u))
		}
	case "ListUsers":
		sb.WriteString("<Users>")
		if users, ok := data["Users"].([]map[string]any); ok {
			for _, u := range users {
				sb.WriteString("<member>")
				sb.WriteString(encodeUser(u))
				sb.WriteString("</member>")
			}
		}
		sb.WriteString("</Users>")
		sb.WriteString(xmlTag("IsTruncated", "false"))
	case "CreateAccessKey":
		if ak, ok := data["AccessKey"].(map[string]any); ok {
			sb.WriteString("<AccessKey>")
			sb.WriteString(xmlTag("AccessKeyId", str(ak["AccessKeyId"])))
			sb.WriteString(xmlTag("SecretAccessKey", str(ak["SecretAccessKey"])))
			sb.WriteString(xmlTag("Status", str(ak["Status"])))
			sb.WriteString(xmlTag("UserName", str(ak["UserName"])))
			sb.WriteString(xmlTag("CreateDate", str(ak["CreateDate"])))
			sb.WriteString("</AccessKey>")
		}
	case "ListAccessKeys":
		sb.WriteString("<AccessKeyMetadata>")
		if keys, ok := data["AccessKeyMetadata"].([]map[string]any); ok {
			for _, ak := range keys {
				sb.WriteString("<member>")
				sb.WriteString(xmlTag("AccessKeyId", str(ak["AccessKeyId"])))
				sb.WriteString(xmlTag("UserName", str(ak["UserName"])))
				sb.WriteString(xmlTag("Status", str(ak["Status"])))
				sb.WriteString("</member>")
			}
		}
		sb.WriteString("</AccessKeyMetadata>")
		sb.WriteString(xmlTag("IsTruncated", "false"))
	case "ListRoleTags":
		sb.WriteString("<Tags>")
		if tags, ok := data["Tags"].([]map[string]any); ok {
			for _, t := range tags {
				sb.WriteString("<member>")
				sb.WriteString(xmlTag("Key", str(t["Key"])))
				sb.WriteString(xmlTag("Value", str(t["Value"])))
				sb.WriteString("</member>")
			}
		}
		sb.WriteString("</Tags>")
		sb.WriteString(xmlTag("IsTruncated", "false"))

	// STS actions
	case "AssumeRole":
		if creds, ok := data["Credentials"].(map[string]any); ok {
			sb.WriteString(encodeSTS(creds))
		}
		if aru, ok := data["AssumedRoleUser"].(map[string]any); ok {
			sb.WriteString("<AssumedRoleUser>")
			sb.WriteString(xmlTag("AssumedRoleId", str(aru["AssumedRoleId"])))
			sb.WriteString(xmlTag("Arn", str(aru["Arn"])))
			sb.WriteString("</AssumedRoleUser>")
		}
	case "GetSessionToken":
		if creds, ok := data["Credentials"].(map[string]any); ok {
			sb.WriteString(encodeSTS(creds))
		}
	case "GetCallerIdentity":
		sb.WriteString(xmlTag("Account", str(data["Account"])))
		sb.WriteString(xmlTag("Arn", str(data["Arn"])))
		sb.WriteString(xmlTag("UserId", str(data["UserId"])))
	}
	return sb.String()
}

func encodeRole(r map[string]any) string {
	var sb strings.Builder
	sb.WriteString("<Role>")
	sb.WriteString(xmlTag("RoleName", str(r["RoleName"])))
	sb.WriteString(xmlTag("RoleId", str(r["RoleId"])))
	sb.WriteString(xmlTag("Arn", str(r["Arn"])))
	sb.WriteString(xmlTag("Path", str(r["Path"])))
	sb.WriteString(xmlTag("AssumeRolePolicyDocument", str(r["AssumeRolePolicyDocument"])))
	sb.WriteString(xmlTag("Description", str(r["Description"])))
	sb.WriteString(xmlTag("CreateDate", str(r["CreateDate"])))
	sb.WriteString("</Role>")
	return sb.String()
}

func encodePolicy(p map[string]any) string {
	var sb strings.Builder
	sb.WriteString("<Policy>")
	sb.WriteString(xmlTag("PolicyName", str(p["PolicyName"])))
	sb.WriteString(xmlTag("PolicyId", str(p["PolicyId"])))
	sb.WriteString(xmlTag("Arn", str(p["Arn"])))
	sb.WriteString(xmlTag("Path", str(p["Path"])))
	sb.WriteString(xmlTag("AttachmentCount", str(p["AttachmentCount"])))
	sb.WriteString(xmlTag("CreateDate", str(p["CreateDate"])))
	sb.WriteString(xmlTag("UpdateDate", str(p["UpdateDate"])))
	sb.WriteString("</Policy>")
	return sb.String()
}

func encodeUser(u map[string]any) string {
	var sb strings.Builder
	sb.WriteString("<User>")
	sb.WriteString(xmlTag("UserName", str(u["UserName"])))
	sb.WriteString(xmlTag("UserId", str(u["UserId"])))
	sb.WriteString(xmlTag("Arn", str(u["Arn"])))
	sb.WriteString(xmlTag("Path", str(u["Path"])))
	sb.WriteString(xmlTag("CreateDate", str(u["CreateDate"])))
	sb.WriteString("</User>")
	return sb.String()
}

func encodeSTS(creds map[string]any) string {
	var sb strings.Builder
	sb.WriteString("<Credentials>")
	sb.WriteString(xmlTag("AccessKeyId", str(creds["AccessKeyId"])))
	sb.WriteString(xmlTag("SecretAccessKey", str(creds["SecretAccessKey"])))
	sb.WriteString(xmlTag("SessionToken", str(creds["SessionToken"])))
	sb.WriteString(xmlTag("Expiration", str(creds["Expiration"])))
	sb.WriteString("</Credentials>")
	return sb.String()
}

// jsonToXMLParams decodes a JSON body into IAM params (for JSON-over-HTTP clients).
func jsonToXMLParams(body []byte) map[string]any {
	var params map[string]any
	_ = json.Unmarshal(body, &params)
	return params
}
