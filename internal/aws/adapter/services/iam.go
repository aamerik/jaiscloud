package services

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
	"jaiscloud/internal/reqctx"
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

	params := flattenIAMQueryParams(values)

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
	reqID := reqctx.GetRequestID(nr.Raw.Context())
	if reqID == "" {
		reqID = "00000000-0000-0000-0000-000000000000"
	}
	body := buildIAMXML(nr.Action, nr.Service, resp.Data, reqID)
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
	reqID := ""
	if nr != nil && nr.Raw != nil {
		reqID = reqctx.GetRequestID(nr.Raw.Context())
	}
	if reqID == "" {
		reqID = "00000000-0000-0000-0000-000000000000"
	}
	var extra strings.Builder
	for k, v := range perr.Data {
		extra.WriteString(fmt.Sprintf("<%s>%s</%s>", xmlEscape(k), xmlEscape(fmt.Sprint(v)), xmlEscape(k)))
	}
	body := fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<ErrorResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">`+
			`<Error><Type>Sender</Type><Code>%s</Code><Message>%s</Message>%s</Error>`+
			`<RequestId>%s</RequestId>`+
			`</ErrorResponse>`,
		xmlEscape(code), xmlEscape(perr.Message), extra.String(), reqID,
	)
	return perr.HTTPStatus, h, []byte(body)
}

var iamErrorCodeMap = map[string]string{
	"NoSuchEntity":        "NoSuchEntity",
	"EntityAlreadyExists": "EntityAlreadyExists",
	"ValidationError":     "ValidationError",
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func isSTSAction(action string) bool {
	switch action {
	case "AssumeRole", "AssumeRoleWithSAML", "AssumeRoleWithWebIdentity",
		"GetCallerIdentity", "GetSessionToken", "GetFederationToken",
		"DecodeAuthorizationMessage", "GetAccessKeyInfo":
		return true
	}
	return false
}

// flattenIAMQueryParams extracts all IAM Query-protocol params from url.Values.
// Simple string keys are stored directly. Structured arrays (Tags, TagKeys,
// PolicyArns, ActionNames) are parsed into []any slices.
func flattenIAMQueryParams(values url.Values) map[string]any {
	params := map[string]any{}

	// Collect all simple (non-member) string values first.
	for k, vs := range values {
		if !strings.Contains(k, ".") && len(vs) > 0 {
			params[k] = vs[0]
		}
	}

	// Parse Tags.member.N.Key / Tags.member.N.Value → []any
	params["Tags"] = parseMemberKeyValue(values, "Tags")

	// Parse TagKeys.member.N → []any of strings
	params["TagKeys"] = parseMemberStrings(values, "TagKeys")

	// Parse PolicyArns.member.N.arn → []any (used by GetFederationToken etc.)
	params["PolicyArns"] = parseMemberStrings(values, "PolicyArns")

	// Parse ActionNames.member.N → []any of strings (SimulatePolicy)
	params["ActionNames"] = parseMemberStrings(values, "ActionNames")

	// Also grab dotted simple keys (e.g. from some SDK versions)
	for k, vs := range values {
		if _, exists := params[k]; !exists && len(vs) > 0 {
			params[k] = vs[0]
		}
	}

	// Coerce known integer fields from string to float64 so providers can use
	// a uniform float64 type assertion (matching JSON-decoded params).
	for _, key := range []string{"MaxItems", "MaxSessionDuration", "DurationSeconds"} {
		if s, ok := params[key].(string); ok && s != "" {
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				params[key] = f
			}
		}
	}

	return params
}

// parseMemberKeyValue parses prefix.member.N.Key / prefix.member.N.Value patterns.
func parseMemberKeyValue(values url.Values, prefix string) []any {
	var out []any
	for i := 1; ; i++ {
		k := values.Get(fmt.Sprintf("%s.member.%d.Key", prefix, i))
		v := values.Get(fmt.Sprintf("%s.member.%d.Value", prefix, i))
		if k == "" {
			break
		}
		out = append(out, map[string]any{"Key": k, "Value": v})
	}
	return out
}

// parseMemberStrings parses prefix.member.N patterns into a string slice.
func parseMemberStrings(values url.Values, prefix string) []any {
	var out []any
	for i := 1; ; i++ {
		v := values.Get(fmt.Sprintf("%s.member.%d", prefix, i))
		if v == "" {
			break
		}
		out = append(out, v)
	}
	return out
}

// buildIAMXML produces the AWS IAM/STS XML response envelope.
func buildIAMXML(action, service string, data map[string]any, reqID string) string {
	xmlns := "https://iam.amazonaws.com/doc/2010-05-08/"
	if service == "sts" {
		xmlns = "https://sts.amazonaws.com/doc/2011-06-15/"
	}
	if reqID == "" {
		reqID = "00000000-0000-0000-0000-000000000000"
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
		`<ResponseMetadata><RequestId>` + reqID + `</RequestId></ResponseMetadata>` +
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
			sb.WriteString("<Role>")
			sb.WriteString(roleInline(r))
			sb.WriteString("</Role>")
		}
	case "ListRoles":
		sb.WriteString("<Roles>")
		if roles, ok := data["Roles"].([]map[string]any); ok {
			for _, r := range roles {
				sb.WriteString("<member>")
				sb.WriteString(roleInline(r))
				sb.WriteString("</member>")
			}
		}
		sb.WriteString("</Roles>")
		sb.WriteString(xmlPaginationFields(data))
	case "CreatePolicy", "GetPolicy":
		if p, ok := data["Policy"].(map[string]any); ok {
			sb.WriteString("<Policy>")
			sb.WriteString(policyInline(p))
			sb.WriteString("</Policy>")
		}
	case "ListPolicies":
		sb.WriteString("<Policies>")
		if policies, ok := data["Policies"].([]map[string]any); ok {
			for _, p := range policies {
				sb.WriteString("<member>")
				sb.WriteString(policyInline(p))
				sb.WriteString("</member>")
			}
		}
		sb.WriteString("</Policies>")
		sb.WriteString(xmlPaginationFields(data))
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
		sb.WriteString(xmlPaginationFields(data))
	case "ListRolePolicies":
		sb.WriteString("<PolicyNames>")
		if names, ok := data["PolicyNames"].([]string); ok {
			for _, n := range names {
				sb.WriteString(xmlTag("member", n))
			}
		}
		sb.WriteString("</PolicyNames>")
		sb.WriteString(xmlPaginationFields(data))
	case "GetRolePolicy":
		sb.WriteString(xmlTag("RoleName", str(data["RoleName"])))
		sb.WriteString(xmlTag("PolicyName", str(data["PolicyName"])))
		sb.WriteString(xmlTag("PolicyDocument", str(data["PolicyDocument"])))
	case "CreateUser", "GetUser":
		if u, ok := data["User"].(map[string]any); ok {
			sb.WriteString("<User>")
			sb.WriteString(userInline(u))
			sb.WriteString("</User>")
		}
	case "ListUsers":
		sb.WriteString("<Users>")
		if users, ok := data["Users"].([]map[string]any); ok {
			for _, u := range users {
				sb.WriteString("<member>")
				sb.WriteString(userInline(u))
				sb.WriteString("</member>")
			}
		}
		sb.WriteString("</Users>")
		sb.WriteString(xmlPaginationFields(data))
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
		sb.WriteString(xmlPaginationFields(data))
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
		sb.WriteString(xmlPaginationFields(data))

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
		sb.WriteString(xmlTag("PackedPolicySize", str(data["PackedPolicySize"])))
	case "GetSessionToken":
		if creds, ok := data["Credentials"].(map[string]any); ok {
			sb.WriteString(encodeSTS(creds))
		}
	case "GetCallerIdentity":
		sb.WriteString(xmlTag("Account", str(data["Account"])))
		sb.WriteString(xmlTag("Arn", str(data["Arn"])))
		sb.WriteString(xmlTag("UserId", str(data["UserId"])))
	case "GetFederationToken":
		if creds, ok := data["Credentials"].(map[string]any); ok {
			sb.WriteString(encodeSTS(creds))
		}
		if fu, ok := data["FederatedUser"].(map[string]any); ok {
			sb.WriteString("<FederatedUser>")
			sb.WriteString(xmlTag("FederatedUserId", str(fu["FederatedUserId"])))
			sb.WriteString(xmlTag("Arn", str(fu["Arn"])))
			sb.WriteString("</FederatedUser>")
		}
		sb.WriteString(xmlTag("PackedPolicySize", str(data["PackedPolicySize"])))
	case "AssumeRoleWithWebIdentity":
		if creds, ok := data["Credentials"].(map[string]any); ok {
			sb.WriteString(encodeSTS(creds))
		}
		if aru, ok := data["AssumedRoleUser"].(map[string]any); ok {
			sb.WriteString("<AssumedRoleUser>")
			sb.WriteString(xmlTag("AssumedRoleId", str(aru["AssumedRoleId"])))
			sb.WriteString(xmlTag("Arn", str(aru["Arn"])))
			sb.WriteString("</AssumedRoleUser>")
		}
		sb.WriteString(xmlTag("SubjectFromWebIdentityToken", str(data["SubjectFromWebIdentityToken"])))
		sb.WriteString(xmlTag("PackedPolicySize", str(data["PackedPolicySize"])))
	case "AssumeRoleWithSAML":
		if creds, ok := data["Credentials"].(map[string]any); ok {
			sb.WriteString(encodeSTS(creds))
		}
		if aru, ok := data["AssumedRoleUser"].(map[string]any); ok {
			sb.WriteString("<AssumedRoleUser>")
			sb.WriteString(xmlTag("AssumedRoleId", str(aru["AssumedRoleId"])))
			sb.WriteString(xmlTag("Arn", str(aru["Arn"])))
			sb.WriteString("</AssumedRoleUser>")
		}
		sb.WriteString(xmlTag("Subject", str(data["Subject"])))
		sb.WriteString(xmlTag("SubjectType", str(data["SubjectType"])))
		sb.WriteString(xmlTag("Issuer", str(data["Issuer"])))
		sb.WriteString(xmlTag("Audience", str(data["Audience"])))
		sb.WriteString(xmlTag("NameQualifier", str(data["NameQualifier"])))
		sb.WriteString(xmlTag("PackedPolicySize", str(data["PackedPolicySize"])))
	case "DecodeAuthorizationMessage":
		sb.WriteString(xmlTag("DecodedMessage", str(data["DecodedMessage"])))
	case "GetAccessKeyInfo":
		sb.WriteString(xmlTag("Account", str(data["Account"])))

	// Groups
	case "CreateGroup":
		if g, ok := data["Group"].(map[string]any); ok {
			sb.WriteString("<Group>")
			sb.WriteString(groupInline(g))
			sb.WriteString("</Group>")
		}
	case "GetGroup":
		if g, ok := data["Group"].(map[string]any); ok {
			sb.WriteString("<Group>")
			sb.WriteString(groupInline(g))
			sb.WriteString("</Group>")
		}
		sb.WriteString("<Users>")
		if users, ok := data["Users"].([]map[string]any); ok {
			for _, u := range users {
				sb.WriteString("<member>")
				sb.WriteString(userInline(u))
				sb.WriteString("</member>")
			}
		}
		sb.WriteString("</Users>")
		sb.WriteString(xmlPaginationFields(data))
	case "ListGroups", "ListGroupsForUser":
		sb.WriteString("<Groups>")
		if groups, ok := data["Groups"].([]map[string]any); ok {
			for _, g := range groups {
				sb.WriteString("<member>")
				sb.WriteString(groupInline(g))
				sb.WriteString("</member>")
			}
		}
		sb.WriteString("</Groups>")
		sb.WriteString(xmlPaginationFields(data))

	// User policy attachments
	case "ListAttachedUserPolicies":
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
		sb.WriteString(xmlPaginationFields(data))
	case "ListUserPolicies":
		sb.WriteString("<PolicyNames>")
		if names, ok := data["PolicyNames"].([]string); ok {
			for _, n := range names {
				sb.WriteString(xmlTag("member", n))
			}
		}
		sb.WriteString("</PolicyNames>")
		sb.WriteString(xmlPaginationFields(data))
	case "GetUserPolicy":
		sb.WriteString(xmlTag("UserName", str(data["UserName"])))
		sb.WriteString(xmlTag("PolicyName", str(data["PolicyName"])))
		sb.WriteString(xmlTag("PolicyDocument", str(data["PolicyDocument"])))

	// User tags
	case "ListUserTags":
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
		sb.WriteString(xmlPaginationFields(data))

	// Instance profiles
	case "CreateInstanceProfile", "GetInstanceProfile":
		if ip, ok := data["InstanceProfile"].(map[string]any); ok {
			sb.WriteString("<InstanceProfile>")
			sb.WriteString(instanceProfileInline(ip))
			sb.WriteString("</InstanceProfile>")
		}
	case "ListInstanceProfiles":
		sb.WriteString("<InstanceProfiles>")
		if ips, ok := data["InstanceProfiles"].([]map[string]any); ok {
			for _, ip := range ips {
				sb.WriteString("<member>")
				sb.WriteString(instanceProfileInline(ip))
				sb.WriteString("</member>")
			}
		}
		sb.WriteString("</InstanceProfiles>")
		sb.WriteString(xmlPaginationFields(data))

	// Simulation
	case "SimulatePrincipalPolicy", "SimulateCustomPolicy":
		sb.WriteString("<EvaluationResults>")
		if results, ok := data["EvaluationResults"].([]map[string]any); ok {
			for _, r := range results {
				sb.WriteString("<member>")
				sb.WriteString(xmlTag("EvalActionName", str(r["EvalActionName"])))
				sb.WriteString(xmlTag("EvalDecision", str(r["EvalDecision"])))
				sb.WriteString("</member>")
			}
		}
		sb.WriteString("</EvaluationResults>")
		sb.WriteString(xmlPaginationFields(data))
	}
	return sb.String()
}

// xmlPaginationFields emits IsTruncated and, when present, Marker from the
// provider response map. All IAM list operations must use this instead of
// hardcoding IsTruncated=false so that paginated responses are correct.
func xmlPaginationFields(data map[string]any) string {
	truncated := "false"
	if v, ok := data["IsTruncated"].(bool); ok && v {
		truncated = "true"
	}
	s := xmlTag("IsTruncated", truncated)
	if m, ok := data["Marker"].(string); ok && m != "" {
		s += xmlTag("Marker", m)
	}
	return s
}

// roleInline outputs Role fields without a wrapper element (for use inside <member> or <Role>).
func roleInline(r map[string]any) string {
	var sb strings.Builder
	sb.WriteString(xmlTag("RoleName", str(r["RoleName"])))
	sb.WriteString(xmlTag("RoleId", str(r["RoleId"])))
	sb.WriteString(xmlTag("Arn", str(r["Arn"])))
	sb.WriteString(xmlTag("Path", str(r["Path"])))
	sb.WriteString(xmlTag("AssumeRolePolicyDocument", str(r["AssumeRolePolicyDocument"])))
	sb.WriteString(xmlTag("Description", str(r["Description"])))
	sb.WriteString(xmlTag("CreateDate", str(r["CreateDate"])))
	return sb.String()
}

func policyInline(p map[string]any) string {
	var sb strings.Builder
	sb.WriteString(xmlTag("PolicyName", str(p["PolicyName"])))
	sb.WriteString(xmlTag("PolicyId", str(p["PolicyId"])))
	sb.WriteString(xmlTag("Arn", str(p["Arn"])))
	sb.WriteString(xmlTag("Path", str(p["Path"])))
	sb.WriteString(xmlTag("AttachmentCount", str(p["AttachmentCount"])))
	sb.WriteString(xmlTag("CreateDate", str(p["CreateDate"])))
	sb.WriteString(xmlTag("UpdateDate", str(p["UpdateDate"])))
	return sb.String()
}

func userInline(u map[string]any) string {
	var sb strings.Builder
	sb.WriteString(xmlTag("UserName", str(u["UserName"])))
	sb.WriteString(xmlTag("UserId", str(u["UserId"])))
	sb.WriteString(xmlTag("Arn", str(u["Arn"])))
	sb.WriteString(xmlTag("Path", str(u["Path"])))
	sb.WriteString(xmlTag("CreateDate", str(u["CreateDate"])))
	return sb.String()
}

func groupInline(g map[string]any) string {
	var sb strings.Builder
	sb.WriteString(xmlTag("GroupName", str(g["GroupName"])))
	sb.WriteString(xmlTag("GroupId", str(g["GroupId"])))
	sb.WriteString(xmlTag("Arn", str(g["Arn"])))
	sb.WriteString(xmlTag("Path", str(g["Path"])))
	sb.WriteString(xmlTag("CreateDate", str(g["CreateDate"])))
	return sb.String()
}

func instanceProfileInline(ip map[string]any) string {
	var sb strings.Builder
	sb.WriteString(xmlTag("InstanceProfileName", str(ip["InstanceProfileName"])))
	sb.WriteString(xmlTag("InstanceProfileId", str(ip["InstanceProfileId"])))
	sb.WriteString(xmlTag("Arn", str(ip["Arn"])))
	sb.WriteString(xmlTag("Path", str(ip["Path"])))
	sb.WriteString(xmlTag("CreateDate", str(ip["CreateDate"])))
	sb.WriteString("<Roles>")
	if roles, ok := ip["Roles"].([]map[string]any); ok {
		for _, r := range roles {
			sb.WriteString("<member>")
			sb.WriteString(roleInline(r))
			sb.WriteString("</member>")
		}
	}
	sb.WriteString("</Roles>")
	return sb.String()
}

func encodeRole(r map[string]any) string {
	return "<Role>" + roleInline(r) + "</Role>"
}

func encodePolicy(p map[string]any) string { return "<Policy>" + policyInline(p) + "</Policy>" }
func encodeUser(u map[string]any) string   { return "<User>" + userInline(u) + "</User>" }

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
