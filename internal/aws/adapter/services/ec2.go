package services

import (
	"fmt"
	"net/http"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
)

// EC2Codec handles the EC2 Query protocol (shared by EC2, VPC, RDS, ElastiCache, CloudFormation).
// Protocol: POST form-encoded body with Action=<Action> + XML responses.
type EC2Codec struct {
	service string // "ec2", "rds", "elasticache", "cloudformation"
}

func NewEC2Codec(service string) *EC2Codec { return &EC2Codec{service: service} }

var _ adapter.Codec = (*EC2Codec)(nil)

func (c *EC2Codec) ServiceName() string { return c.service }

func (c *EC2Codec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	values := mergeQueryAndForm(r, body)
	action := values.Get("Action")
	if action == "" {
		return nil, fmt.Errorf("missing Action parameter for %s", c.service)
	}
	// Flatten all form values into params map
	params := map[string]any{}
	for k, vs := range values {
		if len(vs) == 1 {
			params[k] = vs[0]
		} else if len(vs) > 1 {
			params[k] = vs
		}
	}
	return &model.NormalizedRequest{
		Service: c.service,
		Action:  action,
		Params:  params,
		Raw:     r,
	}, nil
}

func (c *EC2Codec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "text/xml;charset=UTF-8")
	body := buildEC2XML(nr.Action, resp.Data)
	return resp.HTTPStatus, h, []byte(body)
}

func (c *EC2Codec) EncodeError(nr *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "text/xml;charset=UTF-8")
	code := perr.Code
	body := fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<Response>`+
			`<Errors><Error><Code>%s</Code><Message>%s</Message></Error></Errors>`+
			`<RequestID>00000000-0000-0000-0000-000000000000</RequestID>`+
			`</Response>`,
		xmlEscape(code), xmlEscape(perr.Message),
	)
	return perr.HTTPStatus, h, []byte(body)
}

// buildEC2XML produces the EC2 XML response envelope.
func buildEC2XML(action string, data map[string]any) string {
	xmlns := "http://ec2.amazonaws.com/doc/2016-11-15/"
	inner := encodeEC2Result(action, data)
	return `<?xml version="1.0" encoding="UTF-8"?>` +
		`<` + action + `Response xmlns="` + xmlns + `">` +
		`<requestId>00000000-0000-0000-0000-000000000000</requestId>` +
		inner +
		`</` + action + `Response>`
}

// encodeEC2Result converts the provider response map into EC2-style XML elements.
func encodeEC2Result(action string, data map[string]any) string {
	if data == nil {
		return "<return>true</return>"
	}
	var sb strings.Builder
	switch action {
	// ── Instances ─────────────────────────────────────────────────────────────
	case "RunInstances":
		sb.WriteString(xmlTag("reservationId", str(data["ReservationId"])))
		sb.WriteString(xmlTag("ownerId", str(data["OwnerId"])))
		sb.WriteString("<instancesSet>")
		if items, ok := data["Instances"].([]map[string]any); ok {
			for _, inst := range items {
				sb.WriteString("<item>")
				sb.WriteString(encodeInstance(inst))
				sb.WriteString("</item>")
			}
		}
		sb.WriteString("</instancesSet>")
	case "DescribeInstances":
		sb.WriteString("<reservationSet>")
		if items, ok := data["Reservations"].([]map[string]any); ok {
			for _, res := range items {
				sb.WriteString("<item>")
				sb.WriteString(xmlTag("reservationId", str(res["ReservationId"])))
				sb.WriteString(xmlTag("ownerId", str(res["OwnerId"])))
				sb.WriteString("<instancesSet>")
				if insts, ok := res["Instances"].([]map[string]any); ok {
					for _, inst := range insts {
						sb.WriteString("<item>")
						sb.WriteString(encodeInstance(inst))
						sb.WriteString("</item>")
					}
				}
				sb.WriteString("</instancesSet>")
				sb.WriteString("</item>")
			}
		}
		sb.WriteString("</reservationSet>")
	case "TerminateInstances", "StartInstances", "StopInstances":
		sb.WriteString("<instancesSet>")
		if items, ok := data["TerminatingInstances"].([]map[string]any); ok {
			for _, s := range items {
				sb.WriteString("<item>")
				sb.WriteString(xmlTag("instanceId", str(s["InstanceId"])))
				sb.WriteString("<currentState>")
				if cs, ok := s["CurrentState"].(map[string]any); ok {
					sb.WriteString(xmlTag("code", str(cs["Code"])))
					sb.WriteString(xmlTag("name", str(cs["Name"])))
				}
				sb.WriteString("</currentState>")
				sb.WriteString("</item>")
			}
		}
		sb.WriteString("</instancesSet>")
	// ── Security Groups ───────────────────────────────────────────────────────
	case "CreateSecurityGroup":
		sb.WriteString(xmlTag("groupId", str(data["GroupId"])))
		sb.WriteString("<return>true</return>")
	case "DescribeSecurityGroups":
		sb.WriteString("<securityGroupInfo>")
		if groups, ok := data["SecurityGroups"].([]map[string]any); ok {
			for _, g := range groups {
				sb.WriteString("<item>")
				sb.WriteString(xmlTag("groupId", str(g["GroupId"])))
				sb.WriteString(xmlTag("groupName", str(g["GroupName"])))
				sb.WriteString(xmlTag("groupDescription", str(g["Description"])))
				sb.WriteString(xmlTag("vpcId", str(g["VpcId"])))
				sb.WriteString(xmlTag("ownerId", str(g["OwnerId"])))
				sb.WriteString(encodeSGIPPermissions("ipPermissions", g["IngressRules"]))
				sb.WriteString(encodeSGIPPermissions("ipPermissionsEgress", g["EgressRules"]))
				sb.WriteString("</item>")
			}
		}
		sb.WriteString("</securityGroupInfo>")
	// ── Key Pairs ─────────────────────────────────────────────────────────────
	case "CreateKeyPair":
		sb.WriteString(xmlTag("keyName", str(data["KeyName"])))
		sb.WriteString(xmlTag("keyFingerprint", str(data["KeyFingerprint"])))
		sb.WriteString(xmlTag("keyMaterial", str(data["KeyMaterial"])))
		sb.WriteString(xmlTag("keyPairId", str(data["KeyPairId"])))
	case "DescribeKeyPairs":
		sb.WriteString("<keySet>")
		if pairs, ok := data["KeyPairs"].([]map[string]any); ok {
			for _, kp := range pairs {
				sb.WriteString("<item>")
				sb.WriteString(xmlTag("keyName", str(kp["KeyName"])))
				sb.WriteString(xmlTag("keyFingerprint", str(kp["KeyFingerprint"])))
				sb.WriteString(xmlTag("keyPairId", str(kp["KeyPairId"])))
				sb.WriteString("</item>")
			}
		}
		sb.WriteString("</keySet>")
	case "ImportKeyPair":
		sb.WriteString(xmlTag("keyName", str(data["KeyName"])))
		sb.WriteString(xmlTag("keyFingerprint", str(data["KeyFingerprint"])))
		sb.WriteString(xmlTag("keyPairId", str(data["KeyPairId"])))
	// ── AMIs ──────────────────────────────────────────────────────────────────
	case "DescribeImages":
		sb.WriteString("<imagesSet>")
		if images, ok := data["Images"].([]map[string]any); ok {
			for _, img := range images {
				sb.WriteString("<item>")
				sb.WriteString(xmlTag("imageId", str(img["ImageId"])))
				sb.WriteString(xmlTag("name", str(img["Name"])))
				sb.WriteString(xmlTag("imageState", str(img["State"])))
				sb.WriteString(xmlTag("architecture", str(img["Architecture"])))
				sb.WriteString(xmlTag("imageType", str(img["ImageType"])))
				sb.WriteString(xmlTag("ownerId", str(img["OwnerId"])))
				sb.WriteString("</item>")
			}
		}
		sb.WriteString("</imagesSet>")
	// ── VPC ───────────────────────────────────────────────────────────────────
	case "CreateVpc":
		if vpc, ok := data["Vpc"].(map[string]any); ok {
			sb.WriteString("<vpc>")
			sb.WriteString(encodeVpc(vpc))
			sb.WriteString("</vpc>")
		}
	case "DescribeVpcs":
		sb.WriteString("<vpcSet>")
		if vpcs, ok := data["Vpcs"].([]map[string]any); ok {
			for _, vpc := range vpcs {
				sb.WriteString("<item>")
				sb.WriteString(encodeVpc(vpc))
				sb.WriteString("</item>")
			}
		}
		sb.WriteString("</vpcSet>")
	// ── Subnets ───────────────────────────────────────────────────────────────
	case "CreateSubnet":
		if sn, ok := data["Subnet"].(map[string]any); ok {
			sb.WriteString("<subnet>")
			sb.WriteString(encodeSubnet(sn))
			sb.WriteString("</subnet>")
		}
	case "DescribeSubnets":
		sb.WriteString("<subnetSet>")
		if subnets, ok := data["Subnets"].([]map[string]any); ok {
			for _, sn := range subnets {
				sb.WriteString("<item>")
				sb.WriteString(encodeSubnet(sn))
				sb.WriteString("</item>")
			}
		}
		sb.WriteString("</subnetSet>")
	// ── Internet Gateways ─────────────────────────────────────────────────────
	case "CreateInternetGateway":
		if igw, ok := data["InternetGateway"].(map[string]any); ok {
			sb.WriteString("<internetGateway>")
			sb.WriteString(xmlTag("internetGatewayId", str(igw["InternetGatewayId"])))
			sb.WriteString(xmlTag("ownerId", str(igw["OwnerId"])))
			sb.WriteString("<attachmentSet/>")
			sb.WriteString("</internetGateway>")
		}
	case "DescribeInternetGateways":
		sb.WriteString("<internetGatewaySet>")
		if igws, ok := data["InternetGateways"].([]map[string]any); ok {
			for _, igw := range igws {
				sb.WriteString("<item>")
				sb.WriteString(xmlTag("internetGatewayId", str(igw["InternetGatewayId"])))
				sb.WriteString(xmlTag("ownerId", str(igw["OwnerId"])))
				attachments := ""
				if atts, ok := igw["Attachments"].([]map[string]any); ok && len(atts) > 0 {
					for _, att := range atts {
						attachments += "<item>" +
							xmlTag("vpcId", str(att["VpcId"])) +
							xmlTag("state", str(att["State"])) +
							"</item>"
					}
				}
				if attachments != "" {
					sb.WriteString("<attachmentSet>" + attachments + "</attachmentSet>")
				} else {
					sb.WriteString("<attachmentSet/>")
				}
				sb.WriteString("</item>")
			}
		}
		sb.WriteString("</internetGatewaySet>")
	// ── Route Tables ──────────────────────────────────────────────────────────
	case "CreateRouteTable":
		if rt, ok := data["RouteTable"].(map[string]any); ok {
			sb.WriteString("<routeTable>")
			sb.WriteString(encodeRouteTable(rt))
			sb.WriteString("</routeTable>")
		}
	case "DescribeRouteTables":
		sb.WriteString("<routeTableSet>")
		if rts, ok := data["RouteTables"].([]map[string]any); ok {
			for _, rt := range rts {
				sb.WriteString("<item>")
				sb.WriteString(encodeRouteTable(rt))
				sb.WriteString("</item>")
			}
		}
		sb.WriteString("</routeTableSet>")
	case "AssociateRouteTable":
		sb.WriteString(xmlTag("associationId", str(data["AssociationId"])))
	// ── NAT Gateways ──────────────────────────────────────────────────────────
	case "CreateNatGateway":
		if ngw, ok := data["NatGateway"].(map[string]any); ok {
			sb.WriteString("<natGateway>")
			sb.WriteString(encodeNatGateway(ngw))
			sb.WriteString("</natGateway>")
		}
	case "DescribeNatGateways":
		sb.WriteString("<natGatewaySet>")
		if ngws, ok := data["NatGateways"].([]map[string]any); ok {
			for _, ngw := range ngws {
				sb.WriteString("<item>")
				sb.WriteString(encodeNatGateway(ngw))
				sb.WriteString("</item>")
			}
		}
		sb.WriteString("</natGatewaySet>")
	default:
		sb.WriteString("<return>true</return>")
	}
	return sb.String()
}

func encodeInstance(inst map[string]any) string {
	var sb strings.Builder
	sb.WriteString(xmlTag("instanceId", str(inst["InstanceId"])))
	sb.WriteString(xmlTag("imageId", str(inst["ImageId"])))
	sb.WriteString(xmlTag("instanceType", str(inst["InstanceType"])))
	sb.WriteString(xmlTag("keyName", str(inst["KeyName"])))
	sb.WriteString(xmlTag("privateDnsName", str(inst["PrivateDnsName"])))
	sb.WriteString(xmlTag("privateIpAddress", str(inst["PrivateIpAddress"])))
	sb.WriteString(xmlTag("subnetId", str(inst["SubnetId"])))
	sb.WriteString(xmlTag("vpcId", str(inst["VpcId"])))
	sb.WriteString("<instanceState>")
	if s, ok := inst["State"].(map[string]any); ok {
		sb.WriteString(xmlTag("code", str(s["Code"])))
		sb.WriteString(xmlTag("name", str(s["Name"])))
	}
	sb.WriteString("</instanceState>")
	sb.WriteString(xmlTag("launchTime", str(inst["LaunchTime"])))
	return sb.String()
}

func encodeVpc(vpc map[string]any) string {
	var sb strings.Builder
	sb.WriteString(xmlTag("vpcId", str(vpc["VpcId"])))
	sb.WriteString(xmlTag("state", str(vpc["State"])))
	sb.WriteString(xmlTag("cidrBlock", str(vpc["CidrBlock"])))
	sb.WriteString(xmlTag("isDefault", str(vpc["IsDefault"])))
	sb.WriteString(xmlTag("ownerId", str(vpc["OwnerId"])))
	return sb.String()
}

func encodeSubnet(sn map[string]any) string {
	var sb strings.Builder
	sb.WriteString(xmlTag("subnetId", str(sn["SubnetId"])))
	sb.WriteString(xmlTag("state", str(sn["State"])))
	sb.WriteString(xmlTag("vpcId", str(sn["VpcId"])))
	sb.WriteString(xmlTag("cidrBlock", str(sn["CidrBlock"])))
	sb.WriteString(xmlTag("availableIpAddressCount", str(sn["AvailableIpAddressCount"])))
	sb.WriteString(xmlTag("availabilityZone", str(sn["AvailabilityZone"])))
	return sb.String()
}

func encodeRouteTable(rt map[string]any) string {
	var sb strings.Builder
	sb.WriteString(xmlTag("routeTableId", str(rt["RouteTableId"])))
	sb.WriteString(xmlTag("vpcId", str(rt["VpcId"])))
	sb.WriteString(xmlTag("ownerId", str(rt["OwnerId"])))
	sb.WriteString("<routeSet>")
	if routes, ok := rt["Routes"].([]map[string]any); ok {
		for _, r := range routes {
			sb.WriteString("<item>")
			sb.WriteString(xmlTag("destinationCidrBlock", str(r["DestinationCidrBlock"])))
			sb.WriteString(xmlTag("gatewayId", str(r["GatewayId"])))
			sb.WriteString(xmlTag("state", str(r["State"])))
			sb.WriteString("</item>")
		}
	}
	sb.WriteString("</routeSet>")
	sb.WriteString("<associationSet/>")
	return sb.String()
}

func encodeNatGateway(ngw map[string]any) string {
	var sb strings.Builder
	sb.WriteString(xmlTag("natGatewayId", str(ngw["NatGatewayId"])))
	sb.WriteString(xmlTag("vpcId", str(ngw["VpcId"])))
	sb.WriteString(xmlTag("subnetId", str(ngw["SubnetId"])))
	sb.WriteString(xmlTag("state", str(ngw["State"])))
	sb.WriteString(xmlTag("createTime", str(ngw["CreateTime"])))
	return sb.String()
}

// encodeSGIPPermissions serialises a slice of SG rules as <ipPermissions> or
// <ipPermissionsEgress> XML items.
func encodeSGIPPermissions(label string, v any) string {
	rules, ok := v.([]map[string]any)
	if !ok || len(rules) == 0 {
		return "<" + label + "/>"
	}
	var sb strings.Builder
	sb.WriteString("<" + label + ">")
	for _, r := range rules {
		sb.WriteString("<item>")
		sb.WriteString(xmlTag("ipProtocol", str(r["IpProtocol"])))
		sb.WriteString(xmlTag("fromPort", str(r["FromPort"])))
		sb.WriteString(xmlTag("toPort", str(r["ToPort"])))
		if ranges, ok := r["IpRanges"].([]map[string]any); ok && len(ranges) > 0 {
			sb.WriteString("<ipRanges>")
			for _, rng := range ranges {
				sb.WriteString("<item>")
				sb.WriteString(xmlTag("cidrIp", str(rng["CidrIp"])))
				if desc := str(rng["Description"]); desc != "" {
					sb.WriteString(xmlTag("description", desc))
				}
				sb.WriteString("</item>")
			}
			sb.WriteString("</ipRanges>")
		} else {
			sb.WriteString("<ipRanges/>")
		}
		sb.WriteString("</item>")
	}
	sb.WriteString("</" + label + ">")
	return sb.String()
}
