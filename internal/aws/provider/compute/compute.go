// Package compute implements the EC2 and VPC provider (ComputeProvider).
package compute

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/pagination"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// ComputeProvider handles EC2 instances, VPC networking, security groups, and key pairs.
type ComputeProvider struct {
	resources store.ResourceStore
	accountID string
	region    string
}

func New(resources store.ResourceStore, accountID, region string) *ComputeProvider {
	p := &ComputeProvider{resources: resources, accountID: accountID, region: region}
	p.seedDefaultVPC(context.Background(), accountID, region)
	return p
}

// seedDefaultVPC creates a default VPC + subnets for the given account/region if none exist yet.
// It is called both at startup (for the configured default account) and lazily for any
// other account that first issues a DescribeVpcs or similar request.
func (p *ComputeProvider) seedDefaultVPC(ctx context.Context, accountID, region string) {
	// Return if a default VPC already exists (e.g. restarted with persistent mode).
	entries, _ := p.resources.List(ctx, accountID, region, rtVpc, "")
	for _, e := range entries {
		var vpc ec2Vpc
		json.Unmarshal(e.Data, &vpc)
		if vpc.IsDefault {
			return
		}
	}
	// Use a suffix derived from the account so IDs are unique across accounts.
	acctSuffix := accountID
	if len(acctSuffix) > 8 {
		acctSuffix = acctSuffix[len(acctSuffix)-8:]
	}
	vpcId := "vpc-dflt" + acctSuffix
	vpc := ec2Vpc{
		VpcId:              vpcId,
		State:              "available",
		CidrBlock:          "172.31.0.0/16",
		IsDefault:          true,
		EnableDnsSupport:   true,
		EnableDnsHostnames: true,
	}
	data, _ := json.Marshal(vpc)
	_ = p.resources.Create(ctx, accountID, region, store.ResourceEntry{Type: rtVpc, ID: vpcId, Data: data, Seeded: true})

	// Seed three default subnets in different AZs.
	azCidrs := [][2]string{
		{"us-east-1a", "172.31.0.0/20"},
		{"us-east-1b", "172.31.16.0/20"},
		{"us-east-1c", "172.31.32.0/20"},
	}
	for i, azCidr := range azCidrs {
		subnetId := fmt.Sprintf("subnet-dflt%s%04d", acctSuffix, i+1)
		subnet := ec2Subnet{
			SubnetId:                subnetId,
			VpcId:                   vpcId,
			CidrBlock:               azCidr[1],
			AvailabilityZone:        azCidr[0],
			AvailableIpAddressCount: 4091,
			State:                   "available",
			IsDefault:               true,
			MapPublicIpOnLaunch:     true,
		}
		sdata, _ := json.Marshal(subnet)
		_ = p.resources.Create(ctx, accountID, region, store.ResourceEntry{Type: rtSubnet, ID: subnetId, Data: sdata, Seeded: true})
	}
}

// Reset implements admin.Resetter — reseeds the default VPC after a store wipe.
func (p *ComputeProvider) Reset(ctx context.Context) {
	p.seedDefaultVPC(context.Background(), p.accountID, p.region)
}

func (p *ComputeProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		// Instances
		"Compute.RunInstances":                   p.RunInstances,
		"Compute.DescribeInstances":              p.DescribeInstances,
		"Compute.TerminateInstances":             p.TerminateInstances,
		"Compute.StartInstances":                 p.StartInstances,
		"Compute.StopInstances":                  p.StopInstances,
		"Compute.RebootInstances":                p.RebootInstances,
		"Compute.ModifyInstanceAttribute":        p.ModifyInstanceAttribute,
		"Compute.DescribeInstanceAttribute":      p.DescribeInstanceAttribute,
		"Compute.DescribeInstanceStatus":         p.DescribeInstanceStatus,
		"Compute.AssociateIamInstanceProfile":    p.AssociateIamInstanceProfile,
		"Compute.DisassociateIamInstanceProfile": p.DisassociateIamInstanceProfile,
		// AMIs
		"Compute.DescribeImages": p.DescribeImages,
		// Security Groups
		"Compute.CreateSecurityGroup":           p.CreateSecurityGroup,
		"Compute.DescribeSecurityGroups":        p.DescribeSecurityGroups,
		"Compute.DeleteSecurityGroup":           p.DeleteSecurityGroup,
		"Compute.AuthorizeSecurityGroupIngress": p.AuthorizeSecurityGroupIngress,
		"Compute.AuthorizeSecurityGroupEgress":  p.AuthorizeSecurityGroupEgress,
		"Compute.RevokeSecurityGroupIngress":    p.RevokeSecurityGroupIngress,
		// Key Pairs
		"Compute.CreateKeyPair":    p.CreateKeyPair,
		"Compute.DescribeKeyPairs": p.DescribeKeyPairs,
		"Compute.DeleteKeyPair":    p.DeleteKeyPair,
		"Compute.ImportKeyPair":    p.ImportKeyPair,
		// VPC
		"Compute.CreateVpc":          p.CreateVpc,
		"Compute.DescribeVpcs":       p.DescribeVpcs,
		"Compute.DeleteVpc":          p.DeleteVpc,
		"Compute.ModifyVpcAttribute": p.ModifyVpcAttribute,
		// Subnets
		"Compute.CreateSubnet":    p.CreateSubnet,
		"Compute.DescribeSubnets": p.DescribeSubnets,
		"Compute.DeleteSubnet":    p.DeleteSubnet,
		// Internet Gateways
		"Compute.CreateInternetGateway":    p.CreateInternetGateway,
		"Compute.DescribeInternetGateways": p.DescribeInternetGateways,
		"Compute.DeleteInternetGateway":    p.DeleteInternetGateway,
		"Compute.AttachInternetGateway":    p.AttachInternetGateway,
		"Compute.DetachInternetGateway":    p.DetachInternetGateway,
		// Route Tables
		"Compute.CreateRouteTable":    p.CreateRouteTable,
		"Compute.DescribeRouteTables": p.DescribeRouteTables,
		"Compute.DeleteRouteTable":    p.DeleteRouteTable,
		"Compute.CreateRoute":         p.CreateRoute,
		"Compute.DeleteRoute":         p.DeleteRoute,
		"Compute.AssociateRouteTable": p.AssociateRouteTable,
		// NAT Gateways
		"Compute.CreateNatGateway":    p.CreateNatGateway,
		"Compute.DescribeNatGateways": p.DescribeNatGateways,
		"Compute.DeleteNatGateway":    p.DeleteNatGateway,
		// Tags (14.5)
		"Compute.CreateTags":   p.CreateTags,
		"Compute.DeleteTags":   p.DeleteTags,
		"Compute.DescribeTags": p.DescribeTags,
	}
}

// ─── Resource types ───────────────────────────────────────────────────────────

const (
	rtInstance      = "ec2_instance"
	rtSecurityGroup = "ec2_security_group"
	rtKeyPair       = "ec2_key_pair"
	rtVpc           = "ec2_vpc"
	rtSubnet        = "ec2_subnet"
	rtIGW           = "ec2_igw"
	rtRouteTable    = "ec2_route_table"
	rtNatGateway    = "ec2_nat_gateway"
)

// tagKeyCustomID is a special tag that lets callers specify their own IDs for
// resources instead of getting random one.
const tagKeyCustomID = "_custom_id_"

// ─── ID generators ────────────────────────────────────────────────────────────

func newID(prefix string) string {
	b := make([]byte, 4)
	rand.Read(b)
	return fmt.Sprintf("%s-%08x", prefix, b)
}

// ─── Instance metadata ────────────────────────────────────────────────────────

type ec2Instance struct {
	InstanceId             string            `json:"InstanceId"`
	ImageId                string            `json:"ImageId"`
	InstanceType           string            `json:"InstanceType"`
	KeyName                string            `json:"KeyName"`
	SubnetId               string            `json:"SubnetId"`
	VpcId                  string            `json:"VpcId"`
	SecurityGroupIds       []string          `json:"SecurityGroupIds"`
	PrivateIpAddress       string            `json:"PrivateIpAddress"`
	PrivateDnsName         string            `json:"PrivateDnsName"`
	State                  string            `json:"State"` // pending, running, stopping, stopped, terminated
	Tags                   map[string]string `json:"Tags"`
	LaunchTime             time.Time         `json:"LaunchTime"`
	UserData               string            `json:"UserData,omitempty"`
	IamInstanceProfileArn  string            `json:"IamInstanceProfileArn,omitempty"`
	IamInstanceProfileName string            `json:"IamInstanceProfileName,omitempty"`
	DisableApiTermination  bool              `json:"DisableApiTermination,omitempty"`
	LastRebootTime         string            `json:"LastRebootTime,omitempty"`
}

func (p *ComputeProvider) saveInstance(ctx context.Context, account, region string, inst ec2Instance) error {
	data, _ := json.Marshal(inst)
	entry := store.ResourceEntry{Type: rtInstance, ID: inst.InstanceId, Data: data}
	return p.resources.Upsert(ctx, account, region, entry)
}

func (p *ComputeProvider) loadInstance(ctx context.Context, account, region, id string) (ec2Instance, error) {
	e, err := p.resources.Get(ctx, account, region, rtInstance, id)
	if err == store.ErrNotFound {
		return ec2Instance{}, &model.ProviderError{Code: "InvalidInstanceID.NotFound", Message: fmt.Sprintf("The instance ID '%s' does not exist", id), HTTPStatus: http.StatusBadRequest}
	}
	if err != nil {
		return ec2Instance{}, err
	}
	var inst ec2Instance
	json.Unmarshal(e.Data, &inst)
	return inst, nil
}

func instanceToWire(inst ec2Instance) map[string]any {
	sgList := make([]map[string]any, len(inst.SecurityGroupIds))
	for i, id := range inst.SecurityGroupIds {
		sgList[i] = map[string]any{"GroupId": id}
	}
	tagList := make([]map[string]any, 0, len(inst.Tags))
	for k, v := range inst.Tags {
		tagList = append(tagList, map[string]any{"Key": k, "Value": v})
	}
	w := map[string]any{
		"InstanceId":       inst.InstanceId,
		"ImageId":          inst.ImageId,
		"InstanceType":     inst.InstanceType,
		"KeyName":          inst.KeyName,
		"SubnetId":         inst.SubnetId,
		"VpcId":            inst.VpcId,
		"PrivateIpAddress": inst.PrivateIpAddress,
		"PrivateDnsName":   inst.PrivateDnsName,
		"State":            map[string]any{"Code": stateCode(inst.State), "Name": inst.State},
		"LaunchTime":       inst.LaunchTime.UTC().Format(time.RFC3339),
		"SecurityGroups":   sgList,
		"Tags":             tagList,
	}
	if inst.UserData != "" {
		w["UserData"] = inst.UserData
	}
	if inst.IamInstanceProfileArn != "" {
		w["IamInstanceProfile"] = map[string]any{"Arn": inst.IamInstanceProfileArn, "Id": newID("aipa")}
	}
	return w
}

func stateCode(state string) string {
	switch state {
	case "pending":
		return "0"
	case "running":
		return "16"
	case "shutting-down":
		return "32"
	case "terminated":
		return "48"
	case "stopping":
		return "64"
	case "stopped":
		return "80"
	}
	return "0"
}

// ─── Instance operations ──────────────────────────────────────────────────────

func (p *ComputeProvider) RunInstances(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	imageId := strParam(nr.Params, "ImageId")
	instanceType := strParam(nr.Params, "InstanceType")
	if instanceType == "" {
		instanceType = "t3.micro"
	}
	minCount := intParam(nr.Params, "MinCount", 1)
	maxCount := intParam(nr.Params, "MaxCount", minCount)
	if maxCount < minCount {
		maxCount = minCount
	}
	keyName := strParam(nr.Params, "KeyName")
	subnetId := strParam(nr.Params, "SubnetId")
	userData := strParam(nr.Params, "UserData")

	// If no subnet specified, place in the first default subnet (seeded at startup).
	defaultVpcId := ""
	if subnetId == "" {
		snEntries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, rtSubnet, "")
		for _, se := range snEntries {
			var sn ec2Subnet
			if json.Unmarshal(se.Data, &sn) == nil && sn.IsDefault {
				subnetId = sn.SubnetId
				defaultVpcId = sn.VpcId
				break
			}
		}
	}

	// SecurityGroupIds.N
	sgIds := extractIndexedParam(nr.Params, "SecurityGroupId")

	// IamInstanceProfile
	var iamArn, iamName string
	if iap, ok := nr.Params["IamInstanceProfile"].(map[string]any); ok {
		iamArn, _ = iap["Arn"].(string)
		iamName, _ = iap["Name"].(string)
	} else {
		iamArn = strParam(nr.Params, "IamInstanceProfile.Arn")
		iamName = strParam(nr.Params, "IamInstanceProfile.Name")
	}

	// TagSpecifications — collect tags destined for instance resources
	tags := extractTagSpecTags(nr.Params, "instance")

	instances := make([]map[string]any, 0, maxCount)
	reservationId := newID("r")
	for i := 0; i < maxCount; i++ {
		inst := ec2Instance{
			InstanceId:             newID("i"),
			ImageId:                imageId,
			InstanceType:           instanceType,
			KeyName:                keyName,
			SubnetId:               subnetId,
			VpcId:                  defaultVpcId,
			SecurityGroupIds:       sgIds,
			PrivateIpAddress:       fmt.Sprintf("10.0.%d.%d", i/256, i%256+10),
			State:                  "pending",
			LaunchTime:             time.Now(),
			UserData:               userData,
			IamInstanceProfileArn:  iamArn,
			IamInstanceProfileName: iamName,
			Tags:                   tags,
		}
		inst.PrivateDnsName = fmt.Sprintf("ip-%s.ec2.internal", strings.ReplaceAll(inst.PrivateIpAddress, ".", "-"))
		if err := p.saveInstance(ctx, nr.AccountID, nr.Region, inst); err != nil {
			return nil, err
		}
		// Transition pending → running after 2s; skip if instance was deleted (reset).
		instCopy := inst
		account, region := nr.AccountID, nr.Region
		time.AfterFunc(2*time.Second, func() {
			loaded, err := p.loadInstance(context.Background(), account, region, instCopy.InstanceId)
			if err != nil || loaded.State != "pending" {
				return
			}
			loaded.State = "running"
			p.saveInstance(context.Background(), account, region, loaded)
		})
		instances = append(instances, instanceToWire(inst))
	}
	return provider.OK(map[string]any{
		"ReservationId": reservationId,
		"OwnerId":       nr.AccountID,
		"Instances":     instances,
	}), nil
}

// extractTagSpecTags collects tags from TagSpecification.N.Tag.M for resources of the given type.
func extractTagSpecTags(params map[string]any, resourceType string) map[string]string {
	tags := map[string]string{}
	for ts := 1; ; ts++ {
		rt := strParam(params, fmt.Sprintf("TagSpecification.%d.ResourceType", ts))
		if rt == "" {
			break
		}
		if rt != resourceType {
			continue
		}
		for t := 1; ; t++ {
			k := strParam(params, fmt.Sprintf("TagSpecification.%d.Tag.%d.Key", ts, t))
			if k == "" {
				break
			}
			v := strParam(params, fmt.Sprintf("TagSpecification.%d.Tag.%d.Value", ts, t))
			tags[k] = v
		}
	}
	return tags
}

// parseEC2Filters returns a map of filter-name → accepted values from Filter.N.Name/Value.M params.
func parseEC2Filters(params map[string]any) map[string][]string {
	filters := map[string][]string{}
	for i := 1; ; i++ {
		name := strParam(params, fmt.Sprintf("Filter.%d.Name", i))
		if name == "" {
			break
		}
		var vals []string
		for j := 1; ; j++ {
			v := strParam(params, fmt.Sprintf("Filter.%d.Value.%d", i, j))
			if v == "" {
				break
			}
			vals = append(vals, v)
		}
		filters[name] = vals
	}
	return filters
}

// matchInstance returns true if inst matches all provided filters (AND across names, OR across values).
func matchInstance(inst ec2Instance, filters map[string][]string) bool {
	for name, vals := range filters {
		switch name {
		case "instance-state-name":
			if !containsStr(vals, inst.State) {
				return false
			}
		case "instance-type":
			if !containsStr(vals, inst.InstanceType) {
				return false
			}
		case "vpc-id":
			if !containsStr(vals, inst.VpcId) {
				return false
			}
		case "subnet-id":
			if !containsStr(vals, inst.SubnetId) {
				return false
			}
		case "tag-key":
			found := false
			for k := range inst.Tags {
				if containsStr(vals, k) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		default:
			if strings.HasPrefix(name, "tag:") {
				tagKey := name[4:]
				tagVal, ok := inst.Tags[tagKey]
				if !ok || !containsStr(vals, tagVal) {
					return false
				}
			}
		}
	}
	return true
}

func (p *ComputeProvider) DescribeInstances(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	filterIds := extractIndexedParam(nr.Params, "InstanceId")
	filters := parseEC2Filters(nr.Params)
	// Only suppress terminated/shutting-down instances when the caller has not
	// explicitly asked for those states via an instance-state-name filter.
	_, hasStateFilter := filters["instance-state-name"]
	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, rtInstance, "")
	if err != nil {
		return nil, err
	}
	reservations := []map[string]any{}
	for _, e := range entries {
		var inst ec2Instance
		json.Unmarshal(e.Data, &inst)
		if !hasStateFilter && (inst.State == "terminated" || inst.State == "shutting-down") {
			continue
		}
		if len(filterIds) > 0 && !containsStr(filterIds, inst.InstanceId) {
			continue
		}
		if !matchInstance(inst, filters) {
			continue
		}
		reservations = append(reservations, map[string]any{
			"ReservationId": newID("r"),
			"OwnerId":       nr.AccountID,
			"Instances":     []map[string]any{instanceToWire(inst)},
		})
	}
	maxResults := 100
	if v, ok := nr.Params["MaxResults"].(float64); ok && v > 0 {
		maxResults = int(v)
	}
	token, _ := nr.Params["NextToken"].(string)
	page, next, pgErr := pagination.Paginate(reservations, maxResults, token, "DescribeInstances")
	if pgErr != nil {
		return nil, model.NewProviderError("InvalidParameterValue", pgErr.Error(), 400)
	}
	data := map[string]any{"Reservations": page}
	if next != "" {
		data["NextToken"] = next
	}
	return provider.OK(data), nil
}

func (p *ComputeProvider) TerminateInstances(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ids := extractIndexedParam(nr.Params, "InstanceId")
	result := []map[string]any{}
	account, region := nr.AccountID, nr.Region
	for _, id := range ids {
		inst, err := p.loadInstance(ctx, account, region, id)
		if err != nil {
			return nil, err
		}
		prev := inst.State
		inst.State = "shutting-down"
		p.saveInstance(ctx, account, region, inst)
		// Transition shutting-down → terminated after 2s; skip if instance was deleted (reset).
		instCopy := inst
		time.AfterFunc(2*time.Second, func() {
			loaded, err := p.loadInstance(context.Background(), account, region, instCopy.InstanceId)
			if err != nil || loaded.State != "shutting-down" {
				return
			}
			loaded.State = "terminated"
			p.saveInstance(context.Background(), account, region, loaded)
		})
		result = append(result, map[string]any{
			"InstanceId":    id,
			"CurrentState":  map[string]any{"Code": stateCode("shutting-down"), "Name": "shutting-down"},
			"PreviousState": map[string]any{"Code": stateCode(prev), "Name": prev},
		})
	}
	return provider.OK(map[string]any{"TerminatingInstances": result}), nil
}

func (p *ComputeProvider) StartInstances(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ids := extractIndexedParam(nr.Params, "InstanceId")
	result := []map[string]any{}
	for _, id := range ids {
		inst, err := p.loadInstance(ctx, nr.AccountID, nr.Region, id)
		if err != nil {
			return nil, err
		}
		prev := inst.State
		inst.State = "running"
		p.saveInstance(ctx, nr.AccountID, nr.Region, inst)
		result = append(result, map[string]any{
			"InstanceId":    id,
			"CurrentState":  map[string]any{"Code": "16", "Name": "running"},
			"PreviousState": map[string]any{"Code": stateCode(prev), "Name": prev},
		})
	}
	return provider.OK(map[string]any{"TerminatingInstances": result}), nil
}

func (p *ComputeProvider) StopInstances(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ids := extractIndexedParam(nr.Params, "InstanceId")
	result := []map[string]any{}
	account, region := nr.AccountID, nr.Region
	for _, id := range ids {
		inst, err := p.loadInstance(ctx, account, region, id)
		if err != nil {
			return nil, err
		}
		prev := inst.State
		inst.State = "stopping"
		p.saveInstance(ctx, account, region, inst)
		// Transition stopping → stopped after 2s.
		instCopy := inst
		time.AfterFunc(2*time.Second, func() {
			instCopy.State = "stopped"
			p.saveInstance(context.Background(), account, region, instCopy)
		})
		result = append(result, map[string]any{
			"InstanceId":    id,
			"CurrentState":  map[string]any{"Code": stateCode("stopping"), "Name": "stopping"},
			"PreviousState": map[string]any{"Code": stateCode(prev), "Name": prev},
		})
	}
	return provider.OK(map[string]any{"StoppingInstances": result}), nil
}

func (p *ComputeProvider) RebootInstances(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ids := extractIndexedParam(nr.Params, "InstanceId")
	for _, id := range ids {
		inst, err := p.loadInstance(ctx, nr.AccountID, nr.Region, id)
		if err != nil {
			return nil, err
		}
		inst.LastRebootTime = time.Now().UTC().Format(time.RFC3339)
		p.saveInstance(ctx, nr.AccountID, nr.Region, inst)
	}
	return provider.OK(nil), nil
}

func (p *ComputeProvider) ModifyInstanceAttribute(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "InstanceId")
	inst, err := p.loadInstance(ctx, nr.AccountID, nr.Region, id)
	if err != nil {
		return nil, err
	}
	if v := strParam(nr.Params, "InstanceType.Value"); v != "" {
		inst.InstanceType = v
	}
	if v := strParam(nr.Params, "UserData.Value"); v != "" {
		inst.UserData = v
	}
	if v, ok := nr.Params["DisableApiTermination.Value"]; ok {
		switch val := v.(type) {
		case bool:
			inst.DisableApiTermination = val
		case string:
			inst.DisableApiTermination = val == "true"
		}
	}
	p.saveInstance(ctx, nr.AccountID, nr.Region, inst)
	return provider.OK(nil), nil
}

func (p *ComputeProvider) DescribeInstanceAttribute(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "InstanceId")
	attr := strParam(nr.Params, "Attribute")
	inst, err := p.loadInstance(ctx, nr.AccountID, nr.Region, id)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"InstanceId": id}
	switch attr {
	case "instanceType":
		resp["InstanceType"] = map[string]any{"Value": inst.InstanceType}
	case "userData":
		resp["UserData"] = map[string]any{"Value": inst.UserData}
	case "disableApiTermination":
		resp["DisableApiTermination"] = map[string]any{"Value": inst.DisableApiTermination}
	case "sriovNetSupport":
		resp["SriovNetSupport"] = map[string]any{"Value": "simple"}
	default:
		resp[attr] = nil
	}
	return provider.OK(resp), nil
}

func (p *ComputeProvider) DescribeInstanceStatus(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	filterIds := extractIndexedParam(nr.Params, "InstanceId")
	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, rtInstance, "")
	if err != nil {
		return nil, err
	}
	statuses := []map[string]any{}
	for _, e := range entries {
		var inst ec2Instance
		json.Unmarshal(e.Data, &inst)
		if inst.State == "terminated" || inst.State == "shutting-down" {
			continue
		}
		if len(filterIds) > 0 && !containsStr(filterIds, inst.InstanceId) {
			continue
		}
		statuses = append(statuses, map[string]any{
			"InstanceId":       inst.InstanceId,
			"AvailabilityZone": "us-east-1a",
			"InstanceState":    map[string]any{"Code": stateCode(inst.State), "Name": inst.State},
			"InstanceStatus":   map[string]any{"Status": "ok", "Details": []any{}},
			"SystemStatus":     map[string]any{"Status": "ok", "Details": []any{}},
			"Events":           []any{},
		})
	}
	return provider.OK(map[string]any{"InstanceStatuses": statuses}), nil
}

func (p *ComputeProvider) AssociateIamInstanceProfile(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	instanceId := strParam(nr.Params, "InstanceId")
	inst, err := p.loadInstance(ctx, nr.AccountID, nr.Region, instanceId)
	if err != nil {
		return nil, err
	}
	// IamInstanceProfile.Arn and IamInstanceProfile.Name from params.
	iamArn := strParam(nr.Params, "IamInstanceProfile.Arn")
	iamName := strParam(nr.Params, "IamInstanceProfile.Name")
	inst.IamInstanceProfileArn = iamArn
	inst.IamInstanceProfileName = iamName
	if err := p.saveInstance(ctx, nr.AccountID, nr.Region, inst); err != nil {
		return nil, err
	}
	assocId := newID("iip-assoc")
	return provider.OK(map[string]any{
		"IamInstanceProfileAssociation": map[string]any{
			"AssociationId": assocId,
			"InstanceId":    instanceId,
			"IamInstanceProfile": map[string]any{
				"Arn": iamArn,
				"Id":  newID("aipa"),
			},
			"State": "associated",
		},
	}), nil
}

func (p *ComputeProvider) DisassociateIamInstanceProfile(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// AssociationId is the primary key in real AWS, but we also accept InstanceId for simplicity.
	instanceId := strParam(nr.Params, "InstanceId")
	if instanceId == "" {
		// Try to look up by AssociationId — for now map to InstanceId param (best-effort).
		instanceId = strParam(nr.Params, "AssociationId")
	}
	inst, err := p.loadInstance(ctx, nr.AccountID, nr.Region, instanceId)
	if err != nil {
		// If not found by AssociationId as InstanceId, return a placeholder response.
		assocId := strParam(nr.Params, "AssociationId")
		return provider.OK(map[string]any{
			"IamInstanceProfileAssociation": map[string]any{
				"AssociationId": assocId,
				"State":         "disassociated",
			},
		}), nil
	}
	prevArn := inst.IamInstanceProfileArn
	inst.IamInstanceProfileArn = ""
	inst.IamInstanceProfileName = ""
	if err := p.saveInstance(ctx, nr.AccountID, nr.Region, inst); err != nil {
		return nil, err
	}
	assocId := strParam(nr.Params, "AssociationId")
	if assocId == "" {
		assocId = newID("iip-assoc")
	}
	return provider.OK(map[string]any{
		"IamInstanceProfileAssociation": map[string]any{
			"AssociationId": assocId,
			"InstanceId":    inst.InstanceId,
			"IamInstanceProfile": map[string]any{
				"Arn": prevArn,
			},
			"State": "disassociated",
		},
	}), nil
}

func (p *ComputeProvider) DescribeImages(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// Return a set of canned AMIs useful for testing
	images := []map[string]any{
		{"ImageId": "ami-0abcdef1234567890", "Name": "amzn2-ami-hvm-2.0", "State": "available", "Architecture": "x86_64", "ImageType": "machine", "OwnerId": "137112412989"},
		{"ImageId": "ami-0123456789abcdef0", "Name": "ubuntu-22.04-lts", "State": "available", "Architecture": "x86_64", "ImageType": "machine", "OwnerId": "099720109477"},
	}
	filterIds := extractIndexedParam(nr.Params, "ImageId")
	if len(filterIds) > 0 {
		filtered := []map[string]any{}
		for _, img := range images {
			if containsStr(filterIds, str(img["ImageId"])) {
				filtered = append(filtered, img)
			}
		}
		images = filtered
	}
	return provider.OK(map[string]any{"Images": images}), nil
}

// ─── Security Group operations ────────────────────────────────────────────────

type securityGroup struct {
	GroupId      string            `json:"GroupId"`
	GroupName    string            `json:"GroupName"`
	Description  string            `json:"Description"`
	VpcId        string            `json:"VpcId"`
	OwnerId      string            `json:"OwnerId"`
	IngressRules []map[string]any  `json:"IngressRules"`
	EgressRules  []map[string]any  `json:"EgressRules"`
	Tags         map[string]string `json:"Tags"`
}

func (p *ComputeProvider) CreateSecurityGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "GroupName")
	desc := strParam(nr.Params, "GroupDescription")
	vpcId := strParam(nr.Params, "VpcId")
	if name == "" {
		return nil, &model.ProviderError{Code: "MissingParameter", Message: "GroupName is required", HTTPStatus: http.StatusBadRequest}
	}
	sgId := newID("sg")
	if customId := extractCustomID(nr.Params); customId != "" {
		sgId = customId
	}

	sg := securityGroup{
		GroupId:     sgId,
		GroupName:   name,
		Description: desc,
		VpcId:       vpcId,
		OwnerId:     nr.AccountID,
	}
	data, _ := json.Marshal(sg)
	if err := p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtSecurityGroup, ID: sg.GroupId, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			// Idempotent: return the existing subnet if same ID already exists.
			if entry, getErr := p.resources.Get(ctx, nr.AccountID, nr.Region, rtSecurityGroup, sgId); getErr == nil {
				var existing securityGroup
				json.Unmarshal(entry.Data, &existing)
				return provider.OK(map[string]any{"GroupId": existing.GroupId}), nil
			}
		}
		return nil, err
	}
	return provider.OK(map[string]any{"GroupId": sg.GroupId}), nil
}

func (p *ComputeProvider) DescribeSecurityGroups(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	filterIds := extractIndexedParam(nr.Params, "GroupId")
	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, rtSecurityGroup, "")
	if err != nil {
		return nil, err
	}
	groups := []map[string]any{}
	for _, e := range entries {
		var sg securityGroup
		json.Unmarshal(e.Data, &sg)
		if len(filterIds) > 0 && !containsStr(filterIds, sg.GroupId) {
			continue
		}
		ingress := sg.IngressRules
		if ingress == nil {
			ingress = []map[string]any{}
		}
		egress := sg.EgressRules
		if egress == nil {
			egress = []map[string]any{}
		}
		groups = append(groups, map[string]any{
			"GroupId":      sg.GroupId,
			"GroupName":    sg.GroupName,
			"Description":  sg.Description,
			"VpcId":        sg.VpcId,
			"OwnerId":      sg.OwnerId,
			"IngressRules": ingress,
			"EgressRules":  egress,
		})
	}
	maxResults := 100
	if v, ok := nr.Params["MaxResults"].(float64); ok && v > 0 {
		maxResults = int(v)
	}
	token, _ := nr.Params["NextToken"].(string)
	page, next, pgErr := pagination.Paginate(groups, maxResults, token, "DescribeSecurityGroups")
	if pgErr != nil {
		return nil, model.NewProviderError("InvalidParameterValue", pgErr.Error(), 400)
	}
	data := map[string]any{"SecurityGroups": page}
	if next != "" {
		data["NextToken"] = next
	}
	return provider.OK(data), nil
}

func (p *ComputeProvider) DeleteSecurityGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "GroupId")
	if err := p.resources.Delete(ctx, nr.AccountID, nr.Region, rtSecurityGroup, id); err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "InvalidGroup.NotFound", Message: fmt.Sprintf("The security group '%s' does not exist", id), HTTPStatus: http.StatusBadRequest}
	}
	return provider.OK(nil), nil
}

func (p *ComputeProvider) AuthorizeSecurityGroupIngress(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.addSGRule(ctx, nr, true)
}

func (p *ComputeProvider) AuthorizeSecurityGroupEgress(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.addSGRule(ctx, nr, false)
}

func (p *ComputeProvider) RevokeSecurityGroupIngress(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "GroupId")
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtSecurityGroup, id)
	if err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "InvalidGroup.NotFound", Message: "security group not found", HTTPStatus: http.StatusBadRequest}
	}
	var sg securityGroup
	json.Unmarshal(e.Data, &sg)
	toRevoke := parseSGRules(nr.Params)
	sg.IngressRules = removeSGRules(sg.IngressRules, toRevoke)
	data, _ := json.Marshal(sg)
	p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtSecurityGroup, ID: id, Data: data})
	return provider.OK(nil), nil
}

func (p *ComputeProvider) addSGRule(ctx context.Context, nr *model.NormalizedRequest, ingress bool) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "GroupId")
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtSecurityGroup, id)
	if err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "InvalidGroup.NotFound", Message: "security group not found", HTTPStatus: http.StatusBadRequest}
	}
	var sg securityGroup
	json.Unmarshal(e.Data, &sg)
	rules := parseSGRules(nr.Params)
	if ingress {
		sg.IngressRules = append(sg.IngressRules, rules...)
	} else {
		sg.EgressRules = append(sg.EgressRules, rules...)
	}
	data, _ := json.Marshal(sg)
	p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtSecurityGroup, ID: id, Data: data})
	return provider.OK(nil), nil
}

// parseSGRules reads IpPermissions.N.{IpProtocol,FromPort,ToPort} and nested
// IpPermissions.N.IpRanges.M.CidrIp from params into a slice of rule maps.
func parseSGRules(params map[string]any) []map[string]any {
	var rules []map[string]any
	for i := 1; ; i++ {
		proto, ok := params[fmt.Sprintf("IpPermissions.%d.IpProtocol", i)].(string)
		if !ok || proto == "" {
			break
		}
		rule := map[string]any{
			"IpProtocol": proto,
			"FromPort":   strParam(params, fmt.Sprintf("IpPermissions.%d.FromPort", i)),
			"ToPort":     strParam(params, fmt.Sprintf("IpPermissions.%d.ToPort", i)),
		}
		var ipRanges []map[string]any
		for j := 1; ; j++ {
			cidr, ok := params[fmt.Sprintf("IpPermissions.%d.IpRanges.%d.CidrIp", i, j)].(string)
			if !ok || cidr == "" {
				break
			}
			ipRanges = append(ipRanges, map[string]any{"CidrIp": cidr})
		}
		if len(ipRanges) > 0 {
			rule["IpRanges"] = ipRanges
		}
		rules = append(rules, rule)
	}
	// Fallback: legacy single-rule without index (IpPermissions.1.*).
	if len(rules) == 0 {
		proto := strParam(params, "IpPermissions.1.IpProtocol")
		if proto != "" {
			rules = append(rules, map[string]any{
				"IpProtocol": proto,
				"FromPort":   strParam(params, "IpPermissions.1.FromPort"),
				"ToPort":     strParam(params, "IpPermissions.1.ToPort"),
			})
		}
	}
	return rules
}

// removeSGRules removes from existing any rule whose (IpProtocol,FromPort,ToPort) matches a revoke entry.
func removeSGRules(existing, toRevoke []map[string]any) []map[string]any {
	result := existing[:0:0]
	for _, e := range existing {
		matched := false
		for _, r := range toRevoke {
			if strParam(e, "IpProtocol") == strParam(r, "IpProtocol") &&
				strParam(e, "FromPort") == strParam(r, "FromPort") &&
				strParam(e, "ToPort") == strParam(r, "ToPort") {
				matched = true
				break
			}
		}
		if !matched {
			result = append(result, e)
		}
	}
	return result
}

// ─── Key Pair operations ──────────────────────────────────────────────────────

type keyPair struct {
	KeyPairId      string `json:"KeyPairId"`
	KeyName        string `json:"KeyName"`
	KeyFingerprint string `json:"KeyFingerprint"`
	KeyMaterial    string `json:"KeyMaterial,omitempty"`
}

func (p *ComputeProvider) CreateKeyPair(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "KeyName")
	if name == "" {
		return nil, &model.ProviderError{Code: "MissingParameter", Message: "KeyName is required", HTTPStatus: http.StatusBadRequest}
	}
	kp := keyPair{
		KeyPairId:      newID("key"),
		KeyName:        name,
		KeyFingerprint: "aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99",
		KeyMaterial:    "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEA...(mock)...\n-----END RSA PRIVATE KEY-----",
	}
	data, _ := json.Marshal(kp)
	if err := p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtKeyPair, ID: kp.KeyPairId, Data: data}); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"KeyPairId":      kp.KeyPairId,
		"KeyName":        kp.KeyName,
		"KeyFingerprint": kp.KeyFingerprint,
		"KeyMaterial":    kp.KeyMaterial,
	}), nil
}

func (p *ComputeProvider) DescribeKeyPairs(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	filterNames := extractIndexedParam(nr.Params, "KeyName")
	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, rtKeyPair, "")
	if err != nil {
		return nil, err
	}
	pairs := []map[string]any{}
	for _, e := range entries {
		var kp keyPair
		json.Unmarshal(e.Data, &kp)
		if len(filterNames) > 0 && !containsStr(filterNames, kp.KeyName) {
			continue
		}
		pairs = append(pairs, map[string]any{
			"KeyPairId":      kp.KeyPairId,
			"KeyName":        kp.KeyName,
			"KeyFingerprint": kp.KeyFingerprint,
		})
	}
	return provider.OK(map[string]any{"KeyPairs": pairs}), nil
}

func (p *ComputeProvider) DeleteKeyPair(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// Accept either KeyName or KeyPairId
	id := strParam(nr.Params, "KeyPairId")
	if id == "" {
		// Look up by name
		name := strParam(nr.Params, "KeyName")
		entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, rtKeyPair, "")
		for _, e := range entries {
			var kp keyPair
			json.Unmarshal(e.Data, &kp)
			if kp.KeyName == name {
				id = kp.KeyPairId
				break
			}
		}
	}
	p.resources.Delete(ctx, nr.AccountID, nr.Region, rtKeyPair, id)
	return provider.OK(nil), nil
}

func (p *ComputeProvider) ImportKeyPair(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "KeyName")
	kp := keyPair{
		KeyPairId:      newID("key"),
		KeyName:        name,
		KeyFingerprint: "aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99",
	}
	data, _ := json.Marshal(kp)
	p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtKeyPair, ID: kp.KeyPairId, Data: data})
	return provider.OK(map[string]any{
		"KeyPairId":      kp.KeyPairId,
		"KeyName":        kp.KeyName,
		"KeyFingerprint": kp.KeyFingerprint,
	}), nil
}

// ─── VPC operations ───────────────────────────────────────────────────────────

type ec2Vpc struct {
	VpcId              string            `json:"VpcId"`
	State              string            `json:"State"`
	CidrBlock          string            `json:"CidrBlock"`
	IsDefault          bool              `json:"IsDefault"`
	OwnerId            string            `json:"OwnerId"`
	Tags               map[string]string `json:"Tags"`
	EnableDnsSupport   bool              `json:"EnableDnsSupport"`
	EnableDnsHostnames bool              `json:"EnableDnsHostnames"`
}

func (p *ComputeProvider) CreateVpc(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	cidr := strParam(nr.Params, "CidrBlock")
	if cidr == "" {
		return nil, &model.ProviderError{Code: "MissingParameter", Message: "CidrBlock is required", HTTPStatus: http.StatusBadRequest}
	}
	vpcId := newID("vpc")
	if customId := extractCustomID(nr.Params); customId != "" {
		vpcId = customId
	}
	vpc := ec2Vpc{
		VpcId:     vpcId,
		State:     "available",
		CidrBlock: cidr,
		IsDefault: false,
		OwnerId:   nr.AccountID,
	}
	data, _ := json.Marshal(vpc)
	if err := p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtVpc, ID: vpc.VpcId, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			if entry, getErr := p.resources.Get(ctx, nr.AccountID, nr.Region, rtVpc, vpcId); getErr == nil {
				var existingVPC ec2Vpc
				json.Unmarshal(entry.Data, &existingVPC)
				return provider.OK(map[string]any{"Vpc": vpcToWire(existingVPC)}), nil
			}
		}
		return nil, err
	}
	return provider.OK(map[string]any{"Vpc": vpcToWire(vpc)}), nil
}

func (p *ComputeProvider) DescribeVpcs(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	p.seedDefaultVPC(ctx, nr.AccountID, nr.Region)
	filterIds := extractIndexedParam(nr.Params, "VpcId")
	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, rtVpc, "")
	if err != nil {
		return nil, err
	}
	vpcs := []map[string]any{}
	for _, e := range entries {
		var vpc ec2Vpc
		json.Unmarshal(e.Data, &vpc)
		if len(filterIds) > 0 && !containsStr(filterIds, vpc.VpcId) {
			continue
		}
		vpcs = append(vpcs, vpcToWire(vpc))
	}
	maxResults := 100
	if v, ok := nr.Params["MaxResults"].(float64); ok && v > 0 {
		maxResults = int(v)
	}
	token, _ := nr.Params["NextToken"].(string)
	page, next, pgErr := pagination.Paginate(vpcs, maxResults, token, "DescribeVpcs")
	if pgErr != nil {
		return nil, model.NewProviderError("InvalidParameterValue", pgErr.Error(), 400)
	}
	data := map[string]any{"Vpcs": page}
	if next != "" {
		data["NextToken"] = next
	}
	return provider.OK(data), nil
}

func (p *ComputeProvider) DeleteVpc(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "VpcId")
	if err := p.resources.Delete(ctx, nr.AccountID, nr.Region, rtVpc, id); err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "InvalidVpcID.NotFound", Message: fmt.Sprintf("The vpc ID '%s' does not exist", id), HTTPStatus: http.StatusBadRequest}
	}
	return provider.OK(nil), nil
}

func (p *ComputeProvider) ModifyVpcAttribute(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "VpcId")
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtVpc, id)
	if err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "InvalidVpcID.NotFound", Message: fmt.Sprintf("The vpc ID '%s' does not exist", id), HTTPStatus: http.StatusBadRequest}
	}
	var vpc ec2Vpc
	json.Unmarshal(e.Data, &vpc)
	if v, ok := nr.Params["EnableDnsSupport.Value"].(string); ok {
		vpc.EnableDnsSupport = v == "true"
	}
	if v, ok := nr.Params["EnableDnsHostnames.Value"].(string); ok {
		vpc.EnableDnsHostnames = v == "true"
	}
	data, _ := json.Marshal(vpc)
	p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtVpc, ID: id, Data: data})
	return provider.OK(nil), nil
}

func vpcToWire(vpc ec2Vpc) map[string]any {
	isDefault := "false"
	if vpc.IsDefault {
		isDefault = "true"
	}
	return map[string]any{
		"VpcId":     vpc.VpcId,
		"State":     vpc.State,
		"CidrBlock": vpc.CidrBlock,
		"IsDefault": isDefault,
		"OwnerId":   vpc.OwnerId,
	}
}

// ─── Subnet operations ────────────────────────────────────────────────────────

type ec2Subnet struct {
	SubnetId                string            `json:"SubnetId"`
	State                   string            `json:"State"`
	VpcId                   string            `json:"VpcId"`
	CidrBlock               string            `json:"CidrBlock"`
	AvailabilityZone        string            `json:"AvailabilityZone"`
	AvailableIpAddressCount int               `json:"AvailableIpAddressCount"`
	IsDefault               bool              `json:"IsDefault"`
	MapPublicIpOnLaunch     bool              `json:"MapPublicIpOnLaunch"`
	Tags                    map[string]string `json:"Tags"`
}

func (p *ComputeProvider) CreateSubnet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	vpcId := strParam(nr.Params, "VpcId")
	cidr := strParam(nr.Params, "CidrBlock")
	az := strParam(nr.Params, "AvailabilityZone")
	if az == "" {
		az = nr.Region + "a"
	}

	subnetId := newID("subnet")
	if customId := extractCustomID(nr.Params); customId != "" {
		subnetId = customId
	}

	sn := ec2Subnet{
		SubnetId:                subnetId,
		State:                   "available",
		VpcId:                   vpcId,
		CidrBlock:               cidr,
		AvailabilityZone:        az,
		AvailableIpAddressCount: 251,
	}
	data, _ := json.Marshal(sn)
	if err := p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtSubnet, ID: sn.SubnetId, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			// Idempotent: return the existing subnet if same ID already exists.
			if entry, getErr := p.resources.Get(ctx, nr.AccountID, nr.Region, rtSubnet, subnetId); getErr == nil {
				var existing ec2Subnet
				json.Unmarshal(entry.Data, &existing)
				return provider.OK(map[string]any{"Subnet": subnetToWire(existing)}), nil
			}
		}
		return nil, err
	}
	return provider.OK(map[string]any{"Subnet": subnetToWire(sn)}), nil
}

func (p *ComputeProvider) DescribeSubnets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	filterIds := extractIndexedParam(nr.Params, "SubnetId")
	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, rtSubnet, "")
	if err != nil {
		return nil, err
	}
	subnets := []map[string]any{}
	for _, e := range entries {
		var sn ec2Subnet
		json.Unmarshal(e.Data, &sn)
		if len(filterIds) > 0 && !containsStr(filterIds, sn.SubnetId) {
			continue
		}
		subnets = append(subnets, subnetToWire(sn))
	}
	maxResults := 100
	if v, ok := nr.Params["MaxResults"].(float64); ok && v > 0 {
		maxResults = int(v)
	}
	token, _ := nr.Params["NextToken"].(string)
	page, next, pgErr := pagination.Paginate(subnets, maxResults, token, "DescribeSubnets")
	if pgErr != nil {
		return nil, model.NewProviderError("InvalidParameterValue", pgErr.Error(), 400)
	}
	data := map[string]any{"Subnets": page}
	if next != "" {
		data["NextToken"] = next
	}
	return provider.OK(data), nil
}

func (p *ComputeProvider) DeleteSubnet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "SubnetId")
	p.resources.Delete(ctx, nr.AccountID, nr.Region, rtSubnet, id)
	return provider.OK(nil), nil
}

func subnetToWire(sn ec2Subnet) map[string]any {
	return map[string]any{
		"SubnetId":                sn.SubnetId,
		"State":                   sn.State,
		"VpcId":                   sn.VpcId,
		"CidrBlock":               sn.CidrBlock,
		"AvailabilityZone":        sn.AvailabilityZone,
		"AvailableIpAddressCount": strconv.Itoa(sn.AvailableIpAddressCount),
		"DefaultForAz":            sn.IsDefault,
		"MapPublicIpOnLaunch":     sn.MapPublicIpOnLaunch,
	}
}

// ─── Internet Gateway operations ──────────────────────────────────────────────

type ec2IGW struct {
	InternetGatewayId string              `json:"InternetGatewayId"`
	OwnerId           string              `json:"OwnerId"`
	Attachments       []map[string]string `json:"Attachments"`
}

func (p *ComputeProvider) CreateInternetGateway(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	igw := ec2IGW{
		InternetGatewayId: newID("igw"),
		OwnerId:           nr.AccountID,
	}
	data, _ := json.Marshal(igw)
	p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtIGW, ID: igw.InternetGatewayId, Data: data})
	return provider.OK(map[string]any{"InternetGateway": map[string]any{
		"InternetGatewayId": igw.InternetGatewayId,
		"OwnerId":           igw.OwnerId,
	}}), nil
}

func (p *ComputeProvider) DescribeInternetGateways(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	filterIds := extractIndexedParam(nr.Params, "InternetGatewayId")
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, rtIGW, "")
	igws := []map[string]any{}
	for _, e := range entries {
		var igw ec2IGW
		json.Unmarshal(e.Data, &igw)
		if len(filterIds) > 0 && !containsStr(filterIds, igw.InternetGatewayId) {
			continue
		}
		atts := []map[string]any{}
		for _, att := range igw.Attachments {
			atts = append(atts, map[string]any{"VpcId": att["VpcId"], "State": "available"})
		}
		igws = append(igws, map[string]any{
			"InternetGatewayId": igw.InternetGatewayId,
			"OwnerId":           igw.OwnerId,
			"Attachments":       atts,
		})
	}
	return provider.OK(map[string]any{"InternetGateways": igws}), nil
}

func (p *ComputeProvider) DeleteInternetGateway(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "InternetGatewayId")
	p.resources.Delete(ctx, nr.AccountID, nr.Region, rtIGW, id)
	return provider.OK(nil), nil
}

func (p *ComputeProvider) AttachInternetGateway(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	igwId := strParam(nr.Params, "InternetGatewayId")
	vpcId := strParam(nr.Params, "VpcId")
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtIGW, igwId)
	if err != nil {
		return nil, &model.ProviderError{Code: "InvalidInternetGatewayID.NotFound", Message: "igw not found", HTTPStatus: http.StatusBadRequest}
	}
	var igw ec2IGW
	json.Unmarshal(e.Data, &igw)
	igw.Attachments = append(igw.Attachments, map[string]string{"VpcId": vpcId, "State": "available"})
	data, _ := json.Marshal(igw)
	p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtIGW, ID: igwId, Data: data})
	return provider.OK(nil), nil
}

func (p *ComputeProvider) DetachInternetGateway(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	igwId := strParam(nr.Params, "InternetGatewayId")
	vpcId := strParam(nr.Params, "VpcId")
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtIGW, igwId)
	if err != nil {
		return nil, &model.ProviderError{Code: "InvalidInternetGatewayID.NotFound", Message: "igw not found", HTTPStatus: http.StatusBadRequest}
	}
	var igw ec2IGW
	json.Unmarshal(e.Data, &igw)
	filtered := []map[string]string{}
	for _, att := range igw.Attachments {
		if att["VpcId"] != vpcId {
			filtered = append(filtered, att)
		}
	}
	igw.Attachments = filtered
	data, _ := json.Marshal(igw)
	p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtIGW, ID: igwId, Data: data})
	return provider.OK(nil), nil
}

// ─── Route Table operations ───────────────────────────────────────────────────

type ec2RouteTable struct {
	RouteTableId string            `json:"RouteTableId"`
	VpcId        string            `json:"VpcId"`
	OwnerId      string            `json:"OwnerId"`
	Routes       []map[string]any  `json:"Routes"`
	Tags         map[string]string `json:"Tags"`
}

func (p *ComputeProvider) CreateRouteTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	vpcId := strParam(nr.Params, "VpcId")
	rt := ec2RouteTable{
		RouteTableId: newID("rtb"),
		VpcId:        vpcId,
		OwnerId:      nr.AccountID,
		Routes: []map[string]any{
			{"DestinationCidrBlock": "local", "GatewayId": "local", "State": "active"},
		},
	}
	data, _ := json.Marshal(rt)
	p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtRouteTable, ID: rt.RouteTableId, Data: data})
	return provider.OK(map[string]any{"RouteTable": rtToWire(rt)}), nil
}

func (p *ComputeProvider) DescribeRouteTables(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	filterIds := extractIndexedParam(nr.Params, "RouteTableId")
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, rtRouteTable, "")
	rts := []map[string]any{}
	for _, e := range entries {
		var rt ec2RouteTable
		json.Unmarshal(e.Data, &rt)
		if len(filterIds) > 0 && !containsStr(filterIds, rt.RouteTableId) {
			continue
		}
		rts = append(rts, rtToWire(rt))
	}
	return provider.OK(map[string]any{"RouteTables": rts}), nil
}

func (p *ComputeProvider) DeleteRouteTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "RouteTableId")
	p.resources.Delete(ctx, nr.AccountID, nr.Region, rtRouteTable, id)
	return provider.OK(nil), nil
}

func (p *ComputeProvider) CreateRoute(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	rtId := strParam(nr.Params, "RouteTableId")
	dest := strParam(nr.Params, "DestinationCidrBlock")
	gwId := strParam(nr.Params, "GatewayId")
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtRouteTable, rtId)
	if err != nil {
		return nil, &model.ProviderError{Code: "InvalidRouteTableID.NotFound", Message: "route table not found", HTTPStatus: http.StatusBadRequest}
	}
	var rt ec2RouteTable
	json.Unmarshal(e.Data, &rt)
	rt.Routes = append(rt.Routes, map[string]any{
		"DestinationCidrBlock": dest,
		"GatewayId":            gwId,
		"State":                "active",
	})
	data, _ := json.Marshal(rt)
	p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtRouteTable, ID: rtId, Data: data})
	return provider.OK(nil), nil
}

func (p *ComputeProvider) DeleteRoute(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	rtId := strParam(nr.Params, "RouteTableId")
	dest := strParam(nr.Params, "DestinationCidrBlock")
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtRouteTable, rtId)
	if err != nil {
		return nil, &model.ProviderError{Code: "InvalidRouteTableID.NotFound", Message: "route table not found", HTTPStatus: http.StatusBadRequest}
	}
	var rt ec2RouteTable
	json.Unmarshal(e.Data, &rt)
	filtered := []map[string]any{}
	for _, r := range rt.Routes {
		if str(r["DestinationCidrBlock"]) != dest {
			filtered = append(filtered, r)
		}
	}
	rt.Routes = filtered
	data, _ := json.Marshal(rt)
	p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtRouteTable, ID: rtId, Data: data})
	return provider.OK(nil), nil
}

func (p *ComputeProvider) AssociateRouteTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{"AssociationId": newID("rtbassoc")}), nil
}

func rtToWire(rt ec2RouteTable) map[string]any {
	return map[string]any{
		"RouteTableId": rt.RouteTableId,
		"VpcId":        rt.VpcId,
		"OwnerId":      rt.OwnerId,
		"Routes":       rt.Routes,
	}
}

// ─── NAT Gateway operations ───────────────────────────────────────────────────

type ec2NatGateway struct {
	NatGatewayId string    `json:"NatGatewayId"`
	VpcId        string    `json:"VpcId"`
	SubnetId     string    `json:"SubnetId"`
	State        string    `json:"State"`
	CreateTime   time.Time `json:"CreateTime"`
}

func (p *ComputeProvider) CreateNatGateway(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	subnetId := strParam(nr.Params, "SubnetId")
	ngw := ec2NatGateway{
		NatGatewayId: newID("nat"),
		SubnetId:     subnetId,
		State:        "available",
		CreateTime:   time.Now(),
	}
	// Look up VPC from subnet
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, rtSubnet, "")
	for _, e := range entries {
		var sn ec2Subnet
		json.Unmarshal(e.Data, &sn)
		if sn.SubnetId == subnetId {
			ngw.VpcId = sn.VpcId
			break
		}
	}
	data, _ := json.Marshal(ngw)
	p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtNatGateway, ID: ngw.NatGatewayId, Data: data})
	return provider.OK(map[string]any{"NatGateway": ngwToWire(ngw)}), nil
}

func (p *ComputeProvider) DescribeNatGateways(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	filterIds := extractIndexedParam(nr.Params, "NatGatewayId")
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, rtNatGateway, "")
	ngws := []map[string]any{}
	for _, e := range entries {
		var ngw ec2NatGateway
		json.Unmarshal(e.Data, &ngw)
		if ngw.State == "deleted" {
			continue
		}
		if len(filterIds) > 0 && !containsStr(filterIds, ngw.NatGatewayId) {
			continue
		}
		ngws = append(ngws, ngwToWire(ngw))
	}
	return provider.OK(map[string]any{"NatGateways": ngws}), nil
}

func (p *ComputeProvider) DeleteNatGateway(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "NatGatewayId")
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtNatGateway, id)
	if err != nil {
		return nil, &model.ProviderError{Code: "NatGatewayNotFound", Message: "nat gateway not found", HTTPStatus: http.StatusBadRequest}
	}
	var ngw ec2NatGateway
	json.Unmarshal(e.Data, &ngw)
	ngw.State = "deleted"
	data, _ := json.Marshal(ngw)
	p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtNatGateway, ID: id, Data: data})
	return provider.OK(map[string]any{"NatGatewayId": id}), nil
}

func ngwToWire(ngw ec2NatGateway) map[string]any {
	return map[string]any{
		"NatGatewayId": ngw.NatGatewayId,
		"VpcId":        ngw.VpcId,
		"SubnetId":     ngw.SubnetId,
		"State":        ngw.State,
		"CreateTime":   ngw.CreateTime.UTC().Format(time.RFC3339),
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

func intParam(params map[string]any, key string, def int) int {
	if v, ok := params[key]; ok {
		switch n := v.(type) {
		case string:
			i, _ := strconv.Atoi(n)
			return i
		case float64:
			return int(n)
		}
	}
	return def
}

func str(v any) string {
	if v == nil {
		return ""
	}
	switch s := v.(type) {
	case string:
		return s
	default:
		return fmt.Sprintf("%v", v)
	}
}

// extractIndexedParam collects EC2-style indexed params: "InstanceId.1", "InstanceId.2", ...
func extractIndexedParam(params map[string]any, prefix string) []string {
	result := []string{}
	for i := 1; ; i++ {
		key := fmt.Sprintf("%s.%d", prefix, i)
		v, ok := params[key]
		if !ok {
			break
		}
		if s, ok := v.(string); ok {
			result = append(result, s)
		}
	}
	return result
}

func extractCustomID(params map[string]any) string {
	for ts := 1; ; ts++ {
		tagKeyPrefix := fmt.Sprintf("TagSpecification.%d.Tag.", ts)
		found := false
		for t := 1; ; t++ {
			key := strParam(params, fmt.Sprintf("%s%d.Key", tagKeyPrefix, t))
			if key == "" {
				break
			}
			found = true
			if key == tagKeyCustomID {
				return strParam(params, fmt.Sprintf("%s%d.Value", tagKeyPrefix, t))
			}
		}
		if !found {
			break
		}
	}
	return ""
}

func containsStr(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

// Suppress unused import warning for strings package
var _ = strings.TrimSpace
