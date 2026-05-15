package stack

import "fmt"

// isSAMTemplate returns true if the template has a SAM Transform declaration.
func isSAMTemplate(template map[string]any) bool {
	switch t := template["Transform"].(type) {
	case string:
		return t == "AWS::Serverless-2016-10-31"
	case []any:
		for _, v := range t {
			if s, ok := v.(string); ok && s == "AWS::Serverless-2016-10-31" {
				return true
			}
		}
	}
	return false
}

// TransformSAM expands SAM resource types into standard CFN resources.
// Called at CreateStack/UpdateStack time when isSAMTemplate returns true.
func TransformSAM(template map[string]any) (map[string]any, error) {
	resources, _ := template["Resources"].(map[string]any)
	if resources == nil {
		return template, nil
	}
	extra := map[string]any{}
	toDelete := []string{}

	for logicalID, raw := range resources {
		res, _ := raw.(map[string]any)
		resType, _ := res["Type"].(string)
		props, _ := res["Properties"].(map[string]any)
		if props == nil {
			props = map[string]any{}
		}

		switch resType {
		case "AWS::Serverless::Function":
			toDelete = append(toDelete, logicalID)
			// IAM execution role
			roleID := logicalID + "Role"
			extra[roleID] = samExecutionRole(logicalID)
			// Lambda function
			extra[logicalID] = samLambdaFunction(props, roleID)
			// Events → permissions
			if events, ok := props["Events"].(map[string]any); ok {
				for evtName, evtRaw := range events {
					evt, _ := evtRaw.(map[string]any)
					if evt["Type"] == "Api" {
						extra[logicalID+"Perm"+evtName] = samLambdaPermission(logicalID)
					}
				}
			}

		case "AWS::Serverless::SimpleTable":
			toDelete = append(toDelete, logicalID)
			extra[logicalID] = samDynamoTable(props)

		case "AWS::Serverless::Api":
			toDelete = append(toDelete, logicalID)
			extra[logicalID] = samRestAPI(props)
		}
	}

	for _, id := range toDelete {
		delete(resources, id)
	}
	for k, v := range extra {
		resources[k] = v
	}
	template["Resources"] = resources
	return template, nil
}

func samExecutionRole(funcLogicalID string) map[string]any {
	_ = funcLogicalID // used as part of the naming context by the caller
	return map[string]any{
		"Type": "AWS::IAM::Role",
		"Properties": map[string]any{
			"AssumeRolePolicyDocument": map[string]any{
				"Version": "2012-10-17",
				"Statement": []any{map[string]any{
					"Effect":    "Allow",
					"Principal": map[string]any{"Service": "lambda.amazonaws.com"},
					"Action":    "sts:AssumeRole",
				}},
			},
			"ManagedPolicyArns": []any{"arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"},
		},
	}
}

func samLambdaFunction(props map[string]any, roleLogicalID string) map[string]any {
	fnProps := map[string]any{
		"FunctionName": props["FunctionName"],
		"Runtime":      props["Runtime"],
		"Handler":      props["Handler"],
		"Role":         map[string]any{"Fn::GetAtt": []any{roleLogicalID, "Arn"}},
		"Code": map[string]any{
			"ZipFile":  props["InlineCode"],
			"S3Bucket": props["CodeUri"], // simplified; SAM CodeUri maps to S3
		},
		"Environment": props["Environment"],
		"Timeout":     props["Timeout"],
		"MemorySize":  props["MemorySize"],
	}
	// Remove nil values
	for k, v := range fnProps {
		if v == nil {
			delete(fnProps, k)
		}
	}
	return map[string]any{"Type": "AWS::Lambda::Function", "Properties": fnProps}
}

func samLambdaPermission(funcLogicalID string) map[string]any {
	return map[string]any{
		"Type": "AWS::Lambda::Permission",
		"Properties": map[string]any{
			"Action":       "lambda:InvokeFunction",
			"FunctionName": map[string]any{"Ref": funcLogicalID},
			"Principal":    "apigateway.amazonaws.com",
		},
	}
}

func samDynamoTable(props map[string]any) map[string]any {
	tblProps := map[string]any{
		"AttributeDefinitions": []any{map[string]any{
			"AttributeName": "id", "AttributeType": "S",
		}},
		"KeySchema": []any{map[string]any{
			"AttributeName": "id", "KeyType": "HASH",
		}},
		"BillingMode": "PAY_PER_REQUEST",
	}
	if tn, ok := props["TableName"]; ok {
		tblProps["TableName"] = tn
	}
	return map[string]any{"Type": "AWS::DynamoDB::Table", "Properties": tblProps}
}

func samRestAPI(props map[string]any) map[string]any {
	apiProps := map[string]any{"Name": props["Name"]}
	if apiProps["Name"] == nil {
		apiProps["Name"] = "ServerlessAPI"
	}
	_ = fmt.Sprintf // ensure fmt is used
	return map[string]any{"Type": "AWS::ApiGateway::RestApi", "Properties": apiProps}
}
