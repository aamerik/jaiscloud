package stack

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func baseCtx() *resolveCtx {
	rc := newResolveCtx("us-east-1", "123456789012", 4566)
	rc.pseudoParams["AWS::StackName"] = "my-stack"
	rc.pseudoParams["AWS::StackId"] = "arn:aws:cloudformation:us-east-1:123456789012:stack/my-stack/abc"
	return rc
}

func TestResolveRef_PseudoParam(t *testing.T) {
	rc := baseCtx()
	got := rc.Resolve(map[string]any{"Ref": "AWS::Region"})
	assert.Equal(t, "us-east-1", got)
}

func TestResolveRef_Param(t *testing.T) {
	rc := baseCtx()
	rc.params["Env"] = "prod"
	got := rc.Resolve(map[string]any{"Ref": "Env"})
	assert.Equal(t, "prod", got)
}

func TestResolveRef_Resource(t *testing.T) {
	rc := baseCtx()
	rc.resources["MyQueue"] = &cfResource{PhysicalResourceId: "https://sqs.us-east-1.amazonaws.com/123/my-queue"}
	got := rc.Resolve(map[string]any{"Ref": "MyQueue"})
	assert.Equal(t, "https://sqs.us-east-1.amazonaws.com/123/my-queue", got)
}

func TestResolveRef_Unknown(t *testing.T) {
	rc := baseCtx()
	got := rc.Resolve(map[string]any{"Ref": "DoesNotExist"})
	assert.Equal(t, "${DoesNotExist}", got)
}

func TestResolveGetAtt(t *testing.T) {
	rc := baseCtx()
	rc.resources["MyTopic"] = &cfResource{
		PhysicalResourceId: "arn:aws:sns:us-east-1:123:my-topic",
		Attributes:         map[string]any{"TopicArn": "arn:aws:sns:us-east-1:123:my-topic"},
	}
	got := rc.Resolve(map[string]any{"Fn::GetAtt": []any{"MyTopic", "TopicArn"}})
	assert.Equal(t, "arn:aws:sns:us-east-1:123:my-topic", got)
}

func TestResolveGetAtt_ArnFallback(t *testing.T) {
	rc := baseCtx()
	rc.resources["MyBucket"] = &cfResource{
		PhysicalResourceId: "arn:aws:s3:::my-bucket",
		Attributes:         map[string]any{},
	}
	got := rc.Resolve(map[string]any{"Fn::GetAtt": []any{"MyBucket", "Arn"}})
	assert.Equal(t, "arn:aws:s3:::my-bucket", got)
}

func TestResolveSub_Simple(t *testing.T) {
	rc := baseCtx()
	rc.params["BucketName"] = "my-bucket"
	got := rc.Resolve(map[string]any{"Fn::Sub": "arn:aws:s3:::${BucketName}"})
	assert.Equal(t, "arn:aws:s3:::my-bucket", got)
}

func TestResolveSub_WithLocalVars(t *testing.T) {
	rc := baseCtx()
	got := rc.Resolve(map[string]any{
		"Fn::Sub": []any{
			"Hello ${Name}, welcome to ${Place}",
			map[string]any{"Name": "World", "Place": "JaisCloud"},
		},
	})
	assert.Equal(t, "Hello World, welcome to JaisCloud", got)
}

func TestResolveSub_PseudoParam(t *testing.T) {
	rc := baseCtx()
	got := rc.Resolve(map[string]any{"Fn::Sub": "Region is ${AWS::Region}"})
	assert.Equal(t, "Region is us-east-1", got)
}

func TestResolveJoin(t *testing.T) {
	rc := baseCtx()
	got := rc.Resolve(map[string]any{
		"Fn::Join": []any{"-", []any{"a", "b", "c"}},
	})
	assert.Equal(t, "a-b-c", got)
}

func TestResolveJoin_Empty(t *testing.T) {
	rc := baseCtx()
	got := rc.Resolve(map[string]any{
		"Fn::Join": []any{"", []any{"foo", "bar"}},
	})
	assert.Equal(t, "foobar", got)
}

func TestResolveIf_True(t *testing.T) {
	rc := baseCtx()
	rc.conditions["IsProd"] = true
	got := rc.Resolve(map[string]any{
		"Fn::If": []any{"IsProd", "production", "staging"},
	})
	assert.Equal(t, "production", got)
}

func TestResolveIf_False(t *testing.T) {
	rc := baseCtx()
	rc.conditions["IsProd"] = false
	got := rc.Resolve(map[string]any{
		"Fn::If": []any{"IsProd", "production", "staging"},
	})
	assert.Equal(t, "staging", got)
}

func TestResolveSelect(t *testing.T) {
	rc := baseCtx()
	got := rc.Resolve(map[string]any{
		"Fn::Select": []any{1, []any{"zero", "one", "two"}},
	})
	assert.Equal(t, "one", got)
}

func TestResolveSelect_OutOfBounds(t *testing.T) {
	rc := baseCtx()
	got := rc.Resolve(map[string]any{
		"Fn::Select": []any{5, []any{"zero", "one"}},
	})
	assert.Nil(t, got)
}

func TestResolveSplit(t *testing.T) {
	rc := baseCtx()
	got := rc.Resolve(map[string]any{
		"Fn::Split": []any{",", "a,b,c"},
	})
	assert.Equal(t, []any{"a", "b", "c"}, got)
}

func TestResolveFindInMap(t *testing.T) {
	rc := baseCtx()
	rc.mappings["RegionMap"] = map[string]any{
		"us-east-1": map[string]any{"AMI": "ami-12345"},
	}
	got := rc.Resolve(map[string]any{
		"Fn::FindInMap": []any{"RegionMap", "us-east-1", "AMI"},
	})
	assert.Equal(t, "ami-12345", got)
}

func TestResolveFindInMap_DynamicKey(t *testing.T) {
	rc := baseCtx()
	rc.mappings["RegionMap"] = map[string]any{
		"us-east-1": map[string]any{"AMI": "ami-east"},
	}
	// Key derived from pseudo-param at resolve time.
	got := rc.Resolve(map[string]any{
		"Fn::FindInMap": []any{"RegionMap", map[string]any{"Ref": "AWS::Region"}, "AMI"},
	})
	assert.Equal(t, "ami-east", got)
}

func TestResolveBase64(t *testing.T) {
	rc := baseCtx()
	got := rc.Resolve(map[string]any{"Fn::Base64": "hello"})
	assert.Equal(t, "aGVsbG8=", got)
}

func TestResolveNot(t *testing.T) {
	rc := baseCtx()
	rc.conditions["Flag"] = true
	got := rc.Resolve(map[string]any{
		"Fn::Not": []any{map[string]any{"Condition": "Flag"}},
	})
	assert.Equal(t, false, got)
}

func TestResolveAnd(t *testing.T) {
	rc := baseCtx()
	rc.conditions["A"] = true
	rc.conditions["B"] = false
	got := rc.Resolve(map[string]any{
		"Fn::And": []any{
			map[string]any{"Condition": "A"},
			map[string]any{"Condition": "B"},
		},
	})
	assert.Equal(t, false, got)
}

func TestResolveOr(t *testing.T) {
	rc := baseCtx()
	rc.conditions["A"] = false
	rc.conditions["B"] = true
	got := rc.Resolve(map[string]any{
		"Fn::Or": []any{
			map[string]any{"Condition": "A"},
			map[string]any{"Condition": "B"},
		},
	})
	assert.Equal(t, true, got)
}

func TestResolveEquals_True(t *testing.T) {
	rc := baseCtx()
	rc.params["Stage"] = "prod"
	got := rc.Resolve(map[string]any{
		"Fn::Equals": []any{map[string]any{"Ref": "Stage"}, "prod"},
	})
	assert.Equal(t, true, got)
}

func TestResolveEquals_False(t *testing.T) {
	rc := baseCtx()
	rc.params["Stage"] = "dev"
	got := rc.Resolve(map[string]any{
		"Fn::Equals": []any{map[string]any{"Ref": "Stage"}, "prod"},
	})
	assert.Equal(t, false, got)
}

func TestResolveLength(t *testing.T) {
	rc := baseCtx()
	got := rc.resolveLength([]any{"a", "b", "c"})
	assert.Equal(t, 3, got)
}

func TestEvaluateConditions(t *testing.T) {
	rc := baseCtx()
	rc.params["Env"] = "prod"
	rc.evaluateConditions(map[string]any{
		"IsProd": map[string]any{
			"Fn::Equals": []any{map[string]any{"Ref": "Env"}, "prod"},
		},
		"IsNotProd": map[string]any{
			"Fn::Not": []any{map[string]any{
				"Fn::Equals": []any{map[string]any{"Ref": "Env"}, "prod"},
			}},
		},
	})
	assert.True(t, rc.conditions["IsProd"])
	assert.False(t, rc.conditions["IsNotProd"])
}

func TestResolveParameters_WithDefault(t *testing.T) {
	rc := baseCtx()
	rc.resolveParameters(
		map[string]any{
			"Env":    map[string]any{"Type": "String", "Default": "dev"},
			"Region": map[string]any{"Type": "String"},
		},
		map[string]string{"Region": "eu-west-1"},
	)
	assert.Equal(t, "dev", rc.params["Env"])
	assert.Equal(t, "eu-west-1", rc.params["Region"])
}

func TestResolveParameters_CallerOverridesDefault(t *testing.T) {
	rc := baseCtx()
	rc.resolveParameters(
		map[string]any{"Env": map[string]any{"Type": "String", "Default": "dev"}},
		map[string]string{"Env": "staging"},
	)
	assert.Equal(t, "staging", rc.params["Env"])
}

func TestPlainObject_PassThrough(t *testing.T) {
	rc := baseCtx()
	got := rc.Resolve(map[string]any{"Key": "Value", "Num": 42})
	m, ok := got.(map[string]any)
	assert.True(t, ok)
	assert.Equal(t, "Value", m["Key"])
	assert.Equal(t, 42, m["Num"])
}

func TestPlainSlice_PassThrough(t *testing.T) {
	rc := baseCtx()
	got := rc.Resolve([]any{"x", "y"})
	assert.Equal(t, []any{"x", "y"}, got)
}

func TestSubReplace_NoPlaceholders(t *testing.T) {
	got := subReplace("hello world", func(k string) string { return k })
	assert.Equal(t, "hello world", got)
}

func TestSubReplace_Multiple(t *testing.T) {
	vars := map[string]string{"A": "foo", "B": "bar"}
	got := subReplace("${A}-${B}", func(k string) string { return vars[k] })
	assert.Equal(t, "foo-bar", got)
}

func TestImportValue(t *testing.T) {
	rc := baseCtx()
	rc.exports = NewExportTable()
	rc.exports.Register("SharedVpcId", "vpc-abc123", "arn:aws:cloudformation:us-east-1:123456789012:stack/base/xxx")
	got := rc.Resolve(map[string]any{"Fn::ImportValue": "SharedVpcId"})
	assert.Equal(t, "vpc-abc123", got)
}

func TestImportValue_Missing(t *testing.T) {
	rc := baseCtx()
	rc.exports = NewExportTable()
	got := rc.Resolve(map[string]any{"Fn::ImportValue": "NoSuchExport"})
	assert.Equal(t, "${NoSuchExport}", got)
}

func TestCidr(t *testing.T) {
	rc := baseCtx()
	got := rc.Resolve(map[string]any{
		"Fn::Cidr": []any{"10.0.0.0/16", float64(2), float64(8)},
	})
	subnets, ok := got.([]any)
	assert.True(t, ok)
	assert.Len(t, subnets, 2)
	assert.Equal(t, "10.0.0.0/24", subnets[0])
	assert.Equal(t, "10.0.1.0/24", subnets[1])
}

func TestGetAZs(t *testing.T) {
	rc := baseCtx()
	got := rc.Resolve(map[string]any{"Fn::GetAZs": ""})
	assert.Equal(t, []any{"us-east-1a", "us-east-1b", "us-east-1c"}, got)
}

func TestGetAZs_ExplicitRegion(t *testing.T) {
	rc := baseCtx()
	got := rc.Resolve(map[string]any{"Fn::GetAZs": "eu-west-1"})
	assert.Equal(t, []any{"eu-west-1a", "eu-west-1b", "eu-west-1c"}, got)
}

func TestToJsonString(t *testing.T) {
	rc := baseCtx()
	got := rc.Resolve(map[string]any{
		"Fn::ToJsonString": map[string]any{"k": "v"},
	})
	assert.Equal(t, `{"k":"v"}`, got)
}

func TestSubDottedGetAtt(t *testing.T) {
	rc := baseCtx()
	rc.resources["MyRole"] = &cfResource{
		PhysicalResourceId: "arn:aws:iam::123456789012:role/MyRole",
		Attributes:         map[string]any{"Arn": "arn:aws:iam::123456789012:role/MyRole"},
	}
	got := rc.Resolve(map[string]any{"Fn::Sub": "hello-${MyRole.Arn}"})
	assert.Equal(t, "hello-arn:aws:iam::123456789012:role/MyRole", got)
}
