package stack_test

import (
	"testing"

	"jaiscloud/internal/aws/provider/stack"
)

func TestParseTemplateJSON(t *testing.T) {
	body := `{"AWSTemplateFormatVersion":"2010-09-09","Resources":{"Q":{"Type":"AWS::SQS::Queue"}}}`
	out, err := stack.ParseTemplate([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if out["AWSTemplateFormatVersion"] != "2010-09-09" {
		t.Fatal("version missing")
	}
}

func TestParseTemplateYAML(t *testing.T) {
	body := `
AWSTemplateFormatVersion: "2010-09-09"
Parameters:
  BucketName:
    Type: String
Resources:
  MyBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Ref BucketName
Outputs:
  BucketOut:
    Value: !Sub "bucket-${BucketName}"
`
	out, err := stack.ParseTemplate([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	resources, _ := out["Resources"].(map[string]any)
	bucket, _ := resources["MyBucket"].(map[string]any)
	props, _ := bucket["Properties"].(map[string]any)
	ref, _ := props["BucketName"].(map[string]any)
	if ref["Ref"] != "BucketName" {
		t.Fatalf("expected Ref=BucketName, got %v", ref)
	}
}
