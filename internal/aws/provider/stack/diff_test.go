package stack_test

import (
	"testing"

	"jaiscloud/internal/aws/provider/stack"
)

func TestBuildChangeSetAdd(t *testing.T) {
	old := map[string]any{"Resources": map[string]any{}}
	new := map[string]any{"Resources": map[string]any{
		"Q": map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{"VisibilityTimeout": 30}},
	}}
	changes := stack.BuildChangeSet(old, new, nil)
	if len(changes) != 1 || changes[0].Action != "Add" {
		t.Fatalf("expected 1 Add, got %+v", changes)
	}
}

func TestBuildChangeSetModifyNoReplacement(t *testing.T) {
	old := map[string]any{"Resources": map[string]any{
		"Q": map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{"VisibilityTimeout": 30}},
	}}
	new := map[string]any{"Resources": map[string]any{
		"Q": map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{"VisibilityTimeout": 60}},
	}}
	handlers := map[string]stack.ResourceHandler{
		"AWS::SQS::Queue": {ReplacementRules: stack.ReplacementRules{RequireUpdate: []string{"VisibilityTimeout"}}},
	}
	changes := stack.BuildChangeSet(old, new, handlers)
	if len(changes) != 1 || changes[0].Action != "Modify" || changes[0].Replacement != "False" {
		t.Fatalf("expected 1 Modify/False, got %+v", changes)
	}
}

func TestBuildChangeSetRemove(t *testing.T) {
	old := map[string]any{"Resources": map[string]any{
		"Q": map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{}},
	}}
	new := map[string]any{"Resources": map[string]any{}}
	changes := stack.BuildChangeSet(old, new, nil)
	if len(changes) != 1 || changes[0].Action != "Remove" {
		t.Fatalf("expected 1 Remove, got %+v", changes)
	}
}
