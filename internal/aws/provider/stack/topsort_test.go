package stack

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopoSort_NoDependencies(t *testing.T) {
	resources := map[string]any{
		"Queue": map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{}},
		"Topic": map[string]any{"Type": "AWS::SNS::Topic", "Properties": map[string]any{}},
	}
	order, err := topoSort(resources)
	require.NoError(t, err)
	assert.Len(t, order, 2)
	// No dependency between them — both orderings are valid.
	assert.ElementsMatch(t, []string{"Queue", "Topic"}, order)
}

func TestTopoSort_ExplicitDependsOn(t *testing.T) {
	resources := map[string]any{
		"Queue": map[string]any{
			"Type":       "AWS::SQS::Queue",
			"Properties": map[string]any{},
		},
		"Topic": map[string]any{
			"Type":       "AWS::SNS::Topic",
			"DependsOn":  "Queue",
			"Properties": map[string]any{},
		},
	}
	order, err := topoSort(resources)
	require.NoError(t, err)
	require.Len(t, order, 2)
	assert.Equal(t, "Queue", order[0])
	assert.Equal(t, "Topic", order[1])
}

func TestTopoSort_ExplicitDependsOnList(t *testing.T) {
	resources := map[string]any{
		"A": map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{}},
		"B": map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{}},
		"C": map[string]any{
			"Type":       "AWS::SNS::Topic",
			"DependsOn":  []any{"A", "B"},
			"Properties": map[string]any{},
		},
	}
	order, err := topoSort(resources)
	require.NoError(t, err)
	require.Len(t, order, 3)
	cIdx := indexOf(order, "C")
	aIdx := indexOf(order, "A")
	bIdx := indexOf(order, "B")
	assert.Greater(t, cIdx, aIdx, "C must come after A")
	assert.Greater(t, cIdx, bIdx, "C must come after B")
}

func TestTopoSort_ImplicitRefDependency(t *testing.T) {
	resources := map[string]any{
		"MyQueue": map[string]any{
			"Type":       "AWS::SQS::Queue",
			"Properties": map[string]any{},
		},
		"MyTopic": map[string]any{
			"Type": "AWS::SNS::Topic",
			"Properties": map[string]any{
				"Subscription": map[string]any{
					"Endpoint": map[string]any{"Ref": "MyQueue"},
					"Protocol": "sqs",
				},
			},
		},
	}
	order, err := topoSort(resources)
	require.NoError(t, err)
	require.Len(t, order, 2)
	assert.Equal(t, "MyQueue", order[0])
	assert.Equal(t, "MyTopic", order[1])
}

func TestTopoSort_ImplicitGetAttDependency(t *testing.T) {
	resources := map[string]any{
		"MyBucket": map[string]any{
			"Type":       "AWS::S3::Bucket",
			"Properties": map[string]any{},
		},
		"MyFunction": map[string]any{
			"Type": "AWS::Lambda::Function",
			"Properties": map[string]any{
				"Environment": map[string]any{
					"Variables": map[string]any{
						"BUCKET_ARN": map[string]any{"Fn::GetAtt": []any{"MyBucket", "Arn"}},
					},
				},
			},
		},
	}
	order, err := topoSort(resources)
	require.NoError(t, err)
	require.Len(t, order, 2)
	assert.Equal(t, "MyBucket", order[0])
	assert.Equal(t, "MyFunction", order[1])
}

func TestTopoSort_ChainDependency(t *testing.T) {
	// A ← B ← C (A must come first, C last)
	resources := map[string]any{
		"A": map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{}},
		"B": map[string]any{
			"Type":       "AWS::SNS::Topic",
			"DependsOn":  "A",
			"Properties": map[string]any{},
		},
		"C": map[string]any{
			"Type":       "AWS::Lambda::Function",
			"DependsOn":  "B",
			"Properties": map[string]any{},
		},
	}
	order, err := topoSort(resources)
	require.NoError(t, err)
	require.Len(t, order, 3)
	assert.Equal(t, "A", order[0])
	assert.Equal(t, "B", order[1])
	assert.Equal(t, "C", order[2])
}

func TestTopoSort_SingleResource(t *testing.T) {
	resources := map[string]any{
		"Alone": map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{}},
	}
	order, err := topoSort(resources)
	require.NoError(t, err)
	assert.Equal(t, []string{"Alone"}, order)
}

func TestTopoSort_Empty(t *testing.T) {
	order, err := topoSort(map[string]any{})
	require.NoError(t, err)
	assert.Empty(t, order)
}

func TestTopoSort_CycleDetected(t *testing.T) {
	resources := map[string]any{
		"A": map[string]any{
			"Type":       "AWS::SQS::Queue",
			"DependsOn":  "B",
			"Properties": map[string]any{},
		},
		"B": map[string]any{
			"Type":       "AWS::SNS::Topic",
			"DependsOn":  "A",
			"Properties": map[string]any{},
		},
	}
	_, err := topoSort(resources)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "circular dependency")
}

func TestTopoSort_ExternalRefIgnored(t *testing.T) {
	// A Ref to a parameter (not a resource) must not create a dependency.
	resources := map[string]any{
		"MyQueue": map[string]any{
			"Type": "AWS::SQS::Queue",
			"Properties": map[string]any{
				"QueueName": map[string]any{"Ref": "SomeParameter"},
			},
		},
	}
	order, err := topoSort(resources)
	require.NoError(t, err)
	assert.Equal(t, []string{"MyQueue"}, order)
}

func TestTopoSort_DeterministicOrder(t *testing.T) {
	// Without edges, alphabetical order should be stable across runs.
	resources := map[string]any{
		"Zebra":  map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{}},
		"Alpha":  map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{}},
		"Middle": map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{}},
	}
	order, err := topoSort(resources)
	require.NoError(t, err)
	assert.Equal(t, []string{"Alpha", "Middle", "Zebra"}, order)
}

// indexOf returns the position of s in slice, or -1.
func indexOf(slice []string, s string) int {
	for i, v := range slice {
		if v == s {
			return i
		}
	}
	return -1
}
