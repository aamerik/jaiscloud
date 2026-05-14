package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEKSNodegroupRoundtrip tests that nodegroup paths are routable (fix 1.1.5).
// Uses raw HTTP because the EKS SDK is not a project dependency.
func TestEKSNodegroupRoundtrip(t *testing.T) {
	resetState(t)

	do := func(method, path string, body any) (int, map[string]any) {
		t.Helper()
		var r io.Reader
		if body != nil {
			b, _ := json.Marshal(body)
			r = bytes.NewReader(b)
		}
		req, _ := http.NewRequest(method, jaiscloudEndpoint+path, r)
		req.Header.Set("Authorization", `AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/eks/aws4_request, SignedHeaders=host, Signature=abc`)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		var out map[string]any
		json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}

	// CreateCluster
	status, out := do("POST", "/clusters", map[string]any{
		"name": "my-cluster",
		"resourcesVpcConfig": map[string]any{
			"subnetIds": []string{"subnet-1"},
		},
	})
	assert.Equal(t, http.StatusOK, status, "CreateCluster: %v", out)
	clusterName := "my-cluster"

	// CreateNodegroup — this path was unreachable before fix 1.1.5.
	status, out = do("POST", fmt.Sprintf("/clusters/%s/node-groups", clusterName), map[string]any{
		"nodegroupName": "ng-1",
		"nodeRole":      "arn:aws:iam::000000000000:role/NodeRole",
		"subnets":       []string{"subnet-1"},
		"scalingConfig": map[string]any{"minSize": 1, "maxSize": 3, "desiredSize": 1},
	})
	assert.Equal(t, http.StatusOK, status, "CreateNodegroup must not return 400, got: %v", out)

	// DescribeNodegroup
	status, _ = do("GET", fmt.Sprintf("/clusters/%s/node-groups/ng-1", clusterName), nil)
	assert.NotEqual(t, 400, status, "DescribeNodegroup must not return InvalidRequest 400")

	// ListNodegroups
	status, _ = do("GET", fmt.Sprintf("/clusters/%s/node-groups", clusterName), nil)
	assert.NotEqual(t, 400, status, "ListNodegroups must not return InvalidRequest 400")

	// DeleteNodegroup
	status, _ = do("DELETE", fmt.Sprintf("/clusters/%s/node-groups/ng-1", clusterName), nil)
	assert.NotEqual(t, 400, status, "DeleteNodegroup must not return InvalidRequest 400")
}
