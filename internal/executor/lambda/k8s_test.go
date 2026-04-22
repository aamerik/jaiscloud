package lambda

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeK8s is a minimal fake Kubernetes API server for unit tests.
type fakeK8s struct {
	mu       sync.Mutex
	pods     []string // pod names created
	svcs     []string // service names created
	deleted  []string // names deleted (pods and services)
}

func newFakeK8sServer(f *fakeK8s) *httptest.Server {
	mux := http.NewServeMux()

	// POST pod
	mux.HandleFunc("/api/v1/namespaces/jaiscloud/pods", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			meta, _ := body["metadata"].(map[string]any)
			name, _ := meta["name"].(string)
			f.mu.Lock()
			f.pods = append(f.pods, name)
			f.mu.Unlock()
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(body)
			return
		}
		// GET list pods by label
		if r.Method == http.MethodGet {
			f.mu.Lock()
			items := make([]map[string]any, len(f.pods))
			for i, n := range f.pods {
				items[i] = map[string]any{"metadata": map[string]any{"name": n}}
			}
			f.mu.Unlock()
			json.NewEncoder(w).Encode(map[string]any{"items": items})
		}
	})

	// DELETE or GET individual pod
	mux.HandleFunc("/api/v1/namespaces/jaiscloud/pods/", func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/api/v1/namespaces/jaiscloud/pods/")
		if r.Method == http.MethodDelete {
			f.mu.Lock()
			f.deleted = append(f.deleted, name)
			f.mu.Unlock()
			w.WriteHeader(http.StatusOK)
			return
		}
		// GET — return Running+Ready pod status
		resp := map[string]any{
			"status": map[string]any{
				"phase": "Running",
				"conditions": []map[string]any{
					{"type": "Ready", "status": "True"},
				},
			},
		}
		json.NewEncoder(w).Encode(resp)
	})

	// POST service
	mux.HandleFunc("/api/v1/namespaces/jaiscloud/services", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			meta, _ := body["metadata"].(map[string]any)
			name, _ := meta["name"].(string)
			f.mu.Lock()
			f.svcs = append(f.svcs, name)
			f.mu.Unlock()
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(body)
			return
		}
		// GET list services by label
		if r.Method == http.MethodGet {
			f.mu.Lock()
			items := make([]map[string]any, len(f.svcs))
			for i, n := range f.svcs {
				items[i] = map[string]any{"metadata": map[string]any{"name": n}}
			}
			f.mu.Unlock()
			json.NewEncoder(w).Encode(map[string]any{"items": items})
		}
	})

	// DELETE individual service
	mux.HandleFunc("/api/v1/namespaces/jaiscloud/services/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			name := strings.TrimPrefix(r.URL.Path, "/api/v1/namespaces/jaiscloud/services/")
			f.mu.Lock()
			f.deleted = append(f.deleted, name)
			f.mu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
	})

	return httptest.NewTLSServer(mux)
}

func newTestK8sExecutor(t *testing.T, srv *httptest.Server) *K8sExecutor {
	t.Helper()
	cfg := LambdaConfig{
		Namespace:     "jaiscloud",
		APIServer:     srv.URL,
		KeepaliveSecs: 300,
	}
	e := &K8sExecutor{
		cfg:    cfg,
		k8s:    srv.Client(),
		invoke: &http.Client{Timeout: 5 * time.Second},
		pods:   make(map[string]*warmPod),
		done:   make(chan struct{}),
	}
	return e
}

func TestK8sLambda_Close_DeletesAllWarmPods(t *testing.T) {
	f := &fakeK8s{}
	srv := newFakeK8sServer(f)
	defer srv.Close()

	e := newTestK8sExecutor(t, srv)

	// Manually insert warm pods.
	e.pods["fn-a"] = &warmPod{podName: "jc-lambda-fn-a-0001", svcName: "jc-lambda-fn-a", endpoint: "http://fake:8080"}
	e.pods["fn-b"] = &warmPod{podName: "jc-lambda-fn-b-0002", svcName: "jc-lambda-fn-b", endpoint: "http://fake:8080"}

	require.NoError(t, e.Close())

	f.mu.Lock()
	defer f.mu.Unlock()
	assert.Contains(t, f.deleted, "jc-lambda-fn-a-0001")
	assert.Contains(t, f.deleted, "jc-lambda-fn-b-0002")
	assert.Contains(t, f.deleted, "jc-lambda-fn-a")
	assert.Contains(t, f.deleted, "jc-lambda-fn-b")
}

func TestK8sLambda_CleanupOrphans_DeletesOrphanedPodsAndServices(t *testing.T) {
	f := &fakeK8s{
		pods: []string{"jc-lambda-old-pod-aaaa"},
		svcs: []string{"jc-lambda-old-svc"},
	}
	srv := newFakeK8sServer(f)
	defer srv.Close()

	e := newTestK8sExecutor(t, srv)
	e.cleanupOrphans()

	f.mu.Lock()
	defer f.mu.Unlock()
	assert.Contains(t, f.deleted, "jc-lambda-old-pod-aaaa")
	assert.Contains(t, f.deleted, "jc-lambda-old-svc")
}

func TestK8sLambda_CleanupOrphans_NoOrphans_NoOp(t *testing.T) {
	f := &fakeK8s{}
	srv := newFakeK8sServer(f)
	defer srv.Close()

	e := newTestK8sExecutor(t, srv)
	e.cleanupOrphans()

	f.mu.Lock()
	defer f.mu.Unlock()
	assert.Empty(t, f.deleted)
}

func TestK8sLambda_DeleteFunction_RemovesPodAndService(t *testing.T) {
	f := &fakeK8s{}
	srv := newFakeK8sServer(f)
	defer srv.Close()

	e := newTestK8sExecutor(t, srv)
	e.pods["my-fn"] = &warmPod{
		podName:  "jc-lambda-my-fn-1234",
		svcName:  "jc-lambda-my-fn",
		endpoint: "http://fake:8080",
	}

	e.DeleteFunction(context.Background(), "my-fn")

	f.mu.Lock()
	defer f.mu.Unlock()
	assert.Contains(t, f.deleted, "jc-lambda-my-fn-1234")
	assert.Contains(t, f.deleted, "jc-lambda-my-fn")

	e.mu.Lock()
	_, exists := e.pods["my-fn"]
	e.mu.Unlock()
	assert.False(t, exists, "pod entry must be removed from map")
}
