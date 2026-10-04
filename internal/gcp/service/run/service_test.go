package run

import (
	"context"
	"errors"
	"strings"
	"testing"

	"jaiscloud/internal/events"
	runstore "jaiscloud/internal/gcp/store/run"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newTestService() *Service {
	return NewService(runstore.NewMemoryStore(), store.NewMemoryResourceStore())
}

// A wired event bus receives one cloud-neutral status event per service
// mutation so the console's live stream can invalidate the GCP/run query tree.
func TestMutationsPublishStatusEvents(t *testing.T) {
	ctx := context.Background()
	bus := events.NewEventBus()
	s := NewService(runstore.NewMemoryStore(), store.NewMemoryResourceStore(), WithEventBus(bus))

	var got []events.StatusEvent
	bus.Subscribe(events.EventStatus, func(e events.Event) {
		if se, ok := e.Payload.(events.StatusEvent); ok {
			got = append(got, se)
		}
	})

	if _, err := s.CreateService(ctx, "proj", "us-central1", "svc", createRequest()); err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	if _, err := s.UpdateService(ctx, "proj", "us-central1", "svc", createRequest(), "template"); err != nil {
		t.Fatalf("UpdateService: %v", err)
	}
	if _, err := s.DeleteService(ctx, "proj", "us-central1", "svc"); err != nil {
		t.Fatalf("DeleteService: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("status events = %d, want 3: %+v", len(got), got)
	}
	if got[0].State != "READY" || got[1].State != "READY" || got[2].State != "DELETED" {
		t.Fatalf("states = %q,%q,%q", got[0].State, got[1].State, got[2].State)
	}
	first := got[0]
	if first.Cloud != model.CloudGCP || first.ID != "svc" || first.Resource != "gcp-run-service" {
		t.Fatalf("first event = %+v", first)
	}
	if len(first.Keys) != 2 || first.Keys[0] != "gcp" || first.Keys[1] != "run" {
		t.Fatalf("first keys = %v", first.Keys)
	}
}

func createRequest() map[string]any {
	return map[string]any{
		"template": map[string]any{
			"containers": []any{
				map[string]any{
					"image": "nginx:latest",
					"ports": []any{map[string]any{"containerPort": float64(80)}},
				},
			},
		},
	}
}

func TestCreateGetListDeleteService(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	op, err := s.CreateService(ctx, "proj", "us-central1", "svc", createRequest())
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	if !op.Done {
		t.Fatalf("operation not done synchronously")
	}
	if got := op.ID; !strings.HasPrefix(got, "operation-run-") {
		t.Errorf("operation id = %q, want operation-run- prefix", got)
	}
	if op.Target != "projects/proj/locations/us-central1/services/svc" {
		t.Errorf("operation target = %q", op.Target)
	}

	svc, err := s.GetService(ctx, "proj", "us-central1", "svc")
	if err != nil {
		t.Fatalf("GetService: %v", err)
	}
	js := ServiceJSON(svc)
	if js["name"] != "projects/proj/locations/us-central1/services/svc" {
		t.Errorf("name = %v", js["name"])
	}
	uri, _ := js["uri"].(string)
	if !strings.HasPrefix(uri, "https://svc-") || !strings.HasSuffix(uri, ".us-central1.run.app") {
		t.Errorf("uri = %q", uri)
	}
	if lrr, _ := js["latestReadyRevision"].(string); !strings.Contains(lrr, "/revisions/") {
		t.Errorf("latestReadyRevision = %q", lrr)
	}
	cond, _ := js["terminalCondition"].(map[string]any)
	if cond["type"] != "Ready" || cond["state"] != "CONDITION_SUCCEEDED" {
		t.Errorf("terminalCondition = %v", cond)
	}
	statuses, _ := js["trafficStatuses"].([]any)
	if len(statuses) != 1 || statuses[0].(map[string]any)["percent"] != 100 {
		t.Errorf("trafficStatuses = %v", statuses)
	}
	urls, _ := js["urls"].([]any)
	if len(urls) != 1 || urls[0] != uri {
		t.Errorf("urls = %v", urls)
	}

	// Revisions.
	revs, err := s.ListRevisions(ctx, "proj", "us-central1", "svc")
	if err != nil {
		t.Fatalf("ListRevisions: %v", err)
	}
	if len(revs) != 1 || revs[0].ID != "svc-00001" {
		t.Fatalf("revisions = %+v", revs)
	}
	rev, err := s.GetRevision(ctx, "proj", "us-central1", "svc", "svc-00001")
	if err != nil {
		t.Fatalf("GetRevision: %v", err)
	}
	rj := RevisionJSON(rev)
	if rj["service"] != "projects/proj/locations/us-central1/services/svc" {
		t.Errorf("revision service = %v", rj["service"])
	}
	containers, _ := rj["containers"].([]any)
	if len(containers) != 1 || containers[0].(map[string]any)["image"] != "nginx:latest" {
		t.Errorf("revision containers = %v", containers)
	}

	// Delete.
	delOp, err := s.DeleteService(ctx, "proj", "us-central1", "svc")
	if err != nil {
		t.Fatalf("DeleteService: %v", err)
	}
	if !delOp.Done || delOp.Service == nil || delOp.Service.DeleteTime.IsZero() {
		t.Errorf("delete op = %+v", delOp)
	}
	if _, err := s.GetService(ctx, "proj", "us-central1", "svc"); !isNotFound(err) {
		t.Errorf("GetService after delete err = %v, want NotFound", err)
	}
	if _, err := s.GetRevision(ctx, "proj", "us-central1", "svc", "svc-00001"); !isNotFound(err) {
		t.Errorf("GetRevision after delete err = %v, want NotFound", err)
	}
}

func TestCreateDuplicate(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateService(ctx, "p", "l", "svc", nil); err != nil {
		t.Fatalf("first create: %v", err)
	}
	_, err := s.CreateService(ctx, "p", "l", "svc", nil)
	var perr *model.ProviderError
	if !errors.As(err, &perr) || perr.HTTPStatus != 409 {
		t.Fatalf("duplicate create err = %v, want 409", err)
	}
}

func TestCreateInvalidID(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	_, err := s.CreateService(ctx, "p", "l", "Bad_ID", nil)
	var perr *model.ProviderError
	if !errors.As(err, &perr) || perr.HTTPStatus != 400 {
		t.Fatalf("invalid id err = %v, want 400", err)
	}
}

func TestUpdateMintsRevision(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateService(ctx, "p", "l", "svc", createRequest()); err != nil {
		t.Fatalf("create: %v", err)
	}
	newBody := map[string]any{"template": map[string]any{"containers": []any{map[string]any{"image": "httpd:latest"}}}}
	op, err := s.UpdateService(ctx, "p", "l", "svc", newBody, "template")
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if op.Verb != "update" {
		t.Errorf("op verb = %q", op.Verb)
	}
	if got := op.Service.LatestCreatedRevision; !strings.HasSuffix(got, "/revisions/svc-00002") {
		t.Errorf("latestCreatedRevision = %q, want svc-00002", got)
	}
	if op.Service.LatestReadyRevision != op.Service.LatestCreatedRevision {
		t.Errorf("mock mode should make the new revision ready")
	}
	revs, _ := s.ListRevisions(ctx, "p", "l", "svc")
	if len(revs) != 2 {
		t.Fatalf("revisions = %d, want 2", len(revs))
	}
}

func TestIAMPolicy(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateService(ctx, "p", "l", "svc", nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	pol, err := s.ServiceSetIamPolicy(ctx, "p", "l", "svc", map[string]any{
		"policy": map[string]any{"bindings": []any{
			map[string]any{"role": "roles/run.invoker", "members": []any{"allUsers"}},
		}},
	})
	if err != nil {
		t.Fatalf("set iam: %v", err)
	}
	if len(pol.Bindings) != 1 {
		t.Fatalf("bindings = %v", pol.Bindings)
	}
	got, err := s.ServiceGetIamPolicy(ctx, "p", "l", "svc")
	if err != nil {
		t.Fatalf("get iam: %v", err)
	}
	if len(got.Bindings) != 1 {
		t.Errorf("get bindings = %v", got.Bindings)
	}
	perms, err := s.ServiceTestIamPermissions(ctx, "p", "l", "svc", []string{"run.services.get"})
	if err != nil {
		t.Fatalf("test iam: %v", err)
	}
	if len(perms) != 1 || perms[0] != "run.services.get" {
		t.Errorf("perms = %v", perms)
	}
	if _, err := s.ServiceGetIamPolicy(ctx, "p", "l", "missing"); !isNotFound(err) {
		t.Errorf("iam on missing service err = %v, want NotFound", err)
	}
}

func TestOperations(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	op, err := s.CreateService(ctx, "p", "l", "svc", nil)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := s.GetOperation(ctx, "p", "l", op.ID)
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	js := OperationJSON(got)
	if js["name"] != "projects/p/locations/l/operations/"+op.ID {
		t.Errorf("op name = %v", js["name"])
	}
	if js["done"] != true {
		t.Errorf("op done = %v", js["done"])
	}
	resp, _ := js["response"].(map[string]any)
	if resp["@type"] != serviceTypeURL {
		t.Errorf("response @type = %v", resp["@type"])
	}
	if got, err := s.WaitOperation(ctx, "p", "l", op.ID); err != nil || got.ID != op.ID {
		t.Errorf("WaitOperation = %v, %v", got.ID, err)
	}
	ops, err := s.ListOperations(ctx, "p", "l")
	if err != nil || len(ops) != 1 {
		t.Fatalf("ListOperations = %v, %v", ops, err)
	}
	if err := s.CancelOperation(ctx, "p", "l", op.ID); err != nil {
		t.Errorf("CancelOperation: %v", err)
	}
	if err := s.DeleteOperation(ctx, "p", "l", op.ID); err != nil {
		t.Errorf("DeleteOperation: %v", err)
	}
	if _, err := s.GetOperation(ctx, "p", "l", op.ID); !isNotFound(err) {
		t.Errorf("GetOperation after delete err = %v, want NotFound", err)
	}
}

func TestListAllServicesAcrossLocations(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateService(ctx, "proj", "us-central1", "beta", createRequest()); err != nil {
		t.Fatalf("CreateService beta: %v", err)
	}
	if _, err := s.CreateService(ctx, "proj", "europe-west1", "alpha", createRequest()); err != nil {
		t.Fatalf("CreateService alpha: %v", err)
	}
	if _, err := s.CreateService(ctx, "other", "us-central1", "gamma", createRequest()); err != nil {
		t.Fatalf("CreateService gamma: %v", err)
	}

	svcs, err := s.ListAllServices(ctx, "proj")
	if err != nil {
		t.Fatalf("ListAllServices: %v", err)
	}
	if len(svcs) != 2 {
		t.Fatalf("got %d services, want 2", len(svcs))
	}
	if svcs[0].ID != "alpha" || svcs[0].Location != "europe-west1" || svcs[1].ID != "beta" || svcs[1].Location != "us-central1" {
		t.Fatalf("unexpected order/scope: %+v", svcs)
	}
}

func TestDeleteRevisionRetired(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateService(ctx, "proj", "us-central1", "svc", createRequest()); err != nil {
		t.Fatalf("create: %v", err)
	}
	newBody := map[string]any{"template": map[string]any{"containers": []any{map[string]any{"image": "httpd:latest"}}}}
	if _, err := s.UpdateService(ctx, "proj", "us-central1", "svc", newBody, "template"); err != nil {
		t.Fatalf("update: %v", err)
	}
	op, err := s.DeleteRevision(ctx, "proj", "us-central1", "svc", "svc-00001", false)
	if err != nil {
		t.Fatalf("DeleteRevision: %v", err)
	}
	if op.Revision == nil || op.Revision.ID != "svc-00001" {
		t.Fatalf("op revision = %+v", op.Revision)
	}
	if op.Revision.DeleteTime.IsZero() {
		t.Errorf("delete operation revision has no deleteTime")
	}
	resp, _ := OperationJSON(op)["response"].(map[string]any)
	if resp["@type"] != revisionTypeURL {
		t.Errorf("response @type = %v, want %v", resp["@type"], revisionTypeURL)
	}
	if resp["deleteTime"] == "" || resp["deleteTime"] == nil {
		t.Errorf("response deleteTime missing: %v", resp["deleteTime"])
	}
	if _, err := s.GetRevision(ctx, "proj", "us-central1", "svc", "svc-00001"); !isNotFound(err) {
		t.Errorf("retired revision still present: %v", err)
	}
	if _, err := s.GetRevision(ctx, "proj", "us-central1", "svc", "svc-00002"); err != nil {
		t.Errorf("serving revision removed: %v", err)
	}
}

func TestDeleteRevisionServingRejected(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateService(ctx, "proj", "us-central1", "svc", createRequest()); err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err := s.DeleteRevision(ctx, "proj", "us-central1", "svc", "svc-00001", false)
	var perr *model.ProviderError
	if !errors.As(err, &perr) || perr.Code != "FailedPrecondition" || perr.HTTPStatus != 400 {
		t.Fatalf("serving delete err = %v, want FailedPrecondition/400", err)
	}
}

func TestDeleteRevisionValidateOnly(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateService(ctx, "proj", "us-central1", "svc", createRequest()); err != nil {
		t.Fatalf("create: %v", err)
	}
	newBody := map[string]any{"template": map[string]any{"containers": []any{map[string]any{"image": "httpd:latest"}}}}
	if _, err := s.UpdateService(ctx, "proj", "us-central1", "svc", newBody, "template"); err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, err := s.DeleteRevision(ctx, "proj", "us-central1", "svc", "svc-00001", true); err != nil {
		t.Fatalf("validate-only DeleteRevision: %v", err)
	}
	if _, err := s.GetRevision(ctx, "proj", "us-central1", "svc", "svc-00001"); err != nil {
		t.Errorf("validate-only removed the revision: %v", err)
	}
	ops, _ := s.ListOperations(ctx, "proj", "us-central1")
	for _, op := range ops {
		if op.Revision != nil {
			t.Errorf("validate-only recorded an operation: %+v", op)
		}
	}
	if _, err := s.DeleteRevision(ctx, "proj", "us-central1", "svc", "svc-99999", false); !isNotFound(err) {
		t.Errorf("delete missing revision err = %v, want NotFound", err)
	}
}

func isNotFound(err error) bool {
	var perr *model.ProviderError
	return errors.As(err, &perr) && perr.HTTPStatus == 404
}
