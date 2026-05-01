package spark

import (
	"context"
	"errors"
	"sync"
)

// fakeK8sClient is an in-memory k8sClientInterface for unit tests.
// It stores jobs in a map and supports simulating error conditions
// via the InjectErr fields.
type fakeK8sClient struct {
	mu sync.Mutex

	jobs map[string]*batchJob // name → job

	// Inject errors for specific operations (nil means no error).
	CreateErr  error
	GetErr     error
	DeleteErr  error
	ListErr    error
	SuspendErr error

	// Track calls for assertion in tests.
	Created   []string
	Deleted   []string
	Suspended []string
	Resumed   []string
}

func newFakeK8sClient() *fakeK8sClient {
	return &fakeK8sClient{jobs: make(map[string]*batchJob)}
}

func (f *fakeK8sClient) createJob(_ context.Context, job batchJob) error {
	if f.CreateErr != nil {
		return f.CreateErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.jobs[job.Metadata.Name]; exists {
		return &k8sAPIError{StatusCode: 409, Body: "already exists"}
	}
	cp := job
	f.jobs[job.Metadata.Name] = &cp
	f.Created = append(f.Created, job.Metadata.Name)
	return nil
}

func (f *fakeK8sClient) getJob(_ context.Context, name string) (*batchJob, error) {
	if f.GetErr != nil {
		return nil, f.GetErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	job, ok := f.jobs[name]
	if !ok {
		return nil, &k8sAPIError{StatusCode: 404, Body: "not found"}
	}
	cp := *job
	return &cp, nil
}

func (f *fakeK8sClient) deleteJob(_ context.Context, name string) error {
	if f.DeleteErr != nil {
		return f.DeleteErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.jobs[name]; !ok {
		return &k8sAPIError{StatusCode: 404, Body: "not found"}
	}
	delete(f.jobs, name)
	f.Deleted = append(f.Deleted, name)
	return nil
}

func (f *fakeK8sClient) listJobs(_ context.Context, _ string) ([]jobListItem, error) {
	if f.ListErr != nil {
		return nil, f.ListErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]jobListItem, 0, len(f.jobs))
	for _, j := range f.jobs {
		out = append(out, jobListItem{
			Metadata: jobListMeta{
				Name:   j.Metadata.Name,
				Labels: j.Metadata.Labels,
			},
			Status: j.Status,
			Spec:   jobListSpec{Suspend: j.Spec.Suspend},
		})
	}
	return out, nil
}

func (f *fakeK8sClient) suspendJob(_ context.Context, name string) error {
	if f.SuspendErr != nil {
		return f.SuspendErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	job, ok := f.jobs[name]
	if !ok {
		return &k8sAPIError{StatusCode: 404, Body: "not found"}
	}
	t := true
	job.Spec.Suspend = &t
	f.Suspended = append(f.Suspended, name)
	return nil
}

func (f *fakeK8sClient) unsuspendJob(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	job, ok := f.jobs[name]
	if !ok {
		return &k8sAPIError{StatusCode: 404, Body: "not found"}
	}
	f2 := false
	job.Spec.Suspend = &f2
	f.Resumed = append(f.Resumed, name)
	return nil
}

// seedJob adds a job directly to the fake store (for test setup).
func (f *fakeK8sClient) seedJob(job batchJob) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := job
	f.jobs[job.Metadata.Name] = &cp
}

// setJobStatus sets a job's status (simulates the K8s controller updating status).
func (f *fakeK8sClient) setJobStatus(name string, status batchJobStatus) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if j, ok := f.jobs[name]; ok {
		j.Status = status
	}
}

// Compile-time check: fakeK8sClient must implement k8sClientInterface.
var _ k8sClientInterface = (*fakeK8sClient)(nil)

// errIsNotFound is a helper for tests to check 404 API errors.
func errIsNotFound(err error) bool {
	return errors.Is(err, ErrJobNotFound)
}
