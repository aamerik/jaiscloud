package spark

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"gopkg.in/yaml.v3"

	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/k8stypes"
)

// PodTemplateLoader fetches and parses Spark pod templates from object storage.
// Cloud-neutral: accepts any BlobFetcher so the same loader handles s3://, s3a://
// URIs today; abfss:// and gs:// will work when matching fetcher implementations
// land. Currently only S3BlobFetcher is implemented — callers passing abfss:// or
// gs:// URIs will receive an "unsupported URI scheme" error from Fetch.
type PodTemplateLoader struct {
	fetcher  blobfs.BlobFetcher
	maxBytes int64 // 0 means no cap
}

// NewPodTemplateLoader constructs a loader. maxBytes <= 0 means no size cap.
func NewPodTemplateLoader(f blobfs.BlobFetcher, maxBytes int64) *PodTemplateLoader {
	return &PodTemplateLoader{fetcher: f, maxBytes: maxBytes}
}

// Load fetches and parses driver and executor templates concurrently.
// Either URI may be empty — the corresponding return value is nil.
// Returns (nil, nil, nil) when both URIs are empty.
func (l *PodTemplateLoader) Load(
	ctx context.Context,
	driverURI, executorURI string,
) (driverSpec, executorSpec *k8stypes.PodSpec, err error) {
	if driverURI == "" && executorURI == "" {
		return nil, nil, nil
	}

	type result struct {
		spec *k8stypes.PodSpec
		err  error
	}
	driverCh := make(chan result, 1)
	execCh := make(chan result, 1)

	var wg sync.WaitGroup

	fetch := func(uri string, ch chan<- result) {
		defer wg.Done()
		b, ferr := l.fetcher.Fetch(ctx, uri)
		if ferr != nil {
			ch <- result{err: fmt.Errorf("template fetch %q: %w", uri, ferr)}
			return
		}
		if l.maxBytes > 0 && int64(len(b)) > l.maxBytes {
			ch <- result{err: fmt.Errorf("template %q exceeds max bytes (%d > %d)", uri, len(b), l.maxBytes)}
			return
		}
		spec, perr := unmarshalTemplateBytes(b)
		ch <- result{spec: spec, err: perr}
	}

	if driverURI != "" {
		wg.Add(1)
		go fetch(driverURI, driverCh)
	} else {
		driverCh <- result{}
	}

	if executorURI != "" {
		wg.Add(1)
		go fetch(executorURI, execCh)
	} else {
		execCh <- result{}
	}

	wg.Wait()

	dr := <-driverCh
	er := <-execCh

	if dr.err != nil {
		return nil, nil, dr.err
	}
	if er.err != nil {
		return nil, nil, er.err
	}
	return dr.spec, er.spec, nil
}

// unmarshalTemplateBytes parses YAML pod-template bytes into a PodSpec.
// Accepts the full-Pod form (apiVersion/kind/metadata/spec) only — that is
// what DPC's PodTemplateBuilder produces. Bare PodSpec form (containers: at root)
// is rejected with a clear error.
//
// Two-step pipeline: YAML → map[string]any → JSON → k8stypes.PodSpec.
// This avoids adding YAML struct tags to k8stypes (which uses json tags).
// Trade-off: YAML anchors, merge keys, and tagged scalars are not preserved.
func unmarshalTemplateBytes(b []byte) (*k8stypes.PodSpec, error) {
	var raw map[string]any
	if err := yaml.Unmarshal(b, &raw); err != nil {
		return nil, fmt.Errorf("yaml parse: %w", err)
	}
	specRaw, hasPodSpec := raw["spec"]
	if !hasPodSpec {
		return nil, fmt.Errorf("pod template must be a full Pod (with spec: key); bare PodSpec not supported")
	}
	b2, err := json.Marshal(specRaw)
	if err != nil {
		return nil, fmt.Errorf("marshal spec: %w", err)
	}
	var spec k8stypes.PodSpec
	if err := json.Unmarshal(b2, &spec); err != nil {
		return nil, fmt.Errorf("unmarshal PodSpec: %w", err)
	}
	return &spec, nil
}
