package emr

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"

	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/k8stypes"
)

// fakeFetcher is a simple in-memory BlobFetcher for tests.
type fakeFetcher struct {
	objects map[string][]byte
	err     error // returned for every fetch if non-nil
}

func (f *fakeFetcher) Fetch(_ context.Context, uri string) ([]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	b, ok := f.objects[uri]
	if !ok {
		return nil, fmt.Errorf("not found: %s", uri)
	}
	return b, nil
}

func testFetcher(scripts map[string]string) *fakeFetcher {
	f := &fakeFetcher{objects: make(map[string][]byte)}
	for uri, content := range scripts {
		f.objects[uri] = []byte(content)
	}
	return f
}

func defaultCfg() BootstrapConfig {
	return BootstrapConfig{
		Image:    "amazon/aws-cli:2.18",
		MaxBytes: 1024 * 1024,
		Prefixes: []string{"/etc/pki", "/home/hadoop"},
	}
}

var testEnv = []k8stypes.EnvVar{
	{Name: "AWS_DEFAULT_REGION", Value: "us-east-1"},
}

func TestResolve_EmptyActions(t *testing.T) {
	inits, vols, mounts, err := Resolve(context.Background(), testFetcher(nil), defaultCfg(), nil, testEnv)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if inits != nil || vols != nil || mounts != nil {
		t.Errorf("expected all nil for empty actions; got inits=%v vols=%v mounts=%v", inits, vols, mounts)
	}
}

func TestResolve_TwoActions_InitContainerShape(t *testing.T) {
	fetcher := testFetcher(map[string]string{
		"s3://bucket/a.sh": "#!/bin/sh\necho a",
		"s3://bucket/b.sh": "#!/bin/sh\necho b",
	})
	actions := []BootstrapAction{
		{Name: "stage-certs", S3Path: "s3://bucket/a.sh", Args: []string{"/etc/pki/ca.crt"}},
		{Name: "init-hadoop", S3Path: "s3://bucket/b.sh"},
	}

	inits, _, _, err := Resolve(context.Background(), fetcher, defaultCfg(), actions, testEnv)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(inits) != 2 {
		t.Fatalf("expected 2 init containers; got %d", len(inits))
	}
	if inits[0].Name != "bootstrap-stage-certs" {
		t.Errorf("unexpected name: %q", inits[0].Name)
	}
	if inits[0].Image != "amazon/aws-cli:2.18" {
		t.Errorf("unexpected image: %q", inits[0].Image)
	}
	if len(inits[0].Command) != 2 || inits[0].Command[0] != "/bin/sh" {
		t.Errorf("unexpected command: %v", inits[0].Command)
	}
}

func TestResolve_InitContainer_RunAsUserZero(t *testing.T) {
	fetcher := testFetcher(map[string]string{"s3://b/s.sh": "echo hi"})
	actions := []BootstrapAction{{Name: "test", S3Path: "s3://b/s.sh"}}

	inits, _, _, err := Resolve(context.Background(), fetcher, defaultCfg(), actions, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sc := inits[0].SecurityContext
	if sc == nil || sc.RunAsUser == nil || *sc.RunAsUser != 0 {
		t.Errorf("expected runAsUser=0; got %v", sc)
	}
}

func TestResolve_VolumeMounts_PerPrefix(t *testing.T) {
	fetcher := testFetcher(map[string]string{"s3://b/s.sh": "echo hi"})
	actions := []BootstrapAction{{Name: "test", S3Path: "s3://b/s.sh"}}

	inits, _, _, err := Resolve(context.Background(), fetcher, defaultCfg(), actions, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(inits[0].VolumeMounts) != 2 {
		t.Fatalf("expected 2 mounts per init container; got %d", len(inits[0].VolumeMounts))
	}
	paths := map[string]bool{}
	for _, m := range inits[0].VolumeMounts {
		paths[m.MountPath] = true
	}
	if !paths["/etc/pki"] || !paths["/home/hadoop"] {
		t.Errorf("expected mounts at /etc/pki and /home/hadoop; got %v", inits[0].VolumeMounts)
	}
}

func TestResolve_MainMounts_AllPrefixes(t *testing.T) {
	fetcher := testFetcher(map[string]string{"s3://b/s.sh": "echo hi"})
	actions := []BootstrapAction{{Name: "test", S3Path: "s3://b/s.sh"}}

	_, _, mounts, err := Resolve(context.Background(), fetcher, defaultCfg(), actions, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mounts) != 2 {
		t.Fatalf("expected 2 main mounts; got %d", len(mounts))
	}
}

func TestResolve_VolumeCount_MatchesPrefixes(t *testing.T) {
	fetcher := testFetcher(map[string]string{"s3://b/s.sh": "echo hi"})
	actions := []BootstrapAction{{Name: "test", S3Path: "s3://b/s.sh"}}

	_, vols, _, err := Resolve(context.Background(), fetcher, defaultCfg(), actions, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(vols) != 2 {
		t.Fatalf("expected 2 volumes (one per prefix); got %d", len(vols))
	}
}

func TestResolve_EmptyDirVolumeNaming(t *testing.T) {
	fetcher := testFetcher(map[string]string{"s3://b/s.sh": "echo hi"})
	actions := []BootstrapAction{{Name: "test", S3Path: "s3://b/s.sh"}}

	_, vols, _, err := Resolve(context.Background(), fetcher, defaultCfg(), actions, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	names := map[string]bool{}
	for _, v := range vols {
		names[v.Name] = true
	}
	if !names["bootstrap-prefix-etc-pki"] {
		t.Errorf("expected volume 'bootstrap-prefix-etc-pki'; got %v", vols)
	}
	if !names["bootstrap-prefix-home-hadoop"] {
		t.Errorf("expected volume 'bootstrap-prefix-home-hadoop'; got %v", vols)
	}
}

func TestResolve_Env_AWSVarsInjected(t *testing.T) {
	fetcher := testFetcher(map[string]string{"s3://b/s.sh": "echo hi"})
	actions := []BootstrapAction{{Name: "test", S3Path: "s3://b/s.sh"}}
	env := []k8stypes.EnvVar{
		{Name: "AWS_DEFAULT_REGION", Value: "us-east-1"},
		{Name: "AWS_ACCESS_KEY_ID", Value: "fake"},
	}

	inits, _, _, err := Resolve(context.Background(), fetcher, defaultCfg(), actions, env)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	envMap := map[string]string{}
	for _, e := range inits[0].Env {
		envMap[e.Name] = e.Value
	}
	if envMap["AWS_DEFAULT_REGION"] != "us-east-1" {
		t.Errorf("AWS_DEFAULT_REGION not injected: %v", inits[0].Env)
	}
	if envMap["AWS_ACCESS_KEY_ID"] != "fake" {
		t.Errorf("AWS_ACCESS_KEY_ID not injected: %v", inits[0].Env)
	}
}

func TestResolve_ScriptBase64Encoded(t *testing.T) {
	script := "#!/bin/sh\necho hello"
	fetcher := testFetcher(map[string]string{"s3://b/s.sh": script})
	actions := []BootstrapAction{{Name: "test", S3Path: "s3://b/s.sh"}}

	inits, _, _, err := Resolve(context.Background(), fetcher, defaultCfg(), actions, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// The arg should contain a base64-encoded version of the (possibly patched) script.
	arg := inits[0].Args[0]
	// Extract the base64 portion between the quotes.
	start := strings.Index(arg, `"`) + 1
	end := strings.LastIndex(arg, `"`)
	if start <= 0 || end <= start {
		t.Fatalf("could not find base64 in arg: %q", arg)
	}
	encoded := arg[start:end]
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("arg is not valid base64: %v", err)
	}
	if !strings.Contains(string(decoded), "echo hello") {
		t.Errorf("decoded script missing content: %q", decoded)
	}
}

func TestResolve_HostCommandScrubbing_Yum(t *testing.T) {
	script := "#!/bin/sh\nyum install -y curl\necho done"
	fetcher := testFetcher(map[string]string{"s3://b/s.sh": script})
	actions := []BootstrapAction{{Name: "test", S3Path: "s3://b/s.sh"}}

	inits, _, _, err := Resolve(context.Background(), fetcher, defaultCfg(), actions, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	arg := inits[0].Args[0]
	start := strings.Index(arg, `"`) + 1
	end := strings.LastIndex(arg, `"`)
	decoded, _ := base64.StdEncoding.DecodeString(arg[start:end])
	if !strings.Contains(string(decoded), "# [jaiscloud-skip]") {
		t.Errorf("yum line not commented out in: %q", decoded)
	}
	if !strings.Contains(string(decoded), "echo done") {
		t.Errorf("other lines should be preserved: %q", decoded)
	}
}

func TestResolve_HostCommandScrubbing_AptGet(t *testing.T) {
	script := "apt-get install curl"
	fetcher := testFetcher(map[string]string{"s3://b/s.sh": script})
	actions := []BootstrapAction{{Name: "test", S3Path: "s3://b/s.sh"}}
	inits, _, _, err := Resolve(context.Background(), fetcher, defaultCfg(), actions, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	arg := inits[0].Args[0]
	start := strings.Index(arg, `"`) + 1
	end := strings.LastIndex(arg, `"`)
	decoded, _ := base64.StdEncoding.DecodeString(arg[start:end])
	if !strings.Contains(string(decoded), "# [jaiscloud-skip]") {
		t.Errorf("apt-get line not scrubbed: %q", decoded)
	}
}

func TestResolve_HostCommandScrubbing_Systemctl(t *testing.T) {
	script := "systemctl restart nginx"
	fetcher := testFetcher(map[string]string{"s3://b/s.sh": script})
	actions := []BootstrapAction{{Name: "test", S3Path: "s3://b/s.sh"}}
	inits, _, _, err := Resolve(context.Background(), fetcher, defaultCfg(), actions, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	arg := inits[0].Args[0]
	start := strings.Index(arg, `"`) + 1
	end := strings.LastIndex(arg, `"`)
	decoded, _ := base64.StdEncoding.DecodeString(arg[start:end])
	if !strings.Contains(string(decoded), "# [jaiscloud-skip]") {
		t.Errorf("systemctl line not scrubbed: %q", decoded)
	}
}

func TestResolve_HostCommandScrubbing_CleanLine(t *testing.T) {
	script := "aws s3 cp s3://bucket/ca.crt /etc/pki/ca.crt"
	fetcher := testFetcher(map[string]string{"s3://b/s.sh": script})
	actions := []BootstrapAction{{Name: "test", S3Path: "s3://b/s.sh"}}
	inits, _, _, err := Resolve(context.Background(), fetcher, defaultCfg(), actions, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	arg := inits[0].Args[0]
	start := strings.Index(arg, `"`) + 1
	end := strings.LastIndex(arg, `"`)
	decoded, _ := base64.StdEncoding.DecodeString(arg[start:end])
	if strings.Contains(string(decoded), "# [jaiscloud-skip]") {
		t.Errorf("aws command should NOT be scrubbed: %q", decoded)
	}
}

func TestResolve_ScriptExceedsMaxBytes(t *testing.T) {
	fetcher := testFetcher(map[string]string{"s3://b/big.sh": strings.Repeat("x", 100)})
	actions := []BootstrapAction{{Name: "test", S3Path: "s3://b/big.sh"}}
	cfg := BootstrapConfig{Image: "img", MaxBytes: 10, Prefixes: []string{"/tmp"}}

	_, _, _, err := Resolve(context.Background(), fetcher, cfg, actions, nil)
	if err == nil {
		t.Fatal("expected error for oversized script")
	}
}

func TestResolve_FetcherError(t *testing.T) {
	f := &fakeFetcher{err: fmt.Errorf("network error")}
	actions := []BootstrapAction{{Name: "test", S3Path: "s3://b/s.sh"}}
	_, _, _, err := Resolve(context.Background(), f, defaultCfg(), actions, nil)
	if err == nil {
		t.Fatal("expected error when fetcher fails")
	}
}

func TestResolve_ActionNameSanitized(t *testing.T) {
	fetcher := testFetcher(map[string]string{"s3://b/s.sh": "echo hi"})
	actions := []BootstrapAction{{Name: "stage/certs here", S3Path: "s3://b/s.sh"}}
	inits, _, _, err := Resolve(context.Background(), fetcher, defaultCfg(), actions, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	name := inits[0].Name
	if strings.Contains(name, "/") || strings.Contains(name, " ") {
		t.Errorf("container name not sanitized: %q", name)
	}
	if !strings.HasPrefix(name, "bootstrap-") {
		t.Errorf("container name should start with 'bootstrap-': %q", name)
	}
}

func TestResolve_ActionArgs_InShellCmd(t *testing.T) {
	fetcher := testFetcher(map[string]string{"s3://b/s.sh": "#!/bin/sh\necho $1"})
	actions := []BootstrapAction{{Name: "test", S3Path: "s3://b/s.sh", Args: []string{"/etc/pki/kafka.jks"}}}
	inits, _, _, err := Resolve(context.Background(), fetcher, defaultCfg(), actions, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	arg := inits[0].Args[0]
	if !strings.Contains(arg, "/etc/pki/kafka.jks") {
		t.Errorf("action arg not passed to shell cmd: %q", arg)
	}
}

func TestS3BlobFetcher_Integration(t *testing.T) {
	store := blobfs.NewMemoryBlobStore()
	store.Put(context.Background(), "bootstrap-bucket", "scripts/stage.sh", []byte("#!/bin/sh\necho ok"))
	fetcher := blobfs.NewS3BlobFetcher(store)

	data, err := fetcher.Fetch(context.Background(), "s3://bootstrap-bucket/scripts/stage.sh")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(string(data), "echo ok") {
		t.Errorf("unexpected data: %q", data)
	}
}
