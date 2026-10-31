package dataproc

import (
	"encoding/json"
	"testing"

	dpstore "jaiscloud/internal/gcp/store/dataproc"
)

// gceCluster builds a stored GCE-backed cluster with the given stored config
// JSON ("" means the caller sent no config at all).
func gceCluster(config string) dpstore.Cluster {
	c := dpstore.Cluster{ProjectID: "proj", Region: "us-central1", Name: "c1", Status: dpstore.ClusterStatus{State: "RUNNING"}}
	if config != "" {
		c.Config = json.RawMessage(config)
	}
	return c
}

// renderedConfig renders c and returns its config object as JSON-decoded data,
// so assertions compare the wire value (float64 numbers) rather than whichever
// Go numeric type the renderer happened to write.
func renderedConfig(t *testing.T, c dpstore.Cluster) map[string]any {
	t.Helper()
	rendered := ClusterJSON(c)
	config, ok := rendered["config"].(map[string]any)
	if !ok {
		t.Fatalf("config missing from render: %v", rendered)
	}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal config: %v", err)
	}
	return decoded
}

// child returns data[key] as a decoded object.
func child(t *testing.T, data map[string]any, key string) map[string]any {
	t.Helper()
	m, ok := data[key].(map[string]any)
	if !ok {
		t.Fatalf("%s missing or not an object: %v", key, data)
	}
	return m
}

const (
	wantZoneLink    = "https://www.googleapis.com/compute/v1/projects/proj/zones/us-central1-a"
	wantMachineLink = "https://www.googleapis.com/compute/v1/projects/proj/zones/us-central1-a/machineTypes/n1-standard-4"
)

// TestClusterJSON_GCEConfigDefaultsMaterialised covers a cluster created with no
// config at all: real Dataproc reports the documented default configuration
// (1 master, 2 workers, n1-standard-4, a 500 GB boot disk, the default Debian
// image, a zone in the region) on every read, and the hashicorp/google provider
// nil-derefs config.softwareConfig when it is absent.
func TestClusterJSON_GCEConfigDefaultsMaterialised(t *testing.T) {
	config := renderedConfig(t, gceCluster(""))

	if sw := child(t, config, "softwareConfig"); sw["imageVersion"] != defaultGCEImageVersion {
		t.Fatalf("softwareConfig.imageVersion = %v, want %s", sw["imageVersion"], defaultGCEImageVersion)
	}
	if gcc := child(t, config, "gceClusterConfig"); gcc["zoneUri"] != wantZoneLink {
		t.Fatalf("gceClusterConfig.zoneUri = %v, want %s", gcc["zoneUri"], wantZoneLink)
	}

	for _, tc := range []struct {
		field string
		count float64
	}{{"masterConfig", 1}, {"workerConfig", 2}} {
		group := child(t, config, tc.field)
		if group["numInstances"] != tc.count {
			t.Fatalf("%s.numInstances = %v, want %v", tc.field, group["numInstances"], tc.count)
		}
		if group["machineTypeUri"] != wantMachineLink {
			t.Fatalf("%s.machineTypeUri = %v, want %s", tc.field, group["machineTypeUri"], wantMachineLink)
		}
		disk := child(t, group, "diskConfig")
		if disk["bootDiskSizeGb"] != float64(500) {
			t.Fatalf("%s.diskConfig.bootDiskSizeGb = %v, want 500", tc.field, disk["bootDiskSizeGb"])
		}
		if _, ok := disk["numLocalSsds"]; ok {
			t.Fatalf("%s.diskConfig must not render the zero-valued numLocalSsds: %v", tc.field, disk)
		}
	}
}

// TestClusterJSON_GCEConfigDefaultsDoNotOverride verifies a caller's own values
// survive: the read defaults only fill what is missing.
func TestClusterJSON_GCEConfigDefaultsDoNotOverride(t *testing.T) {
	config := renderedConfig(t, gceCluster(`{
		"gceClusterConfig":{"zoneUri":"projects/proj/zones/us-central1-f","internalIpOnly":true},
		"softwareConfig":{"imageVersion":"2.2-debian12","properties":{"spark:spark.executor.memory":"4g"}},
		"masterConfig":{"numInstances":1,"machineTypeUri":"n1-highmem-8","diskConfig":{"bootDiskSizeGb":100}},
		"workerConfig":{"numInstances":5},
		"configBucket":"my-bucket"
	}`))

	gcc := child(t, config, "gceClusterConfig")
	if gcc["zoneUri"] != "projects/proj/zones/us-central1-f" {
		t.Fatalf("caller zoneUri rewritten: %v", gcc["zoneUri"])
	}
	if gcc["internalIpOnly"] != true {
		t.Fatalf("caller field lost: %v", gcc)
	}
	sw := child(t, config, "softwareConfig")
	if sw["imageVersion"] != "2.2-debian12" {
		t.Fatalf("caller imageVersion overwritten: %v", sw["imageVersion"])
	}
	if props := child(t, sw, "properties"); props["spark:spark.executor.memory"] != "4g" {
		t.Fatalf("caller properties lost: %v", props)
	}
	master := child(t, config, "masterConfig")
	if master["machineTypeUri"] != "n1-highmem-8" || master["numInstances"] != float64(1) {
		t.Fatalf("caller masterConfig overwritten: %v", master)
	}
	if disk := child(t, master, "diskConfig"); disk["bootDiskSizeGb"] != float64(100) {
		t.Fatalf("caller diskConfig overwritten: %v", disk)
	}

	// A supplied group's missing fields are filled from the caller's own zone,
	// not from the region default.
	worker := child(t, config, "workerConfig")
	if worker["numInstances"] != float64(5) {
		t.Fatalf("caller worker count overwritten: %v", worker["numInstances"])
	}
	if want := "https://www.googleapis.com/compute/v1/projects/proj/zones/us-central1-f/machineTypes/n1-standard-4"; worker["machineTypeUri"] != want {
		t.Fatalf("worker machineTypeUri = %v, want %s", worker["machineTypeUri"], want)
	}
	if disk := child(t, worker, "diskConfig"); disk["bootDiskSizeGb"] != float64(500) {
		t.Fatalf("worker diskConfig not defaulted: %v", disk)
	}
	if config["configBucket"] != "my-bucket" {
		t.Fatalf("caller configBucket lost: %v", config)
	}
}

// TestClusterJSON_GCEConfigFillsOnlyMissingFields covers the partial-config
// shapes: an empty or null gceClusterConfig still gets its defaults, while an
// unrelated nested config (metastoreConfig) is left untouched.
func TestClusterJSON_GCEConfigFillsOnlyMissingFields(t *testing.T) {
	for _, stored := range []string{
		`{"gceClusterConfig":{},"metastoreConfig":{"dataprocMetastoreService":"projects/proj/locations/us-central1/services/hms"}}`,
		`{"gceClusterConfig":null,"metastoreConfig":{"dataprocMetastoreService":"projects/proj/locations/us-central1/services/hms"}}`,
		`{"softwareConfig":null}`,
		`{}`,
	} {
		rendered := renderedConfig(t, gceCluster(stored))
		if gcc := child(t, rendered, "gceClusterConfig"); gcc["zoneUri"] != wantZoneLink {
			t.Fatalf("%s: zoneUri = %v, want %s", stored, gcc["zoneUri"], wantZoneLink)
		}
		if sw := child(t, rendered, "softwareConfig"); sw["imageVersion"] != defaultGCEImageVersion {
			t.Fatalf("%s: imageVersion = %v, want %s", stored, sw["imageVersion"], defaultGCEImageVersion)
		}
		if meta, ok := rendered["metastoreConfig"].(map[string]any); ok {
			if meta["dataprocMetastoreService"] == nil {
				t.Fatalf("%s: metastoreConfig lost: %v", stored, meta)
			}
		}
	}
}

// TestClusterJSON_DefaultImageVersionAddedToPartialSoftwareConfig covers a
// softwareConfig that carries only properties: the image version is still
// reported, because the provider reads it unconditionally.
func TestClusterJSON_DefaultImageVersionAddedToPartialSoftwareConfig(t *testing.T) {
	config := renderedConfig(t, gceCluster(`{"softwareConfig":{"properties":{"spark:x":"y"}}}`))
	sw := child(t, config, "softwareConfig")
	if sw["imageVersion"] != defaultGCEImageVersion {
		t.Fatalf("imageVersion not defaulted: %v", sw["imageVersion"])
	}
	if props := child(t, sw, "properties"); props["spark:x"] != "y" {
		t.Fatalf("properties lost: %v", props)
	}
}

// TestClusterJSON_MalformedConfigIsNotReplaced verifies a stored config that
// does not parse is omitted rather than replaced by an invented default cluster
// (the value is corrupt for some other reason; rendering a synthetic cluster
// would hide it).
func TestClusterJSON_MalformedConfigIsNotReplaced(t *testing.T) {
	c := gceCluster("")
	c.Config = json.RawMessage(`{"gceClusterConfig":`)
	if _, ok := ClusterJSON(c)["config"]; ok {
		t.Fatal("an unparseable stored config must not be rendered as a default cluster")
	}
}

// TestClusterJSON_NonObjectChildIsNotClobbered pins the tolerance for a
// malformed nested field: defaults are dropped for that key, never overwritten.
func TestClusterJSON_NonObjectChildIsNotClobbered(t *testing.T) {
	config := renderedConfig(t, gceCluster(`{"softwareConfig":"nope"}`))
	if config["softwareConfig"] != "nope" {
		t.Fatalf("non-object softwareConfig rewritten: %v", config["softwareConfig"])
	}
	if gcc := child(t, config, "gceClusterConfig"); gcc["zoneUri"] != wantZoneLink {
		t.Fatalf("other defaults must still apply: %v", gcc)
	}
	// A non-object diskConfig inside a group is likewise left alone, while an
	// explicit null (protojson's "unset") is materialised.
	master := child(t, renderedConfig(t, gceCluster(`{"masterConfig":{"diskConfig":"nope"}}`)), "masterConfig")
	if master["diskConfig"] != "nope" {
		t.Fatalf("non-object diskConfig rewritten: %v", master["diskConfig"])
	}
	nullMaster := child(t, renderedConfig(t, gceCluster(`{"masterConfig":{"diskConfig":null}}`)), "masterConfig")
	if disk := child(t, nullMaster, "diskConfig"); disk["bootDiskSizeGb"] != float64(500) {
		t.Fatalf("null diskConfig not materialised: %v", nullMaster["diskConfig"])
	}
}

// TestClusterJSON_GKEClusterKeepsNoGCEConfig guards the other half of the
// contract: defaulting must never invent a config for a virtual cluster.
func TestClusterJSON_GKEClusterKeepsNoGCEConfig(t *testing.T) {
	c := dpstore.Cluster{
		ProjectID: "proj",
		Region:    "us-central1",
		Name:      "gke-1",
		VirtualClusterConfig: json.RawMessage(`{"stagingBucket":"dataproc-staging-proj","kubernetesClusterConfig":{
			"kubernetesNamespace":"dataproc",
			"gkeClusterConfig":{"gkeClusterTarget":"projects/proj/locations/us-central1/clusters/gke-1"},
			"kubernetesSoftwareConfig":{"componentVersion":{"SPARK":"3.5"}}}}`),
	}
	if _, ok := ClusterJSON(c)["config"]; ok {
		t.Fatal("config must stay absent for a GKE-backed cluster")
	}
}

// TestClusterJSON_DoesNotMutateStoredConfig verifies the defaults are render-time
// only: two renders agree and the stored bytes are untouched (memory and
// Postgres then behave identically, and a later update masks against the
// caller's own config, not the defaults).
func TestClusterJSON_DoesNotMutateStoredConfig(t *testing.T) {
	c := gceCluster(`{"gceClusterConfig":{"zoneUri":"us-central1-b"}}`)
	before := string(c.Config)
	first := ClusterJSON(c)
	second := ClusterJSON(c)
	if string(c.Config) != before {
		t.Fatalf("stored config mutated: %s -> %s", before, c.Config)
	}
	firstJSON, err := json.Marshal(first)
	if err != nil {
		t.Fatalf("marshal first render: %v", err)
	}
	secondJSON, err := json.Marshal(second)
	if err != nil {
		t.Fatalf("marshal second render: %v", err)
	}
	if string(firstJSON) != string(secondJSON) {
		t.Fatalf("two renders disagree:\n%s\n%s", firstJSON, secondJSON)
	}
	sw := child(t, child(t, first, "config"), "softwareConfig")
	if sw["imageVersion"] != defaultGCEImageVersion {
		t.Fatalf("softwareConfig.imageVersion not defaulted: %v", sw)
	}
}

// TestDefaultZone pins the zone fallback used when the caller names no zone.
func TestDefaultZone(t *testing.T) {
	if got := defaultZone("europe-west4"); got != "europe-west4-a" {
		t.Fatalf("defaultZone(%q) = %q", "europe-west4", got)
	}
	if got := defaultZone(""); got != "us-central1-a" {
		t.Fatalf("defaultZone(\"\") = %q", got)
	}
}

// TestZoneFromURI covers the three documented zoneUri spellings plus the
// degenerate forms a malformed URI can end in (they must fall back, not leak a
// path segment into machineTypeUri).
func TestZoneFromURI(t *testing.T) {
	for _, tc := range []struct{ uri, want string }{
		{"https://www.googleapis.com/compute/v1/projects/proj/zones/us-east1-b", "us-east1-b"},
		{"projects/proj/zones/us-east1-b", "us-east1-b"},
		{"us-east1-b", "us-east1-b"},
		{"projects/proj/zones/us-east1-b/", "us-east1-b"},
		{"", ""},
		{"projects/proj/zones/", ""},
		{"projects/proj/zones", ""},
		{"projects/proj/regions/us-central1", ""},
		{"us-central1", ""},
	} {
		if got := zoneFromURI(tc.uri); got != tc.want {
			t.Fatalf("zoneFromURI(%q) = %q, want %q", tc.uri, got, tc.want)
		}
	}
}
