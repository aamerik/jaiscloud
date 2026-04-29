package platform

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

// PlatformConfig holds runtime configuration applied to every JaisCloud-managed
// workload. Populated once at startup via LoadFromEnv and injected by pointer
// into executor constructors — never embedded inside workload-specific config.
type PlatformConfig struct {
	TLS               TLSConfig
	Volumes           []VolumeSpec
	Env               map[string]string
	HostPathAllowlist []string // allowed hostPath prefixes; empty = hostPath disabled

	// bundle caches the Docker-mode PEM bundle; scoped here so different
	// PlatformConfig instances (e.g. in tests) get independent caches.
	bundle pemBundleCache
}

// pemBundleCache holds the result of materialising CA sources to a host-side
// PEM file. It is written exactly once per PlatformConfig via sync.Once.
type pemBundleCache struct {
	once sync.Once
	path string
	err  error
}

// LoadFromEnv reads and validates platform configuration from environment
// variables (and optional _FILE variants). Returns a ready-to-use *PlatformConfig.
// Errors are fatal — callers should exit with "platform config: <err>".
func LoadFromEnv() (*PlatformConfig, error) {
	cfg := &PlatformConfig{
		TLS: TLSConfig{
			Enabled:            true,
			TruststorePassword: "changeit",
		},
		Env: map[string]string{},
	}

	// ── TLS enabled flag ─────────────────────────────────────────────────────
	if v := os.Getenv("JAISCLOUD_PLATFORM_TLS_ENABLED"); v == "false" {
		cfg.TLS.Enabled = false
	}

	// ── CA sources ───────────────────────────────────────────────────────────
	caSources, err := loadSlice[CASource]("JAISCLOUD_PLATFORM_TLS_CA_SOURCES")
	if err != nil {
		return nil, err
	}
	if len(caSources) == 0 {
		// Default: the JaisCloud CA ConfigMap shipped with the Helm chart.
		caSources = []CASource{{
			Name:   "jaiscloud",
			Source: CASourceRef{Kind: "configMap", Name: "jaiscloud-ca-cert", Key: "ca.crt"},
		}}
	}
	cfg.TLS.CASources = caSources

	// ── Client cert (optional) ───────────────────────────────────────────────
	clientCert, err := loadSingle[CASource]("JAISCLOUD_PLATFORM_TLS_CLIENT_CERT")
	if err != nil {
		return nil, err
	}
	cfg.TLS.ClientCert = clientCert

	// ── Truststore password ──────────────────────────────────────────────────
	if v := os.Getenv("JAISCLOUD_PLATFORM_TLS_PASSWORD"); v != "" {
		cfg.TLS.TruststorePassword = v
	}

	// ── Volumes ──────────────────────────────────────────────────────────────
	rawVolumes, err := loadSlice[rawVolumeSpec]("JAISCLOUD_PLATFORM_VOLUMES")
	if err != nil {
		return nil, err
	}
	cfg.Volumes, err = canonicaliseVolumes(rawVolumes)
	if err != nil {
		return nil, err
	}

	// ── Extra env ────────────────────────────────────────────────────────────
	envMap, err := loadSingle[map[string]string]("JAISCLOUD_PLATFORM_ENV")
	if err != nil {
		return nil, err
	}
	if envMap != nil {
		cfg.Env = *envMap
	}

	// ── HostPath allowlist ───────────────────────────────────────────────────
	if v := os.Getenv("JAISCLOUD_PLATFORM_HOSTPATH_ALLOWLIST"); v != "" {
		for _, p := range strings.Split(v, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				cfg.HostPathAllowlist = append(cfg.HostPathAllowlist, p)
			}
		}
	}

	if err := validate(cfg); err != nil {
		return nil, err
	}

	logStartup(cfg)
	return cfg, nil
}

// ── validation ─────────────────────────────────────────────────────────────

func validate(cfg *PlatformConfig) error {
	// CA source alias uniqueness.
	seen := map[string]struct{}{}
	for _, ca := range cfg.TLS.CASources {
		if _, dup := seen[ca.Name]; dup {
			return fmt.Errorf("CA source alias %q duplicated", ca.Name)
		}
		seen[ca.Name] = struct{}{}
		if err := validateCASourceRef(ca.Source); err != nil {
			return fmt.Errorf("CA source %q: %w", ca.Name, err)
		}
	}

	// Volume name uniqueness + kind validation.
	vnames := map[string]struct{}{}
	for _, vs := range cfg.Volumes {
		if _, dup := vnames[vs.Name]; dup {
			return fmt.Errorf("volume %q name duplicated", vs.Name)
		}
		vnames[vs.Name] = struct{}{}
		if err := validateVolumeSource(vs.Source, cfg.HostPathAllowlist); err != nil {
			return fmt.Errorf("volume %q: %w", vs.Name, err)
		}
	}

	// Warn when TLS disabled but sources configured.
	if !cfg.TLS.Enabled && len(cfg.TLS.CASources) > 0 {
		slog.Warn("platform tls: Enabled=false but CA sources configured — CA imports will be skipped",
			"ca-sources", len(cfg.TLS.CASources))
	}

	return nil
}

func validateCASourceRef(ref CASourceRef) error {
	if ref.Kind != "configMap" && ref.Kind != "secret" {
		return fmt.Errorf("source kind %q must be configMap or secret", ref.Kind)
	}
	if ref.Name == "" {
		return fmt.Errorf("source name is required")
	}
	if ref.Key == "" {
		return fmt.Errorf("source key is required")
	}
	return nil
}

func validateVolumeSource(src VolumeSource, allowlist []string) error {
	validKinds := map[string]bool{
		"secret": true, "configMap": true, "emptyDir": true,
		"projected": true, "pvc": true, "csi": true, "hostPath": true,
	}
	if !validKinds[src.Kind] {
		return fmt.Errorf("unknown volume kind %q", src.Kind)
	}
	if src.Kind == "hostPath" {
		if src.HostPath == nil {
			return fmt.Errorf("hostPath source missing")
		}
		if !hostPathAllowed(src.HostPath.Path, allowlist) {
			prefixes := strings.Join(allowlist, ", ")
			if prefixes == "" {
				prefixes = "(none)"
			}
			return fmt.Errorf("hostPath %q not in HOSTPATH_ALLOWLIST [%s]", src.HostPath.Path, prefixes)
		}
	}
	return nil
}

func hostPathAllowed(path string, allowlist []string) bool {
	for _, prefix := range allowlist {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

// ── startup log ────────────────────────────────────────────────────────────

func logStartup(cfg *PlatformConfig) {
	aliases := make([]string, len(cfg.TLS.CASources))
	for i, ca := range cfg.TLS.CASources {
		aliases[i] = ca.Name
	}
	clientCert := cfg.TLS.ClientCert != nil
	slog.Info("platform tls",
		"enabled", cfg.TLS.Enabled,
		"materializers", []string{"jvm-truststore", "pem-bundle"},
		"ca-sources", aliases,
		"client-cert", clientCert,
	)
}

// ── short-form canonicalisation ────────────────────────────────────────────

// rawVolumeSpec is the union of short-form and long-form volume JSON shapes.
type rawVolumeSpec struct {
	// Long form
	Name   string      `json:"name"   yaml:"name"`
	Source *rawSource  `json:"source" yaml:"source"`
	Mounts []MountSpec `json:"mounts" yaml:"mounts"`

	// Short-form convenience fields
	ConfigMap string `json:"configMap" yaml:"configMap"`
	Secret    string `json:"secret"    yaml:"secret"`
	PVC       string `json:"pvc"       yaml:"pvc"`
	EmptyDir  bool   `json:"emptyDir"  yaml:"emptyDir"`
	MountPath string `json:"mountPath" yaml:"mountPath"`
	ReadOnly  bool   `json:"readOnly"  yaml:"readOnly"`
}

type rawSource struct {
	Kind      string           `json:"kind"      yaml:"kind"`
	Name      string           `json:"name"      yaml:"name"`
	Secret    *SecretSource    `json:"secret"    yaml:"secret"`
	ConfigMap *ConfigMapSource `json:"configMap" yaml:"configMap"`
	EmptyDir  *EmptyDirSource  `json:"emptyDir"  yaml:"emptyDir"`
	Projected *ProjectedSource `json:"projected" yaml:"projected"`
	PVC       *PVCSource       `json:"pvc"       yaml:"pvc"`
	CSI       *CSISource       `json:"csi"       yaml:"csi"`
	HostPath  *HostPathSource  `json:"hostPath"  yaml:"hostPath"`
}

func canonicaliseVolumes(raws []rawVolumeSpec) ([]VolumeSpec, error) {
	out := make([]VolumeSpec, 0, len(raws))
	for _, r := range raws {
		vs, err := canonicalise(r)
		if err != nil {
			return nil, fmt.Errorf("volume %q: %w", r.Name, err)
		}
		out = append(out, vs)
	}
	return out, nil
}

func canonicalise(r rawVolumeSpec) (VolumeSpec, error) {
	if r.Name == "" {
		return VolumeSpec{}, fmt.Errorf("volume has no name")
	}

	// Long form — source field present.
	if r.Source != nil {
		src := VolumeSource{
			Kind:      r.Source.Kind,
			Secret:    r.Source.Secret,
			ConfigMap: r.Source.ConfigMap,
			EmptyDir:  r.Source.EmptyDir,
			Projected: r.Source.Projected,
			PVC:       r.Source.PVC,
			CSI:       r.Source.CSI,
			HostPath:  r.Source.HostPath,
		}
		// Populate sub-fields from the flat source fields if kind-specific pointer is nil.
		if src.Kind == "configMap" && src.ConfigMap == nil && r.Source.Name != "" {
			src.ConfigMap = &ConfigMapSource{Name: r.Source.Name}
		}
		if src.Kind == "secret" && src.Secret == nil && r.Source.Name != "" {
			src.Secret = &SecretSource{Name: r.Source.Name}
		}
		if src.Kind == "pvc" && src.PVC == nil && r.Source.Name != "" {
			src.PVC = &PVCSource{ClaimName: r.Source.Name}
		}
		mounts := r.Mounts
		if len(mounts) == 0 && r.MountPath != "" {
			mounts = []MountSpec{{MountPath: r.MountPath, ReadOnly: r.ReadOnly}}
		}
		return VolumeSpec{Name: r.Name, Source: src, Mounts: mounts}, nil
	}

	// Short form — derive source from convenience fields.
	var src VolumeSource
	switch {
	case r.ConfigMap != "":
		src = VolumeSource{Kind: "configMap", ConfigMap: &ConfigMapSource{Name: r.ConfigMap}}
	case r.Secret != "":
		src = VolumeSource{Kind: "secret", Secret: &SecretSource{Name: r.Secret}}
	case r.PVC != "":
		src = VolumeSource{Kind: "pvc", PVC: &PVCSource{ClaimName: r.PVC}}
	case r.EmptyDir:
		src = VolumeSource{Kind: "emptyDir", EmptyDir: &EmptyDirSource{}}
	default:
		return VolumeSpec{}, fmt.Errorf("has no source field and no recognised short-form key (configMap/secret/pvc/emptyDir)")
	}

	if r.MountPath == "" {
		return VolumeSpec{}, fmt.Errorf("short-form volume requires mountPath")
	}
	mounts := []MountSpec{{MountPath: r.MountPath, ReadOnly: r.ReadOnly}}
	return VolumeSpec{Name: r.Name, Source: src, Mounts: mounts}, nil
}

// ── generic env-var loader helpers ─────────────────────────────────────────

// loadSlice reads a JSON/YAML array from <envKey> or <envKey>_FILE.
func loadSlice[T any](envKey string) ([]T, error) {
	raw, err := readEnvOrFile(envKey)
	if err != nil || raw == "" {
		return nil, err
	}
	var out []T
	if err := unmarshalAuto(raw, &out); err != nil {
		return nil, fmt.Errorf("parse %s: %w", envKey, err)
	}
	return out, nil
}

// loadSingle reads a JSON/YAML object from <envKey> or <envKey>_FILE.
// Returns nil (no error) when the env var is unset.
func loadSingle[T any](envKey string) (*T, error) {
	raw, err := readEnvOrFile(envKey)
	if err != nil || raw == "" {
		return nil, err
	}
	var out T
	if err := unmarshalAuto(raw, &out); err != nil {
		return nil, fmt.Errorf("parse %s: %w", envKey, err)
	}
	return &out, nil
}

// readEnvOrFile returns the raw string content for envKey, preferring _FILE.
func readEnvOrFile(envKey string) (string, error) {
	fileKey := envKey + "_FILE"
	filePath := os.Getenv(fileKey)
	jsonVal := os.Getenv(envKey)

	if filePath != "" && jsonVal != "" {
		slog.Warn("platform config: both env and _FILE set; using file",
			"env", envKey, "file", filePath)
	}
	if filePath != "" {
		data, err := os.ReadFile(filePath)
		if err != nil {
			return "", fmt.Errorf("read %s=%s: %w", fileKey, filePath, err)
		}
		return string(data), nil
	}
	return jsonVal, nil
}

// unmarshalAuto detects JSON vs YAML by file extension or content prefix.
func unmarshalAuto(raw string, out any) error {
	trimmed := strings.TrimSpace(raw)
	if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
		return json.Unmarshal([]byte(trimmed), out)
	}
	return yaml.Unmarshal([]byte(trimmed), out)
}
