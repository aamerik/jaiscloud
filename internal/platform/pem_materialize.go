package platform

import (
	"fmt"
	"os"
)

// materializePEMBundle writes all CA sources available as local files into a
// single PEM bundle on the host filesystem. The result is cached on cfg so
// each PlatformConfig instance gets its own independent cache (allowing tests
// and multiple server instances to use different CA sets without interference).
// Returns ("", nil) when no file-kind CA sources are present — Docker TLS is
// then a no-op.
func (cfg *PlatformConfig) materializePEMBundle() (string, error) {
	cfg.bundle.once.Do(func() {
		cfg.bundle.path, cfg.bundle.err = writePEMBundle(cfg.TLS.CASources)
	})
	return cfg.bundle.path, cfg.bundle.err
}

func writePEMBundle(sources []CASource) (string, error) {
	f, err := os.CreateTemp("", "jaiscloud-ca-bundle-*.pem")
	if err != nil {
		return "", fmt.Errorf("platform: create PEM bundle temp file: %w", err)
	}
	defer f.Close()

	wrote := 0
	for _, ca := range sources {
		// Only local file sources are readable by the host process.
		// K8s ConfigMap/Secret sources are skipped; they are handled inside
		// the pod via the pem-bundle init container.
		if ca.Source.Kind != "file" {
			continue
		}
		data, err := os.ReadFile(ca.Source.Key)
		if err != nil {
			return "", fmt.Errorf("platform: read CA file %s: %w", ca.Source.Key, err)
		}
		if _, err := f.Write(data); err != nil {
			return "", fmt.Errorf("platform: write PEM bundle: %w", err)
		}
		wrote++
	}

	if wrote == 0 {
		// No local CA files — return empty path; Docker TLS will be a no-op.
		os.Remove(f.Name())
		return "", nil
	}
	return f.Name(), nil
}
