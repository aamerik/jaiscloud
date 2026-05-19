package admin

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// SnapshotMetadata holds metadata about a named snapshot stored on disk.
type SnapshotMetadata struct {
	Name             string         `json:"name"`
	Description      string         `json:"description"`
	CreatedAt        time.Time      `json:"created_at"`
	JaisCloudVersion string         `json:"jaiscloud_version"`
	SchemaVersion    int            `json:"schema_version"`
	Cloud            string         `json:"cloud"`
	SizeBytes        int64          `json:"size_bytes"`
	StoreCounts      map[string]int `json:"store_counts"`
}

// snapshotDir returns the path to the snapshot directory for the given name.
func snapshotDir(dataDir, name string) string {
	return filepath.Join(dataDir, "snapshots", name)
}

// snapshotTarball returns the path to the snapshot tarball for the given name.
func snapshotTarball(dataDir, name string) string {
	return filepath.Join(snapshotDir(dataDir, name), "snapshot.tar.gz")
}

// snapshotMetaFile returns the path to the snapshot metadata file for the given name.
func snapshotMetaFile(dataDir, name string) string {
	return filepath.Join(snapshotDir(dataDir, name), "metadata.json")
}

// writeSnapshotMeta writes metadata.json to the snapshot directory.
func writeSnapshotMeta(dataDir, name string, meta SnapshotMetadata) error {
	dir := snapshotDir(dataDir, name)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}
	return os.WriteFile(snapshotMetaFile(dataDir, name), data, 0o600)
}

// readSnapshotMeta reads and parses metadata.json from the snapshot directory.
func readSnapshotMeta(dataDir, name string) (SnapshotMetadata, error) {
	data, err := os.ReadFile(snapshotMetaFile(dataDir, name))
	if err != nil {
		return SnapshotMetadata{}, fmt.Errorf("read metadata.json: %w", err)
	}
	var meta SnapshotMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return SnapshotMetadata{}, fmt.Errorf("parse metadata.json: %w", err)
	}
	return meta, nil
}

// listSnapshots reads all snapshot directories and returns metadata sorted
// newest-first. Directories without metadata.json are silently skipped.
func listSnapshots(dataDir string) ([]SnapshotMetadata, error) {
	dir := filepath.Join(dataDir, "snapshots")
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read snapshots dir: %w", err)
	}
	var metas []SnapshotMetadata
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		meta, err := readSnapshotMeta(dataDir, e.Name())
		if err != nil {
			continue // skip snapshots without valid metadata
		}
		metas = append(metas, meta)
	}
	// Sort newest-first.
	sort.Slice(metas, func(i, j int) bool {
		return metas[i].CreatedAt.After(metas[j].CreatedAt)
	})
	return metas, nil
}
