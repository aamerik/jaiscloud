package config

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// ResolveStateDir picks a writable state directory using the following priority:
//  1. explicit (JAISCLOUD_STATE_DIR env var, if operator set it)
//  2. $HOME/.jaiscloud
//  3. /var/lib/jaiscloud  (container / system install)
//  4. /tmp/jaiscloud-<uid> (CI fallback — ephemeral, logs WARN)
func ResolveStateDir(explicit string) (string, error) {
	candidates := []struct {
		path      string
		ephemeral bool
	}{}
	if explicit != "" {
		candidates = append(candidates, struct {
			path      string
			ephemeral bool
		}{explicit, false})
	}
	if home, err := os.UserHomeDir(); err == nil {
		candidates = append(candidates, struct {
			path      string
			ephemeral bool
		}{filepath.Join(home, ".jaiscloud"), false})
	}
	candidates = append(candidates,
		struct {
			path      string
			ephemeral bool
		}{"/var/lib/jaiscloud", false},
		struct {
			path      string
			ephemeral bool
		}{filepath.Join("/tmp", "jaiscloud-"+strconv.Itoa(os.Getuid())), true},
	)

	for _, c := range candidates {
		if err := os.MkdirAll(c.path, 0o700); err != nil {
			continue
		}
		// Verify writable.
		probe := filepath.Join(c.path, ".probe")
		if err := os.WriteFile(probe, []byte("x"), 0o600); err != nil {
			continue
		}
		os.Remove(probe)
		if c.ephemeral {
			slog.Warn("jaiscloud: state directory is ephemeral — instance-ID will not persist across restarts",
				"state_dir", c.path,
				"hint", "set JAISCLOUD_STATE_DIR to a persistent path")
		}
		return c.path, nil
	}
	return "", fmt.Errorf("config: no writable state directory found")
}

// LoadOrCreateInstanceID returns (id, source) where source is one of:
// "env" (JAISCLOUD_INSTANCE_ID), "file" (<stateDir>/instance-id), "generated".
// When stateDir is empty or not writable, a UUID is generated but not persisted;
// in that case source is "generated" and a WARN is logged.
func LoadOrCreateInstanceID(stateDir string) (id, source string) {
	// 1. Env var override.
	if v := os.Getenv("JAISCLOUD_INSTANCE_ID"); v != "" {
		return strings.TrimSpace(v), "env"
	}

	// 2. Persistent file.
	if stateDir != "" {
		idFile := filepath.Join(stateDir, "instance-id")
		if data, err := os.ReadFile(idFile); err == nil {
			if s := strings.TrimSpace(string(data)); s != "" {
				return s, "file"
			}
		}
		// Generate and persist.
		uid, err := newUUIDv4()
		if err == nil {
			if writeErr := os.WriteFile(idFile, []byte(uid+"\n"), 0o600); writeErr == nil {
				return uid, "file"
			}
		}
	}

	// 3. Ephemeral fallback.
	uid, err := newUUIDv4()
	if err != nil {
		uid = "unknown-instance"
	}
	slog.Warn("jaiscloud: instance-ID not persisted — multi-instance K8s safety is degraded",
		"id", uid,
		"hint", "set JAISCLOUD_STATE_DIR to a persistent path so the ID survives restarts")
	return uid, "generated"
}

// WriteInstanceID atomically persists the given instance ID to <stateDir>/instance-id.
func WriteInstanceID(stateDir, id string) error {
	if stateDir == "" {
		return errors.New("config: state dir empty")
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return fmt.Errorf("config: mkdir %s: %w", stateDir, err)
	}
	path := filepath.Join(stateDir, "instance-id")
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("config: open instance-id tmp: %w", err)
	}
	if _, err := fmt.Fprintln(f, id); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("config: write instance-id: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("config: fsync instance-id: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("config: close instance-id: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("config: rename instance-id: %w", err)
	}
	return nil
}

// GenerateNewInstanceID forces a fresh UUID to <stateDir>/instance-id,
// overwriting any existing value. Used by admin import --new-instance.
func GenerateNewInstanceID(stateDir string) (string, error) {
	id, err := newUUIDv4()
	if err != nil {
		return "", err
	}
	return id, WriteInstanceID(stateDir, id)
}

// newUUIDv4 returns an RFC 4122 version-4 UUID using crypto/rand.
func newUUIDv4() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("uuid: read rand: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10xx
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(b[0:4]),
		hex.EncodeToString(b[4:6]),
		hex.EncodeToString(b[6:8]),
		hex.EncodeToString(b[8:10]),
		hex.EncodeToString(b[10:16])), nil
}
