// Package platform resolves the data directory for the running binary.
package platform

import (
	"os"
	"path/filepath"
)

// HostDetector abstracts host/container detection for testing.
type HostDetector interface {
	IsContainer() bool
	WorkingDir() string
	HomeDir() (string, bool)
}

// ResolveDataDir returns the absolute data directory path and the source of the resolution:
// "flag" | "env" | "home" | "container" | "fallback".
//
// Priority:
//  1. flag — explicit --data-dir value
//  2. env  — JAISCLOUD_DATA_DIR
//  3. container default — <cwd>/.jaiscloud/<binaryName>/
//  4. host default — <home>/.jaiscloud/<binaryName>/
//  5. fallback — <cwd>/.jaiscloud/<binaryName>/ (same as container)
//
// The result path never starts with /tmp (that is reserved for memory-mode session dirs).
func ResolveDataDir(flag, env, binaryName string, det HostDetector) (path, source string) {
	if flag != "" {
		return absolutise(flag), "flag"
	}
	if env != "" {
		return absolutise(env), "env"
	}
	if det.IsContainer() {
		return absolutise(filepath.Join(det.WorkingDir(), ".jaiscloud", binaryName)), "container"
	}
	if home, ok := det.HomeDir(); ok {
		return filepath.Join(home, ".jaiscloud", binaryName), "home"
	}
	return absolutise(filepath.Join(det.WorkingDir(), ".jaiscloud", binaryName)), "fallback"
}

func absolutise(p string) string {
	if filepath.IsAbs(p) {
		return p
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return p
	}
	return abs
}

// DefaultHostDetector uses os package functions to detect environment.
type DefaultHostDetector struct{}

// IsContainer returns true when the process appears to be running inside a container.
// Detection: $HOME is unset, unwritable, or equals "/" (distroless WORKDIR).
func (d DefaultHostDetector) IsContainer() bool {
	home := os.Getenv("HOME")
	if home == "" || home == "/" {
		return true
	}
	// Writability probe: try to stat $HOME.
	info, err := os.Stat(home)
	if err != nil || !info.IsDir() {
		return true
	}
	// Try opening a file to test writability.
	probe := filepath.Join(home, ".jaiscloud_probe_tmp")
	f, err := os.OpenFile(probe, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
	if err != nil {
		return true
	}
	f.Close()
	os.Remove(probe)
	return false
}

func (d DefaultHostDetector) WorkingDir() string {
	cwd, err := os.Getwd()
	if err != nil {
		return "."
	}
	return cwd
}

func (d DefaultHostDetector) HomeDir() (string, bool) {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return "", false
	}
	return home, true
}
