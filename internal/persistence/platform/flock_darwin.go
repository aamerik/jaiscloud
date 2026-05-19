//go:build darwin

package platform

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// ErrDataDirLocked is returned by AcquireDataDirLock when another process
// already holds the lock on the data directory.
var ErrDataDirLocked = errors.New("data directory is locked by another process; is another jaiscloud instance running?")

// AcquireDataDirLock acquires an OS-level flock on <dataDir>/.lock.
// Returns a release function (which unlocks and closes the file) and nil on
// success. Returns ErrDataDirLocked if the lock is already held by another
// process.
func AcquireDataDirLock(dataDir string) (release func() error, err error) {
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, fmt.Errorf("data dir: %w", err)
	}
	lockPath := filepath.Join(dataDir, ".lock")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, ErrDataDirLocked
		}
		return nil, fmt.Errorf("flock: %w", err)
	}
	release = func() error {
		_ = unix.Flock(int(f.Fd()), unix.LOCK_UN)
		return f.Close()
	}
	return release, nil
}
