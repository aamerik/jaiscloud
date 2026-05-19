//go:build darwin

package snapshot

import (
	"os"

	"golang.org/x/sys/unix"
)

// dirSync flushes the directory entry after a rename on macOS.
// os.File.Sync() on a directory fd is a no-op on most macOS filesystems;
// F_FULLFSYNC is required for equivalent durability.
func dirSync(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	_, err = unix.FcntlInt(d.Fd(), unix.F_FULLFSYNC, 0)
	return err
}
