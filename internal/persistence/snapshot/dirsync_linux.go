//go:build linux

package snapshot

import "os"

// dirSync flushes the directory entry after a rename on Linux.
func dirSync(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
