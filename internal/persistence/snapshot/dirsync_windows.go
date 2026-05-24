//go:build windows

package snapshot

// dirSync is a no-op on Windows: directory fsyncs are not supported.
func dirSync(dir string) error {
	return nil
}
