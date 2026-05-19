package snapshot

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
)

// WriteAtomic writes data to dst atomically: write to a .tmp file, sync, then rename.
// Any stale .tmp from a prior crash is removed before writing.
// On error, the .tmp file is cleaned up; the original dst is unchanged.
func WriteAtomic(dst string, data []byte) error {
	tmp := dst + ".tmp"
	_ = os.Remove(tmp)

	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		os.Remove(tmp)
		return err
	}
	return dirSync(filepath.Dir(dst))
}

// WriteAtomicTarGz writes a gzip-compressed tar archive to dst atomically.
// fn receives a *tar.Writer and is responsible for writing all entries.
// WriteAtomicTarGz handles compression and final sync/rename.
func WriteAtomicTarGz(dst string, fn func(*tar.Writer) error) error {
	tmp := dst + ".tmp"
	_ = os.Remove(tmp)

	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	cleanup := func() { f.Close(); os.Remove(tmp) }

	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)

	if err := fn(tw); err != nil {
		cleanup()
		return err
	}
	if err := tw.Close(); err != nil {
		cleanup()
		return err
	}
	if err := gz.Close(); err != nil {
		cleanup()
		return err
	}
	if err := f.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		os.Remove(tmp)
		return err
	}
	return dirSync(filepath.Dir(dst))
}

// readTarGz opens a gzip-compressed tar archive and passes a *tar.Reader to fn.
func ReadTarGz(path string, fn func(*tar.Reader) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gz.Close()
	return fn(tar.NewReader(gz))
}

// WriteTarEntry writes a single regular file entry into tw.
func WriteTarEntry(tw *tar.Writer, name string, data []byte) error {
	hdr := &tar.Header{
		Name:     name,
		Typeflag: tar.TypeReg,
		Size:     int64(len(data)),
		Mode:     0600,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	_, err := io.WriteString(tw, string(data))
	return err
}
