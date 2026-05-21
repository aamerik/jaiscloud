package emr

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"strings"
)

// s3LogUploader is the minimal interface the EMR provider needs to upload
// step logs to S3.  Implemented by *objectprovider.ObjectProvider; defined
// here to avoid an import cycle.
type s3LogUploader interface {
	InternalPutObject(ctx context.Context, bucket, key, contentType string, body []byte) error
}

// SetObjectProvider wires an S3-capable object provider into the EMR provider
// so that step logs are uploaded to LogUri on step completion.
func (p *EMRProvider) SetObjectProvider(op s3LogUploader) {
	p.objectProvider = op
}

// logBuffer is a bytes.Buffer wrapped as an io.Writer that also captures
// all written data for later S3 upload.
type logBuffer struct {
	buf bytes.Buffer
}

func (lb *logBuffer) Write(b []byte) (int, error) {
	return lb.buf.Write(b)
}

// LogSinkForStep returns an io.Writer that captures log output for the given
// step.  On completion, call flushStepLogs to upload captured bytes to S3.
func (p *EMRProvider) LogSinkForStep(clusterID, stepID, _ string) io.Writer {
	return &logBuffer{}
}

// flushStepLogs uploads captured step log bytes to S3 under LogUri if set.
// Real EMR on EC2 ships step logs to S3 only - there is no CW Logs ingestion.
func (p *EMRProvider) flushStepLogs(ctx context.Context, clusterID, stepID string, sink io.Writer) {
	lb, ok := sink.(*logBuffer)
	if !ok || lb.buf.Len() == 0 {
		return
	}
	if p.objectProvider == nil {
		return
	}
	c, err := p.loadCluster(ctx, "", "", clusterID)
	if err != nil || c.LogUri == "" {
		return
	}
	bucket, prefix, err := parseS3URI(c.LogUri)
	if err != nil {
		slog.Warn("emr: flushStepLogs: invalid LogUri", "logUri", c.LogUri, "err", err)
		return
	}
	gz, err := gzipBytes(lb.buf.Bytes())
	if err != nil {
		slog.Warn("emr: flushStepLogs: gzip failed", "err", err)
		return
	}
	key := fmt.Sprintf("%s%s/steps/%s/stdout.gz", prefix, clusterID, stepID)
	if err := p.objectProvider.InternalPutObject(ctx, bucket, key, "application/gzip", gz); err != nil {
		slog.Warn("emr: flushStepLogs: S3 upload failed", "bucket", bucket, "key", key, "err", err)
		return
	}
	slog.Debug("emr: step logs uploaded to S3", "bucket", bucket, "key", key, "bytes", len(gz))
}

// parseS3URI parses "s3://bucket/prefix/" into (bucket, "prefix/", nil).
func parseS3URI(uri string) (bucket, prefix string, err error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", "", err
	}
	if u.Scheme != "s3" {
		return "", "", fmt.Errorf("not an s3:// URI: %s", uri)
	}
	bucket = u.Host
	prefix = strings.TrimPrefix(u.Path, "/")
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return bucket, prefix, nil
}

// gzipBytes compresses data using gzip and returns the compressed bytes.
func gzipBytes(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
