package emroneks

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"jaiscloud/internal/logstream"
)

// s3LogUploader is the narrow interface used to upload gzip-compressed logs to S3.
type s3LogUploader interface {
	InternalPutObject(ctx context.Context, bucket, key, contentType string, body []byte) error
}

// SetObjectProvider wires an S3 object provider for log uploads.
func (p *EMRContainersProvider) SetObjectProvider(op s3LogUploader) {
	p.s3Uploader = op
}

// SetLogsIngestor wires a CloudWatch Logs ingestor for log forwarding.
func (p *EMRContainersProvider) SetLogsIngestor(i logstream.Ingestor) {
	p.logsIngestor = i
}

// bufferingWriter is an io.Writer that buffers log lines in memory so they can
// be flushed to CloudWatch Logs or S3 after the job completes.
type bufferingWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *bufferingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

// bytes returns a copy of the accumulated bytes.
func (w *bufferingWriter) bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	b := make([]byte, w.buf.Len())
	copy(b, w.buf.Bytes())
	return b
}

// LogSinkForJobRun returns a buffering io.Writer for job-run log capture.
// The returned writer accumulates output until flushJobRunLogs is called.
func (p *EMRContainersProvider) LogSinkForJobRun(virtualClusterID, jobRunID, _ string) io.Writer {
	w := &bufferingWriter{}
	return w
}

// flushJobRunLogs ships accumulated log output to every configured sink in monitoringConfig.
// It handles two sinks:
//   - cloudWatchMonitoringConfiguration → CloudWatch Logs via logstream.Ingestor
//   - s3MonitoringConfiguration         → gzip object upload via s3LogUploader
func (p *EMRContainersProvider) flushJobRunLogs(
	ctx context.Context,
	virtualClusterID, jobRunID string,
	monitoringConfig map[string]any,
	sink io.Writer,
) {
	if monitoringConfig == nil {
		return
	}

	bw, ok := sink.(*bufferingWriter)
	if !ok {
		return
	}
	data := bw.bytes()
	if len(data) == 0 {
		return
	}

	// ── CloudWatch sink ──────────────────────────────────────────────────────
	if cwCfg, ok := monitoringConfig["cloudWatchMonitoringConfiguration"].(map[string]any); ok && p.logsIngestor != nil {
		logGroupName := strParamFromMap(cwCfg, "logGroupName")
		logStreamPrefix := strParamFromMap(cwCfg, "logStreamNamePrefix")
		if logGroupName == "" {
			logGroupName = fmt.Sprintf("/emr-containers/jobs/%s", virtualClusterID)
		}
		logStreamName := fmt.Sprintf("%s%s", logStreamPrefix, jobRunID)

		if err := p.logsIngestor.InternalCreateLogGroup(ctx, logGroupName); err != nil {
			slog.Warn("emroneks: flushJobRunLogs: failed to create log group",
				"logGroupName", logGroupName, "err", err)
		}

		// Split raw bytes into log events (one per line).
		events := bytesToLogEvents(data)
		if err := p.logsIngestor.InternalPutEvents(ctx, logGroupName, logStreamName, events); err != nil {
			slog.Warn("emroneks: flushJobRunLogs: InternalPutEvents failed",
				"logGroupName", logGroupName, "logStreamName", logStreamName, "err", err)
		}
	}

	// ── S3 sink ──────────────────────────────────────────────────────────────
	if s3Cfg, ok := monitoringConfig["s3MonitoringConfiguration"].(map[string]any); ok && p.s3Uploader != nil {
		logDest := strParamFromMap(s3Cfg, "logUri") // e.g. s3://bucket/prefix
		bucket, prefix := parseS3URI(logDest)
		if bucket == "" {
			slog.Warn("emroneks: flushJobRunLogs: s3MonitoringConfiguration missing logUri")
		} else {
			key := fmt.Sprintf("%s%s/%s/stdout.gz", prefix, virtualClusterID, jobRunID)
			compressed, gzErr := gzipBytes(data)
			if gzErr != nil {
				slog.Warn("emroneks: flushJobRunLogs: gzip failed", "err", gzErr)
			} else if err := p.s3Uploader.InternalPutObject(ctx, bucket, key, "application/gzip", compressed); err != nil {
				slog.Warn("emroneks: flushJobRunLogs: InternalPutObject failed",
					"bucket", bucket, "key", key, "err", err)
			}
		}
	}
}

// bytesToLogEvents converts raw log bytes into logstream.Events, splitting on newlines.
func bytesToLogEvents(data []byte) []logstream.Event {
	now := time.Now().UnixMilli()
	lines := bytes.Split(data, []byte("\n"))
	events := make([]logstream.Event, 0, len(lines))
	for _, line := range lines {
		line = bytes.TrimRight(line, "\r")
		if len(line) == 0 {
			continue
		}
		events = append(events, logstream.Event{
			Timestamp: now,
			Message:   string(line),
		})
	}
	return events
}

// gzipBytes compresses data with gzip and returns the result.
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

// parseS3URI splits an s3://bucket/prefix URI. prefix may be empty.
func parseS3URI(uri string) (bucket, prefix string) {
	const scheme = "s3://"
	if len(uri) <= len(scheme) || uri[:len(scheme)] != scheme {
		return "", ""
	}
	rest := uri[len(scheme):]
	idx := -1
	for i, c := range rest {
		if c == '/' {
			idx = i
			break
		}
	}
	if idx < 0 {
		return rest, ""
	}
	return rest[:idx], rest[idx+1:]
}
