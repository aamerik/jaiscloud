package emroneks

import "io"

// LogSinkForJobRun returns an io.Writer for job-run log output.
// Phase 2 will wire real S3 log upload; for now returns io.Discard.
func (p *EMRContainersProvider) LogSinkForJobRun(_, _, _ string) io.Writer {
	return io.Discard
}
