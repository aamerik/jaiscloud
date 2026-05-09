package emr

import "io"

// LogSinkForStep returns an io.Writer that receives log output for the given step.
// Phase 2 will wire real S3 log upload; for now returns io.Discard.
func (p *EMRProvider) LogSinkForStep(_, _, _ string) io.Writer {
	return io.Discard
}
