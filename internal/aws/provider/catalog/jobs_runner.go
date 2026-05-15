// Package catalog — Glue Jobs runner hook for real Spark executor wiring.
// In mock mode (executor == "" or "mock"), StartJobRun immediately marks the run
// as SUCCEEDED (see jobs.go).  When a real executor is injected via
// SetSparkExecutor, StartJobRun will delegate to the Spark executor machinery.
// That wiring is a follow-up; only mock mode is required for the Phase F.1 exit
// criteria.
package catalog

import "sync"

// SparkExecutorAPI is the minimal interface the Glue Jobs runner requires from a
// Spark executor.  The concrete implementation lives in internal/aws/provider/emr.
// It is defined here as an interface to keep the catalog package free of direct
// EMR imports.
type SparkExecutorAPI interface {
	// SubmitSparkJob submits a Spark job and returns a run identifier.
	SubmitSparkJob(scriptLocation string, args []string) (string, error)
}

var sparkExecutorMu sync.RWMutex

// SetSparkExecutor injects an optional Spark executor.  When non-nil, real
// executor modes (docker / k8s) can delegate Glue job runs to the Spark
// infrastructure reused from the EMR provider.
func (p *GlueProvider) SetSparkExecutor(exec SparkExecutorAPI) {
	sparkExecutorMu.Lock()
	defer sparkExecutorMu.Unlock()
	p.sparkExecutor = exec
}

func (p *GlueProvider) getSparkExecutor() SparkExecutorAPI {
	sparkExecutorMu.RLock()
	defer sparkExecutorMu.RUnlock()
	return p.sparkExecutor
}
