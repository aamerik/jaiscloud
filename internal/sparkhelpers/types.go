package sparkhelpers

import (
	"io"

	"jaiscloud/internal/k8shelpers"
	"jaiscloud/internal/platform"
)

// EntryPoint is a sealed interface for Spark job entry points.
type EntryPoint interface{ isEntryPoint() }

// JarEntryPoint describes a JAR-based Spark job.
type JarEntryPoint struct {
	JarURI    string
	MainClass string
}

func (JarEntryPoint) isEntryPoint() {}

// PythonEntryPoint describes a Python-based Spark job.
type PythonEntryPoint struct {
	MainPythonFile string
	PyFiles        []string
}

func (PythonEntryPoint) isEntryPoint() {}

// REntryPoint describes an R-based Spark job.
type REntryPoint struct {
	MainRFile string
}

func (REntryPoint) isEntryPoint() {}

// ResourceProfile holds CPU/memory/count for a driver or executor.
type ResourceProfile struct {
	CPU    string
	Memory string
	Count  int
}

// ClientModeJob is the input to SubmitClientMode.
type ClientModeJob struct {
	JobID             string
	Namespace         string
	EntryPoint        EntryPoint
	SparkSubmitArgs   []string
	JarArgs           []string
	DriverResources   ResourceProfile
	ExecutorResources ResourceProfile
	PlatformOverlay   *platform.PlatformConfig
	// CallerDriverPodTpl is a YAML PodTemplateSpec merged onto the spark-submit pod.
	CallerDriverPodTpl []byte
	// CallerExecutorPodTpl is a YAML PodTemplateSpec merged into the executor pod template ConfigMap.
	CallerExecutorPodTpl []byte
	Labels              map[string]string
	Annotations         map[string]string
	OwnerHint           *k8shelpers.OwnerRefHint
	LogSink             io.Writer
	IdentityMutator     k8shelpers.IdentityMutator
	TTLSecondsAfterFinished *int32
	// SparkSubmitPath overrides the spark-submit binary path (default: "spark-submit").
	SparkSubmitPath string
}

// Final is the terminal result of a Spark client-mode job.
// Embeds k8shelpers.Final (pod-level) and adds Spark-level classification.
type Final struct {
	k8shelpers.Final
	SparkSucceeded bool
	SparkReason    string
}
