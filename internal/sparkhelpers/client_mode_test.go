package sparkhelpers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"jaiscloud/internal/k8shelpers"
	"jaiscloud/internal/platform"
)

// TestBuildClientModeArgs_JAR verifies argv ordering for a JAR entry point.
func TestBuildClientModeArgs_JAR(t *testing.T) {
	job := ClientModeJob{
		JobID:     "test-job-1",
		Namespace: "default",
		EntryPoint: JarEntryPoint{
			JarURI:    "s3://bucket/app.jar",
			MainClass: "com.example.Main",
		},
		SparkSubmitArgs: []string{"--conf", "spark.driver.memory=2g"},
		JarArgs:         []string{"--input", "s3://bucket/data"},
	}

	args := BuildClientModeArgs(job)

	// Verify mandatory leading args.
	require.GreaterOrEqual(t, len(args), 12)
	assert.Equal(t, "--master", args[0])
	assert.Equal(t, defaultMaster, args[1])
	assert.Equal(t, "--deploy-mode", args[2])
	assert.Equal(t, "client", args[3])
	assert.Equal(t, "--conf", args[4])
	assert.Equal(t, "spark.kubernetes.namespace=default", args[5])
	assert.Equal(t, "--conf", args[6])
	assert.Equal(t, "spark.kubernetes.driver.pod.name=$(SPARK_DRIVER_POD_NAME)", args[7])
	assert.Equal(t, "--conf", args[8])
	assert.Equal(t, "spark.kubernetes.executor.podTemplateFile=file:///jaiscloud/spark/executor-template.yaml", args[9])
	assert.Equal(t, "--conf", args[10])
	assert.Equal(t, "spark.driver.bindAddress=0.0.0.0", args[11])

	// Caller args.
	assert.Equal(t, "--conf", args[12])
	assert.Equal(t, "spark.driver.memory=2g", args[13])

	// Entry-point pre-args and jar.
	assert.Equal(t, "--class", args[14])
	assert.Equal(t, "com.example.Main", args[15])
	assert.Equal(t, "s3://bucket/app.jar", args[16])

	// Jar positional args.
	assert.Equal(t, "--input", args[17])
	assert.Equal(t, "s3://bucket/data", args[18])
}

// TestBuildClientModeArgs_Python verifies argv for a Python entry point.
func TestBuildClientModeArgs_Python(t *testing.T) {
	job := ClientModeJob{
		JobID:     "py-job",
		Namespace: "spark",
		EntryPoint: PythonEntryPoint{
			MainPythonFile: "app.py",
			PyFiles:        []string{"lib1.zip", "lib2.zip"},
		},
	}

	args := BuildClientModeArgs(job)

	// Find py-files in args.
	idx := -1
	for i, a := range args {
		if a == "--py-files" {
			idx = i
			break
		}
	}
	require.NotEqual(t, -1, idx, "expected --py-files in args")
	assert.Equal(t, "lib1.zip,lib2.zip", args[idx+1])
	assert.Equal(t, "app.py", args[len(args)-1])
}

// TestBuildClientModeArgs_R verifies argv for an R entry point.
func TestBuildClientModeArgs_R(t *testing.T) {
	job := ClientModeJob{
		JobID:     "r-job",
		Namespace: "spark",
		EntryPoint: REntryPoint{MainRFile: "analysis.R"},
	}

	args := BuildClientModeArgs(job)
	assert.Equal(t, "analysis.R", args[len(args)-1])
}

// TestSubmitClientMode verifies the Job and ConfigMap are created.
func TestSubmitClientMode(t *testing.T) {
	k8s := fake.NewSimpleClientset()
	ctx := context.Background()

	mutatorCalled := false
	var mutator k8shelpers.IdentityMutator = func(_ context.Context, _ kubernetes.Interface, tpl *corev1.PodTemplateSpec) error {
		mutatorCalled = true
		if tpl.Annotations == nil {
			tpl.Annotations = map[string]string{}
		}
		tpl.Annotations["test-mutator"] = "called"
		return nil
	}

	job := ClientModeJob{
		JobID:     "test-job-abc",
		Namespace: "jaiscloud",
		EntryPoint: JarEntryPoint{
			JarURI:    "local:///opt/spark/app.jar",
			MainClass: "com.test.Main",
		},
		Labels:          map[string]string{"app": "spark-test"},
		IdentityMutator: mutator,
	}

	handle, err := SubmitClientMode(ctx, k8s, job)
	require.NoError(t, err)
	assert.NotEmpty(t, handle.JobName)

	// Verify ConfigMap was created.
	cmName := "spark-exec-tpl-" + sanitizeJobID(job.JobID)
	cm, err := k8s.CoreV1().ConfigMaps(job.Namespace).Get(ctx, cmName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Contains(t, cm.Data, executorTemplateKey)

	// Verify Job was created.
	k8sJob, err := k8s.BatchV1().Jobs(job.Namespace).Get(ctx, handle.JobName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "spark-test", k8sJob.Labels["app"])

	// Verify IdentityMutator was called.
	assert.True(t, mutatorCalled)
}

// TestSubmitClientMode_WithPlatformOverlay verifies platform overlay is applied to the spark-submit pod.
func TestSubmitClientMode_WithPlatformOverlay(t *testing.T) {
	k8s := fake.NewSimpleClientset()
	ctx := context.Background()

	overlay := &platform.PlatformConfig{
		TLS: platform.TLSConfig{
			Enabled: true,
			CASources: []platform.CASource{
				{
					Name:   "testca",
					Source: platform.CASourceRef{Kind: "configMap", Name: "my-ca", Key: "ca.crt"},
				},
			},
			TruststorePassword: "changeit",
		},
	}

	job := ClientModeJob{
		JobID:           "overlay-job",
		Namespace:       "jaiscloud",
		EntryPoint:      JarEntryPoint{JarURI: "local:///app.jar"},
		PlatformOverlay: overlay,
	}

	handle, err := SubmitClientMode(ctx, k8s, job)
	require.NoError(t, err)
	assert.NotEmpty(t, handle.JobName)

	// Verify the spark-submit job's pod has init containers from platform overlay.
	k8sJob, err := k8s.BatchV1().Jobs(job.Namespace).Get(ctx, handle.JobName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.NotEmpty(t, k8sJob.Spec.Template.Spec.InitContainers,
		"expected platform overlay TLS init containers in spark-submit pod")
}

// TestSubmitClientMode_CustomSparkSubmitPath verifies custom spark-submit binary path.
func TestSubmitClientMode_CustomSparkSubmitPath(t *testing.T) {
	k8s := fake.NewSimpleClientset()
	ctx := context.Background()

	job := ClientModeJob{
		JobID:           "custom-path-job",
		Namespace:       "jaiscloud",
		EntryPoint:      JarEntryPoint{JarURI: "local:///app.jar"},
		SparkSubmitPath: "/opt/spark/bin/spark-submit",
	}

	handle, err := SubmitClientMode(ctx, k8s, job)
	require.NoError(t, err)

	k8sJob, err := k8s.BatchV1().Jobs(job.Namespace).Get(ctx, handle.JobName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Len(t, k8sJob.Spec.Template.Spec.Containers, 1)
	assert.Equal(t, []string{"/opt/spark/bin/spark-submit"}, k8sJob.Spec.Template.Spec.Containers[0].Command)
}

// TestSubmitClientMode_ExecutorConfigMapHasTemplate verifies executor template is in ConfigMap.
func TestSubmitClientMode_ExecutorConfigMapHasTemplate(t *testing.T) {
	k8s := fake.NewSimpleClientset()
	ctx := context.Background()

	callerExecTpl := []byte(`
spec:
  serviceAccountName: spark-executor-sa
`)

	job := ClientModeJob{
		JobID:                "exec-tpl-job",
		Namespace:            "jaiscloud",
		EntryPoint:           JarEntryPoint{JarURI: "local:///app.jar"},
		CallerExecutorPodTpl: callerExecTpl,
	}

	_, err := SubmitClientMode(ctx, k8s, job)
	require.NoError(t, err)

	cmName := "spark-exec-tpl-" + sanitizeJobID(job.JobID)
	cm, err := k8s.CoreV1().ConfigMaps(job.Namespace).Get(ctx, cmName, metav1.GetOptions{})
	require.NoError(t, err)

	tplData := cm.Data[executorTemplateKey]
	assert.Contains(t, tplData, "spark-executor-sa")
}
