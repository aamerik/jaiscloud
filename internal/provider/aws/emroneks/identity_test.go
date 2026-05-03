package emroneks

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestBuildIRSAMutator_CreatesServiceAccountWithAnnotation(t *testing.T) {
	k8s := fake.NewSimpleClientset()
	m := buildIRSAMutator("jaiscloud", "my-sa", "arn:aws:iam::000000000000:role/execution")
	tpl := &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "spark-submit"}}},
	}
	require.NoError(t, m(context.Background(), k8s, tpl))

	sa, err := k8s.CoreV1().ServiceAccounts("jaiscloud").Get(context.Background(), "my-sa", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "arn:aws:iam::000000000000:role/execution", sa.Annotations[irsaRoleArnAnnotation])
	require.Equal(t, "my-sa", tpl.Spec.ServiceAccountName)

	require.Contains(t, tpl.Spec.Containers[0].Env, corev1.EnvVar{
		Name:  "JAISCLOUD_EMRONEKS_EXECUTION_ROLE_ARN",
		Value: "arn:aws:iam::000000000000:role/execution",
	})
}

func TestBuildIRSAMutator_PatchesExistingSA(t *testing.T) {
	k8s := fake.NewSimpleClientset(&corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "vc-sa", Namespace: "jaiscloud"},
	})
	m := buildIRSAMutator("jaiscloud", "vc-sa", "arn:aws:iam::000000000000:role/vc-role")
	tpl := &corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "spark-submit"}}}}
	require.NoError(t, m(context.Background(), k8s, tpl))

	sa, err := k8s.CoreV1().ServiceAccounts("jaiscloud").Get(context.Background(), "vc-sa", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "arn:aws:iam::000000000000:role/vc-role", sa.Annotations[irsaRoleArnAnnotation])
}

func TestBuildIRSAMutator_CallerWinsOnSAName(t *testing.T) {
	k8s := fake.NewSimpleClientset()
	m := buildIRSAMutator("jaiscloud", "vc-sa", "arn:aws:iam::000000000000:role/vc-role")
	tpl := &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			ServiceAccountName: "caller-sa",
			Containers:         []corev1.Container{{Name: "spark-submit"}},
		},
	}
	require.NoError(t, m(context.Background(), k8s, tpl))

	// SA name unchanged — caller-supplied wins.
	require.Equal(t, "caller-sa", tpl.Spec.ServiceAccountName)
	// The mutator annotates the caller's SA, not vc-sa.
	sa, err := k8s.CoreV1().ServiceAccounts("jaiscloud").Get(context.Background(), "caller-sa", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "arn:aws:iam::000000000000:role/vc-role", sa.Annotations[irsaRoleArnAnnotation])
}

func TestBuildIRSAMutator_CallerWinsOnExistingRoleArn(t *testing.T) {
	k8s := fake.NewSimpleClientset(&corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sa",
			Namespace: "jaiscloud",
			Annotations: map[string]string{
				irsaRoleArnAnnotation: "arn:aws:iam::000000000000:role/caller",
			},
		},
	})
	m := buildIRSAMutator("jaiscloud", "sa", "arn:aws:iam::000000000000:role/vc-role")
	tpl := &corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "spark-submit"}}}}
	require.NoError(t, m(context.Background(), k8s, tpl))

	sa, err := k8s.CoreV1().ServiceAccounts("jaiscloud").Get(context.Background(), "sa", metav1.GetOptions{})
	require.NoError(t, err)
	// Pre-existing annotation preserved — caller-wins.
	require.Equal(t, "arn:aws:iam::000000000000:role/caller", sa.Annotations[irsaRoleArnAnnotation])
}

func TestBuildIRSAMutator_EmptyRoleSkipsKubeCalls(t *testing.T) {
	k8s := fake.NewSimpleClientset()
	m := buildIRSAMutator("jaiscloud", "", "")
	tpl := &corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "spark-submit"}}}}
	require.NoError(t, m(context.Background(), k8s, tpl))
	require.Empty(t, tpl.Spec.ServiceAccountName)

	sas, _ := k8s.CoreV1().ServiceAccounts("jaiscloud").List(context.Background(), metav1.ListOptions{})
	require.Empty(t, sas.Items)
}

func TestSanitizeSAName(t *testing.T) {
	cases := []struct{ in, want string }{
		{"my-cluster", "my-cluster"},
		{"My Cluster!", "my-cluster"},
		{"---bad---", "bad"},
		{"", "default"},
	}
	for _, c := range cases {
		got := sanitizeSAName(c.in)
		if got != c.want {
			t.Errorf("sanitizeSAName(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
