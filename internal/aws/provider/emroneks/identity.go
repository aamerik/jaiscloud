package emroneks

import (
	"context"
	"fmt"
	"strings"
	"unicode"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"jaiscloud/internal/k8shelpers"
)

const irsaRoleArnAnnotation = "eks.amazonaws.com/role-arn"

// buildIRSAMutator returns a k8shelpers.IdentityMutator that:
//  1. Ensures the ServiceAccount exists in the target namespace (creates if missing).
//  2. Patches it with eks.amazonaws.com/role-arn=<executionRoleArn> — the annotation
//     that the real AWS Pod Identity Webhook reads from the SA, not the pod.
//  3. Sets spec.serviceAccountName on the driver pod template (caller-wins).
//  4. Injects JAISCLOUD_EMRONEKS_EXECUTION_ROLE_ARN env var for in-container tooling.
func buildIRSAMutator(namespace, serviceAccount, executionRoleArn string) k8shelpers.IdentityMutator {
	return func(ctx context.Context, k8s kubernetes.Interface, tpl *corev1.PodTemplateSpec) error {
		if serviceAccount == "" && executionRoleArn == "" {
			return nil
		}

		// Caller-wins: if the pod template already names an SA, trust it.
		if tpl.Spec.ServiceAccountName == "" && serviceAccount != "" {
			tpl.Spec.ServiceAccountName = serviceAccount
		}

		saName := tpl.Spec.ServiceAccountName
		if saName == "" {
			saName = serviceAccount
		}
		if saName == "" {
			// Nothing to bind against — pod runs under the namespace default SA.
			return nil
		}

		if executionRoleArn != "" {
			if err := ensureServiceAccountAnnotated(ctx, k8s, namespace, saName, executionRoleArn); err != nil {
				return fmt.Errorf("emroneks: IRSA SA setup failed: %w", err)
			}
			if len(tpl.Spec.Containers) > 0 {
				tpl.Spec.Containers[0].Env = append(tpl.Spec.Containers[0].Env, corev1.EnvVar{
					Name:  "JAISCLOUD_EMRONEKS_EXECUTION_ROLE_ARN",
					Value: executionRoleArn,
				})
			}
		}
		return nil
	}
}

// ensureServiceAccountAnnotated creates the SA if missing then annotates it
// with the IRSA role ARN. Idempotent: if the SA already carries any role-arn
// annotation the existing value is preserved (caller-wins).
func ensureServiceAccountAnnotated(ctx context.Context, k8s kubernetes.Interface,
	namespace, saName, roleArn string) error {

	sas := k8s.CoreV1().ServiceAccounts(namespace)

	// Loop instead of recursing to avoid unbounded stack depth under concurrent
	// SA creation races (Create returns AlreadyExists when two callers race).
	const maxRetries = 3
	for range maxRetries {
		sa, err := sas.Get(ctx, saName, metav1.GetOptions{})
		if k8serrors.IsNotFound(err) {
			_, createErr := sas.Create(ctx, &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{
					Name:      saName,
					Namespace: namespace,
					Annotations: map[string]string{
						irsaRoleArnAnnotation: roleArn,
					},
				},
			}, metav1.CreateOptions{})
			if k8serrors.IsAlreadyExists(createErr) {
				// Another caller created it concurrently — retry the Get.
				continue
			}
			return createErr
		}
		if err != nil {
			return err
		}

		// Caller-wins: if the SA already has a role-arn annotation, leave it alone.
		if _, already := sa.Annotations[irsaRoleArnAnnotation]; already {
			return nil
		}

		if sa.Annotations == nil {
			sa.Annotations = map[string]string{}
		}
		sa.Annotations[irsaRoleArnAnnotation] = roleArn
		_, err = sas.Update(ctx, sa, metav1.UpdateOptions{})
		if err == nil || !k8serrors.IsConflict(err) {
			return err
		}
		// Conflict on Update (resource version changed) — retry.
	}
	return fmt.Errorf("emroneks: ensureServiceAccountAnnotated: too many retries for SA %s/%s", namespace, saName)
}

// sanitizeSAName converts a virtual-cluster name into a valid Kubernetes
// ServiceAccount name (lowercase, alphanumeric + hyphens, max 63 chars).
func sanitizeSAName(name string) string {
	var sb strings.Builder
	for _, r := range strings.ToLower(name) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			sb.WriteRune(r)
		} else {
			sb.WriteRune('-')
		}
	}
	s := sb.String()
	s = strings.Trim(s, "-")
	for strings.Contains(s, "--") {
		s = strings.ReplaceAll(s, "--", "-")
	}
	if len(s) > 63 {
		s = strings.TrimRight(s[:63], "-")
	}
	if s == "" {
		s = "default"
	}
	return s
}
