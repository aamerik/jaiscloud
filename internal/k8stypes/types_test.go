package k8stypes

import (
	"encoding/json"
	"testing"
)

func boolPtr(b bool) *bool   { return &b }
func int64Ptr(i int64) *int64 { return &i }

func TestEnvVar_PlainValue_RoundTrip(t *testing.T) {
	e := EnvVar{Name: "FOO", Value: "bar"}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	var got EnvVar
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.Name != "FOO" || got.Value != "bar" || got.ValueFrom != nil {
		t.Errorf("plain value round-trip failed: %+v", got)
	}
}

func TestEnvVar_ValueFrom_SecretKeyRef(t *testing.T) {
	e := EnvVar{
		Name: "SECRET_VAL",
		ValueFrom: &EnvVarSource{
			SecretKeyRef: &SecretKeySelector{Name: "my-secret", Key: "token", Optional: boolPtr(false)},
		},
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	var got EnvVar
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.Value != "" {
		t.Errorf("Value should be empty when ValueFrom is set; got %q", got.Value)
	}
	if got.ValueFrom == nil || got.ValueFrom.SecretKeyRef == nil {
		t.Fatal("SecretKeyRef lost after round-trip")
	}
	ref := got.ValueFrom.SecretKeyRef
	if ref.Name != "my-secret" || ref.Key != "token" || ref.Optional == nil || *ref.Optional {
		t.Errorf("SecretKeyRef fields wrong: %+v", ref)
	}
}

func TestEnvVar_ValueFrom_ConfigMapKeyRef(t *testing.T) {
	e := EnvVar{
		Name: "CM_VAL",
		ValueFrom: &EnvVarSource{
			ConfigMapKeyRef: &ConfigMapKeySelector{Name: "my-cm", Key: "endpoint"},
		},
	}
	b, _ := json.Marshal(e)
	var got EnvVar
	json.Unmarshal(b, &got)
	if got.ValueFrom == nil || got.ValueFrom.ConfigMapKeyRef == nil {
		t.Fatal("ConfigMapKeyRef lost after round-trip")
	}
	ref := got.ValueFrom.ConfigMapKeyRef
	if ref.Name != "my-cm" || ref.Key != "endpoint" {
		t.Errorf("ConfigMapKeyRef fields wrong: %+v", ref)
	}
}

func TestEnvVar_ValueFrom_FieldRef(t *testing.T) {
	e := EnvVar{
		Name: "POD_NAME",
		ValueFrom: &EnvVarSource{
			FieldRef: &ObjectFieldSelector{FieldPath: "metadata.name"},
		},
	}
	b, _ := json.Marshal(e)
	var got EnvVar
	json.Unmarshal(b, &got)
	if got.ValueFrom == nil || got.ValueFrom.FieldRef == nil {
		t.Fatal("FieldRef lost after round-trip")
	}
	if got.ValueFrom.FieldRef.FieldPath != "metadata.name" {
		t.Errorf("FieldRef.FieldPath wrong: %q", got.ValueFrom.FieldRef.FieldPath)
	}
}

func TestPodSpec_SchedulingFields_RoundTrip(t *testing.T) {
	spec := PodSpec{
		NodeSelector: map[string]string{"zone": "us-east-1a"},
		Tolerations: []Toleration{{
			Key: "key1", Operator: "Equal", Value: "val1", Effect: "NoSchedule",
		}},
		TopologySpreadConstraints: []TopologySpreadConstraint{{
			MaxSkew: 1, TopologyKey: "zone", WhenUnsatisfiable: "DoNotSchedule",
		}},
		SecurityContext: &PodSecurityContext{
			RunAsUser: int64Ptr(1000),
			FSGroup:   int64Ptr(2000),
		},
	}
	b, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	var got PodSpec
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.NodeSelector["zone"] != "us-east-1a" {
		t.Errorf("NodeSelector lost: %v", got.NodeSelector)
	}
	if len(got.Tolerations) != 1 || got.Tolerations[0].Effect != "NoSchedule" {
		t.Errorf("Tolerations lost: %v", got.Tolerations)
	}
	if len(got.TopologySpreadConstraints) != 1 || got.TopologySpreadConstraints[0].MaxSkew != 1 {
		t.Errorf("TopologySpreadConstraints lost: %v", got.TopologySpreadConstraints)
	}
	if got.SecurityContext == nil || *got.SecurityContext.RunAsUser != 1000 {
		t.Errorf("SecurityContext lost: %v", got.SecurityContext)
	}
}

func TestContainerSecurityContext_RoundTrip(t *testing.T) {
	ctr := Container{
		Name:  "main",
		Image: "alpine",
		SecurityContext: &SecurityContext{
			RunAsUser:    int64Ptr(0),
			RunAsNonRoot: boolPtr(false),
		},
	}
	b, err := json.Marshal(ctr)
	if err != nil {
		t.Fatal(err)
	}
	var got Container
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.SecurityContext == nil || *got.SecurityContext.RunAsUser != 0 {
		t.Errorf("SecurityContext lost: %v", got.SecurityContext)
	}
}
