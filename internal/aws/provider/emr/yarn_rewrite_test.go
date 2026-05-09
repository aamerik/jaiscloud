package emr

import (
	"testing"
)

func TestRewriteYARNToK8s_YARNTwoToken(t *testing.T) {
	args := []string{"--master", "yarn", "--class", "com.example.App", "s3://bucket/app.jar"}
	got, rewrote := rewriteYARNToK8s(args)
	if !rewrote {
		t.Fatal("expected rewrote=true for --master yarn")
	}
	if got[0] != "--master" || got[1] != "k8s://kubernetes.default.svc" {
		t.Errorf("expected --master k8s://kubernetes.default.svc, got %v", got[:2])
	}
	// remaining args preserved
	if got[2] != "--class" || got[3] != "com.example.App" {
		t.Errorf("remaining args not preserved: %v", got[2:])
	}
}

func TestRewriteYARNToK8s_YARNEqualsForm(t *testing.T) {
	args := []string{"--master=yarn", "--class", "com.example.App"}
	got, rewrote := rewriteYARNToK8s(args)
	if !rewrote {
		t.Fatal("expected rewrote=true for --master=yarn")
	}
	if got[0] != "--master=k8s://kubernetes.default.svc" {
		t.Errorf("expected --master=k8s://kubernetes.default.svc, got %q", got[0])
	}
}

func TestRewriteYARNToK8s_K8sMasterUnchanged(t *testing.T) {
	args := []string{"--master", "k8s://https://127.0.0.1:6443", "--class", "App"}
	got, rewrote := rewriteYARNToK8s(args)
	if rewrote {
		t.Fatal("expected rewrote=false for k8s:// master")
	}
	if got[1] != "k8s://https://127.0.0.1:6443" {
		t.Errorf("k8s:// master should be unchanged, got %q", got[1])
	}
}

func TestRewriteYARNToK8s_LocalMasterUnchanged(t *testing.T) {
	args := []string{"--master", "local[*]", "app.jar"}
	got, rewrote := rewriteYARNToK8s(args)
	if rewrote {
		t.Fatal("expected rewrote=false for local[*] master")
	}
	if got[1] != "local[*]" {
		t.Errorf("local[*] master should be unchanged, got %q", got[1])
	}
}

func TestRewriteYARNToK8s_NoMasterUnchanged(t *testing.T) {
	args := []string{"--class", "com.example.App", "s3://bucket/app.jar"}
	got, rewrote := rewriteYARNToK8s(args)
	if rewrote {
		t.Fatal("expected rewrote=false when no --master present")
	}
	if len(got) != len(args) {
		t.Errorf("expected args unchanged, got %v", got)
	}
}
