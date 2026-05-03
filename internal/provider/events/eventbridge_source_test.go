package events

import (
	"testing"

	"jaiscloud/internal/events"
	"jaiscloud/internal/model"
)

func TestAWSSource(t *testing.T) {
	cases := []struct {
		cloud   model.Cloud
		service string
		want    string
	}{
		{model.CloudAWS, "emr", "aws.emr"},
		{"", "emr", "aws.emr"},
		{"", "emr-containers", "aws.emr-containers"},
		{model.CloudAWS, "emr-containers", "aws.emr-containers"},
	}
	for _, c := range cases {
		got := awsSource(c.cloud, c.service)
		if got != c.want {
			t.Errorf("awsSource(%q,%q) = %q, want %q", c.cloud, c.service, got, c.want)
		}
	}
}

func TestBuildEMRStepEnvelope_EmptyCloudFallsBack(t *testing.T) {
	ev := events.EMRStepStateEvent{JobFlowID: "j-1", StepID: "s-1", State: "RUNNING"}
	env := buildEMRStepEnvelope(ev)
	if env["source"] != "aws.emr" {
		t.Errorf("empty Cloud must yield aws.emr; got %q", env["source"])
	}
}

func TestBuildEMRClusterEnvelope_EmptyCloudFallsBack(t *testing.T) {
	ev := events.EMRClusterStateEvent{ClusterID: "j-1", State: "STARTING"}
	env := buildEMRClusterEnvelope(ev)
	if env["source"] != "aws.emr" {
		t.Errorf("empty Cloud must yield aws.emr; got %q", env["source"])
	}
}

func TestBuildEMRJobRunEnvelope_EmptyCloudFallsBack(t *testing.T) {
	ev := events.EMRJobRunStateEvent{VirtualClusterID: "vc-1", JobRunID: "jr-1", State: "RUNNING"}
	env := buildEMRJobRunEnvelope(ev)
	if env["source"] != "aws.emr-containers" {
		t.Errorf("empty Cloud must yield aws.emr-containers; got %q", env["source"])
	}
}
