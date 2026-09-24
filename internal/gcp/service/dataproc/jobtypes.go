package dataproc

import (
	"encoding/json"
	"fmt"

	"jaiscloud/internal/clock"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/sparkhelpers"
)

// JobInput carries the caller-supplied fields of a job submission.
type JobInput struct {
	JobID                string
	PlacementClusterName string
	Type                 string
	TypeJob              json.RawMessage
	Labels               map[string]string
}

// JobInputFromMap builds a JobInput from the Discovery/proto Job map (the
// nested "job" object of a SubmitJob request).
func JobInputFromMap(jobBody map[string]any) JobInput {
	in := JobInput{Labels: bodyStringMap(jobBody, "labels")}
	if ref, ok := jobBody["reference"].(map[string]any); ok {
		in.JobID = bodyString(ref, "jobId")
	}
	if placement, ok := jobBody["placement"].(map[string]any); ok {
		in.PlacementClusterName = bodyString(placement, "clusterName")
	}
	jobType, typeJob := extractJobType(jobBody)
	in.Type = jobType
	if jobType != "" && typeJob != nil {
		if data, err := json.Marshal(typeJob); err == nil {
			in.TypeJob = data
		}
	}
	return in
}

// jobTypeKeys is the set of Dataproc oneof type-job field names in precedence
// order. Only Spark-family types run; hadoopJob/hiveJob/pigJob/sparkSqlJob/
// prestoJob/trinoJob/flinkJob are unsupported.
var jobTypeKeys = []string{
	"sparkJob", "pysparkJob", "sparkSqlJob", "sparkRJob",
	"hadoopJob", "hiveJob", "pigJob",
	"prestoJob", "trinoJob", "flinkJob",
}

// extractJobType returns the type-job field name and its body, or ("", nil)
// when the job carries no recognised type.
func extractJobType(body map[string]any) (string, map[string]any) {
	for _, k := range jobTypeKeys {
		if m, ok := body[k].(map[string]any); ok {
			return k, m
		}
	}
	return "", nil
}

// unsupportedJobTypes are job types the emulator does not run.
var unsupportedJobTypes = map[string]bool{
	"hadoopJob":   true,
	"hiveJob":     true,
	"pigJob":      true,
	"sparkSqlJob": true,
	"prestoJob":   true,
	"trinoJob":    true,
	"flinkJob":    true,
}

// jobToEntryPoint maps a Dataproc type-job body to a sparkhelpers.EntryPoint.
// Returns an error for unsupported (fail-loud) or malformed job types.
func jobToEntryPoint(jobType string, typeJob map[string]any) (sparkhelpers.EntryPoint, []string, error) {
	switch jobType {
	case "sparkJob":
		mainJar := bodyString(typeJob, "mainJarFileUri")
		mainClass := bodyString(typeJob, "mainClass")
		if mainJar == "" && mainClass == "" {
			return nil, nil, fmt.Errorf("sparkJob requires mainJarFileUri or mainClass")
		}
		ep := sparkhelpers.JarEntryPoint{JarURI: mainJar, MainClass: mainClass}
		return ep, bodyStringSlice(typeJob, "args"), nil
	case "pysparkJob":
		mainPy := bodyString(typeJob, "mainPythonFileUri")
		if mainPy == "" {
			return nil, nil, fmt.Errorf("pysparkJob requires mainPythonFileUri")
		}
		ep := sparkhelpers.PythonEntryPoint{
			MainPythonFile: mainPy,
			PyFiles:        bodyStringSlice(typeJob, "pythonFileUris"),
		}
		return ep, bodyStringSlice(typeJob, "args"), nil
	case "sparkRJob":
		mainR := bodyString(typeJob, "mainRFileUri")
		if mainR == "" {
			return nil, nil, fmt.Errorf("sparkRJob requires mainRFileUri")
		}
		ep := sparkhelpers.REntryPoint{MainRFile: mainR}
		return ep, bodyStringSlice(typeJob, "args"), nil
	default:
		return nil, nil, fmt.Errorf("job type %q is not supported by the emulator", jobType)
	}
}

// propertiesToConfArgs converts a type-job properties map into "--conf k=v"
// flags (Spark last-value-wins, so these are prepended before caller args).
func propertiesToConfArgs(typeJob map[string]any) []string {
	props, _ := typeJob["properties"].(map[string]any)
	if props == nil {
		return nil
	}
	var out []string
	for k, v := range props {
		if s, ok := v.(string); ok {
			out = append(out, "--conf", k+"="+s)
		}
	}
	return out
}

// jobToStore builds the store Job from a SubmitJob input.
func jobToStore(project, region string, in JobInput) dpstore.Job {
	jobID := in.JobID
	if jobID == "" {
		jobID = randomHex(16)
	}
	now := clock.Now().UTC()
	return dpstore.Job{
		ProjectID:            project,
		Region:               region,
		JobID:                jobID,
		PlacementClusterName: in.PlacementClusterName,
		Type:                 in.Type,
		TypeJob:              in.TypeJob,
		Labels:               in.Labels,
		Status:               dpstore.JobStatus{State: "RUNNING", StateStartTime: now, Substate: substateRunning},
		JobUUID:              randomHex(32),
		CreateTime:           now,
	}
}
