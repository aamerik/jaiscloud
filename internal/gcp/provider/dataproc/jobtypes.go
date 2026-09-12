package dataproc

import (
	"encoding/json"
	"fmt"

	"jaiscloud/internal/clock"
	dataprocstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/model"
	"jaiscloud/internal/sparkhelpers"
)

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

// jobToStore builds the store Job from a SubmitJob body's nested "job" object.
func jobToStore(nr *model.NormalizedRequest, jobBody map[string]any, region string) (dataprocstore.Job, error) {
	jobID := bodyString(jobBodyRef(jobBody), "jobId")
	if jobID == "" {
		jobID = randomHex(16)
	}
	placement, _ := jobBody["placement"].(map[string]any)
	clusterName := bodyString(placement, "clusterName")
	jobType, typeJob := extractJobType(jobBody)

	now := clock.Now().UTC()
	j := dataprocstore.Job{
		ProjectID:            nr.AccountID,
		Region:               region,
		JobID:                jobID,
		PlacementClusterName: clusterName,
		Type:                 jobType,
		Labels:               bodyStringMap(jobBody, "labels"),
		Status:               dataprocstore.JobStatus{State: "RUNNING", StateStartTime: now, Substate: substateRunning},
		JobUUID:              randomHex(32),
		CreateTime:           now,
	}
	if jobType != "" && typeJob != nil {
		if data, err := json.Marshal(typeJob); err == nil {
			j.TypeJob = data
		}
	}
	return j, nil
}

// jobBodyRef returns the job reference object, or an empty map when absent.
func jobBodyRef(jobBody map[string]any) map[string]any {
	if jobBody == nil {
		return nil
	}
	ref, _ := jobBody["reference"].(map[string]any)
	return ref
}
