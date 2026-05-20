// Package stepfunctions provides in-memory state for AWS Step Functions.
package stepfunctions

import "time"

type StateMachineType string

const (
	StateMachineTypeStandard StateMachineType = "STANDARD"
	StateMachineTypeExpress  StateMachineType = "EXPRESS"
)

type StateMachineStatus string

const (
	StateMachineStatusActive   StateMachineStatus = "ACTIVE"
	StateMachineStatusDeleting StateMachineStatus = "DELETING"
)

type ExecutionStatus string

const (
	ExecutionStatusRunning        ExecutionStatus = "RUNNING"
	ExecutionStatusSucceeded      ExecutionStatus = "SUCCEEDED"
	ExecutionStatusFailed         ExecutionStatus = "FAILED"
	ExecutionStatusTimedOut       ExecutionStatus = "TIMED_OUT"
	ExecutionStatusAborted        ExecutionStatus = "ABORTED"
	ExecutionStatusPendingRedrive ExecutionStatus = "PENDING_REDRIVE"
)

type StateMachine struct {
	Name                    string
	ARN                     string
	RevisionID              string
	Definition              string
	RoleARN                 string
	Type                    StateMachineType
	Status                  StateMachineStatus
	LoggingConfiguration    *LoggingConfiguration
	TracingConfiguration    *TracingConfiguration
	EncryptionConfiguration *EncryptionConfiguration
	CreateDate              time.Time
	Tags                    map[string]string
	Versions                map[int64]*StateMachineVersion
	Aliases                 map[string]*StateMachineAlias // alias name → alias
	Description             string
	NextVersion             int64
}

type StateMachineVersion struct {
	Version         int64
	ARN             string
	StateMachineARN string
	RevisionID      string
	Definition      string
	Description     string
	CreationDate    time.Time
}

type StateMachineAlias struct {
	Name                 string
	ARN                  string
	Description          string
	RoutingConfiguration []RoutingConfig
	CreationDate         time.Time
	UpdateDate           time.Time
}

type RoutingConfig struct {
	StateMachineVersionARN string
	Weight                 int
}

type LoggingConfiguration struct {
	Level                string
	IncludeExecutionData bool
	Destinations         []LogDestination
}

type LogDestination struct {
	CloudWatchLogsLogGroup struct {
		LogGroupArn string
	}
}

type TracingConfiguration struct {
	Enabled bool
}

type EncryptionConfiguration struct {
	Type                         string
	KMSKeyID                     string
	KMSDataKeyReusePeriodSeconds int
}

type Execution struct {
	Name                   string
	ARN                    string
	StateMachineARN        string
	StateMachineVersionARN string
	StateMachineAliasARN   string
	Status                 ExecutionStatus
	StartDate              time.Time
	StopDate               *time.Time
	Input                  string
	InputDetails           map[string]any
	Output                 string
	OutputDetails          map[string]any
	Error                  string
	Cause                  string
	TraceHeader            string
	History                []HistoryEvent
	RedriveCount           int
	RedriveDate            *time.Time
	RedriveStatus          string
	RedriveStatusReason    string
}

type HistoryEvent struct {
	ID              int64
	PreviousEventID int64
	Timestamp       time.Time
	Type            string

	ExecutionStartedEventDetails   *ExecutionStartedEventDetails
	ExecutionSucceededEventDetails *ExecutionSucceededEventDetails
	ExecutionFailedEventDetails    *ExecutionFailedEventDetails
	ExecutionAbortedEventDetails   *ExecutionAbortedEventDetails
	StateEnteredEventDetails       *StateEnteredEventDetails
	StateExitedEventDetails        *StateExitedEventDetails
	TaskScheduledEventDetails      *TaskScheduledEventDetails
	TaskSucceededEventDetails      *TaskSucceededEventDetails
	TaskFailedEventDetails         *TaskFailedEventDetails
}

type ExecutionStartedEventDetails struct {
	Input   string
	RoleArn string
}

type ExecutionSucceededEventDetails struct {
	Output string
}

type ExecutionFailedEventDetails struct {
	Error string
	Cause string
}

type ExecutionAbortedEventDetails struct {
	Error string
	Cause string
}

type StateEnteredEventDetails struct {
	Name  string
	Input string
}

type StateExitedEventDetails struct {
	Name   string
	Output string
}

type TaskScheduledEventDetails struct {
	ResourceType string
	Resource     string
	Region       string
	Parameters   string
}

type TaskSucceededEventDetails struct {
	ResourceType string
	Resource     string
	Output       string
}

type TaskFailedEventDetails struct {
	ResourceType string
	Resource     string
	Error        string
	Cause        string
}

type Activity struct {
	Name         string
	ARN          string
	CreationDate time.Time
	Tags         map[string]string
}
