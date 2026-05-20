package asl

import "encoding/json"

// StateMachineDefinition is the top-level ASL structure.
type StateMachineDefinition struct {
	Comment        string              `json:"Comment,omitempty"`
	StartAt        string              `json:"StartAt"`
	States         map[string]StateDef `json:"-"` // populated by Parse
	Version        string              `json:"Version,omitempty"`
	TimeoutSeconds int                 `json:"TimeoutSeconds,omitempty"`
	QueryLanguage  string              `json:"QueryLanguage,omitempty"` // "JSONPath" (default) or "JSONata"
}

// StateDef is the common interface for all state types.
type StateDef interface {
	GetType() string
	GetNext() string
	IsEnd() bool
}

type StateType string

const (
	StateTypePass     StateType = "Pass"
	StateTypeTask     StateType = "Task"
	StateTypeChoice   StateType = "Choice"
	StateTypeWait     StateType = "Wait"
	StateTypeSucceed  StateType = "Succeed"
	StateTypeFail     StateType = "Fail"
	StateTypeParallel StateType = "Parallel"
	StateTypeMap      StateType = "Map"
)

// CommonState holds fields shared by all state types.
type CommonState struct {
	Type       StateType `json:"Type"`
	Comment    string    `json:"Comment,omitempty"`
	Next       string    `json:"Next,omitempty"`
	End        bool      `json:"End,omitempty"`
	InputPath  *string   `json:"InputPath,omitempty"`
	OutputPath *string   `json:"OutputPath,omitempty"`
}

func (c *CommonState) GetType() string { return string(c.Type) }
func (c *CommonState) GetNext() string { return c.Next }
func (c *CommonState) IsEnd() bool     { return c.End }

type PassState struct {
	CommonState
	Result     json.RawMessage `json:"Result,omitempty"`
	ResultPath *string         `json:"ResultPath,omitempty"`
	Parameters json.RawMessage `json:"Parameters,omitempty"`
}

type TaskState struct {
	CommonState
	Resource             string          `json:"Resource"`
	Parameters           json.RawMessage `json:"Parameters,omitempty"`
	ResultSelector       json.RawMessage `json:"ResultSelector,omitempty"`
	ResultPath           *string         `json:"ResultPath,omitempty"`
	Retry                []RetryRule     `json:"Retry,omitempty"`
	Catch                []CatchRule     `json:"Catch,omitempty"`
	TimeoutSeconds       int             `json:"TimeoutSeconds,omitempty"`
	TimeoutSecondsPath   string          `json:"TimeoutSecondsPath,omitempty"`
	HeartbeatSeconds     int             `json:"HeartbeatSeconds,omitempty"`
	HeartbeatSecondsPath string          `json:"HeartbeatSecondsPath,omitempty"`
	Credentials          *Credentials    `json:"Credentials,omitempty"`
}

type Credentials struct {
	RoleArn string `json:"RoleArn,omitempty"`
}

type ChoiceState struct {
	CommonState
	Choices []ChoiceRule `json:"Choices"`
	Default string       `json:"Default,omitempty"`
}

type WaitState struct {
	CommonState
	Seconds       int    `json:"Seconds,omitempty"`
	SecondsPath   string `json:"SecondsPath,omitempty"`
	Timestamp     string `json:"Timestamp,omitempty"`
	TimestampPath string `json:"TimestampPath,omitempty"`
}

type SucceedState struct {
	CommonState
}

type FailState struct {
	CommonState
	Error     string `json:"Error,omitempty"`
	ErrorPath string `json:"ErrorPath,omitempty"`
	Cause     string `json:"Cause,omitempty"`
	CausePath string `json:"CausePath,omitempty"`
}

type ParallelState struct {
	CommonState
	Branches       []StateMachineDefinition `json:"Branches"`
	Parameters     json.RawMessage          `json:"Parameters,omitempty"`
	ResultSelector json.RawMessage          `json:"ResultSelector,omitempty"`
	ResultPath     *string                  `json:"ResultPath,omitempty"`
	Retry          []RetryRule              `json:"Retry,omitempty"`
	Catch          []CatchRule              `json:"Catch,omitempty"`
}

type MapState struct {
	CommonState
	ItemProcessor              *ItemProcessor          `json:"ItemProcessor,omitempty"`
	Iterator                   *StateMachineDefinition `json:"Iterator,omitempty"` // legacy
	ItemsPath                  string                  `json:"ItemsPath,omitempty"`
	ItemSelector               json.RawMessage         `json:"ItemSelector,omitempty"`
	Parameters                 json.RawMessage         `json:"Parameters,omitempty"` // legacy
	ResultSelector             json.RawMessage         `json:"ResultSelector,omitempty"`
	ResultPath                 *string                 `json:"ResultPath,omitempty"`
	MaxConcurrency             int                     `json:"MaxConcurrency,omitempty"`
	MaxConcurrencyPath         string                  `json:"MaxConcurrencyPath,omitempty"`
	Retry                      []RetryRule             `json:"Retry,omitempty"`
	Catch                      []CatchRule             `json:"Catch,omitempty"`
	ToleratedFailureCount      int                     `json:"ToleratedFailureCount,omitempty"`
	ToleratedFailurePercentage float64                 `json:"ToleratedFailurePercentage,omitempty"`
	ItemReader                 *ItemReader             `json:"ItemReader,omitempty"`
	ResultWriter               *ResultWriter           `json:"ResultWriter,omitempty"`
}

type ItemProcessor struct {
	ProcessorConfig *ProcessorConfig    `json:"ProcessorConfig,omitempty"`
	StartAt         string              `json:"StartAt"`
	States          map[string]StateDef `json:"-"`
}

type ProcessorConfig struct {
	Mode          string `json:"Mode,omitempty"`
	ExecutionType string `json:"ExecutionType,omitempty"`
}

type ItemReader struct {
	Resource     string          `json:"Resource"`
	ReaderConfig *ReaderConfig   `json:"ReaderConfig,omitempty"`
	Parameters   json.RawMessage `json:"Parameters,omitempty"`
}

type ReaderConfig struct {
	InputType         string `json:"InputType,omitempty"`
	CSVHeaderLocation string `json:"CSVHeaderLocation,omitempty"`
	MaxItems          int    `json:"MaxItems,omitempty"`
}

type ResultWriter struct {
	Resource   string          `json:"Resource"`
	Parameters json.RawMessage `json:"Parameters,omitempty"`
}

type RetryRule struct {
	ErrorEquals     []string `json:"ErrorEquals"`
	IntervalSeconds int      `json:"IntervalSeconds,omitempty"`
	MaxAttempts     int      `json:"MaxAttempts,omitempty"`
	BackoffRate     float64  `json:"BackoffRate,omitempty"`
	MaxDelaySeconds int      `json:"MaxDelaySeconds,omitempty"`
	JitterStrategy  string   `json:"JitterStrategy,omitempty"`
}

type CatchRule struct {
	ErrorEquals []string `json:"ErrorEquals"`
	Next        string   `json:"Next"`
	ResultPath  *string  `json:"ResultPath,omitempty"`
}

type ChoiceRule struct {
	Variable string `json:"Variable,omitempty"`

	// String comparisons
	StringEquals                string `json:"StringEquals,omitempty"`
	StringEqualsPath            string `json:"StringEqualsPath,omitempty"`
	StringLessThan              string `json:"StringLessThan,omitempty"`
	StringLessThanPath          string `json:"StringLessThanPath,omitempty"`
	StringGreaterThan           string `json:"StringGreaterThan,omitempty"`
	StringGreaterThanPath       string `json:"StringGreaterThanPath,omitempty"`
	StringLessThanEquals        string `json:"StringLessThanEquals,omitempty"`
	StringLessThanEqualsPath    string `json:"StringLessThanEqualsPath,omitempty"`
	StringGreaterThanEquals     string `json:"StringGreaterThanEquals,omitempty"`
	StringGreaterThanEqualsPath string `json:"StringGreaterThanEqualsPath,omitempty"`
	StringMatches               string `json:"StringMatches,omitempty"`

	// Numeric comparisons
	NumericEquals                *float64 `json:"NumericEquals,omitempty"`
	NumericEqualsPath            string   `json:"NumericEqualsPath,omitempty"`
	NumericLessThan              *float64 `json:"NumericLessThan,omitempty"`
	NumericLessThanPath          string   `json:"NumericLessThanPath,omitempty"`
	NumericGreaterThan           *float64 `json:"NumericGreaterThan,omitempty"`
	NumericGreaterThanPath       string   `json:"NumericGreaterThanPath,omitempty"`
	NumericLessThanEquals        *float64 `json:"NumericLessThanEquals,omitempty"`
	NumericLessThanEqualsPath    string   `json:"NumericLessThanEqualsPath,omitempty"`
	NumericGreaterThanEquals     *float64 `json:"NumericGreaterThanEquals,omitempty"`
	NumericGreaterThanEqualsPath string   `json:"NumericGreaterThanEqualsPath,omitempty"`

	// Boolean comparisons
	BooleanEquals     *bool  `json:"BooleanEquals,omitempty"`
	BooleanEqualsPath string `json:"BooleanEqualsPath,omitempty"`

	// Timestamp comparisons
	TimestampEquals                string `json:"TimestampEquals,omitempty"`
	TimestampEqualsPath            string `json:"TimestampEqualsPath,omitempty"`
	TimestampLessThan              string `json:"TimestampLessThan,omitempty"`
	TimestampLessThanPath          string `json:"TimestampLessThanPath,omitempty"`
	TimestampGreaterThan           string `json:"TimestampGreaterThan,omitempty"`
	TimestampGreaterThanPath       string `json:"TimestampGreaterThanPath,omitempty"`
	TimestampLessThanEquals        string `json:"TimestampLessThanEquals,omitempty"`
	TimestampLessThanEqualsPath    string `json:"TimestampLessThanEqualsPath,omitempty"`
	TimestampGreaterThanEquals     string `json:"TimestampGreaterThanEquals,omitempty"`
	TimestampGreaterThanEqualsPath string `json:"TimestampGreaterThanEqualsPath,omitempty"`

	// Type checks
	IsNull      *bool `json:"IsNull,omitempty"`
	IsPresent   *bool `json:"IsPresent,omitempty"`
	IsNumeric   *bool `json:"IsNumeric,omitempty"`
	IsString    *bool `json:"IsString,omitempty"`
	IsBoolean   *bool `json:"IsBoolean,omitempty"`
	IsTimestamp *bool `json:"IsTimestamp,omitempty"`

	// Compound
	And []ChoiceRule `json:"And,omitempty"`
	Or  []ChoiceRule `json:"Or,omitempty"`
	Not *ChoiceRule  `json:"Not,omitempty"`

	Next string `json:"Next,omitempty"`
}
