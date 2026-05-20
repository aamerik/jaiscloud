package stepfunctions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
)

// MemoryStepFunctionsStore is the default in-memory Step Functions store.
type MemoryStepFunctionsStore struct {
	mu           sync.RWMutex
	machines     map[string]*StateMachine // ARN → machine
	nameToARN    map[string]string        // "account:name" → ARN (scoped to prevent cross-account collisions)
	executions   map[string]*Execution   // ARN → execution
	activities   map[string]*Activity    // ARN → activity
	// ARN-keyed tags for any resource type (machines, activities, aliases)
	tags         map[string]map[string]string
}

// sfnNameKey returns the scoped nameToARN key for a state machine.
// ARN format: arn:aws:states:region:account:stateMachine:name
func sfnNameKey(arn, name string) string {
	parts := strings.SplitN(arn, ":", 7)
	if len(parts) >= 5 {
		return parts[4] + ":" + name
	}
	return name
}

func NewMemoryStepFunctionsStore() *MemoryStepFunctionsStore {
	return &MemoryStepFunctionsStore{
		machines:   make(map[string]*StateMachine),
		nameToARN:  make(map[string]string),
		executions: make(map[string]*Execution),
		activities: make(map[string]*Activity),
		tags:       make(map[string]map[string]string),
	}
}

// ─── State Machine CRUD ───────────────────────────────────────────────────────

func (s *MemoryStepFunctionsStore) CreateStateMachine(sm *StateMachine) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.machines[sm.ARN]; ok {
		if existing.Status == StateMachineStatusDeleting {
			return &SFNError{Code: "StateMachineDeleting", Message: fmt.Sprintf("State machine '%s' is being deleted", sm.Name), Status: 400}
		}
		return &SFNError{Code: "StateMachineAlreadyExists", Message: fmt.Sprintf("State machine already exists: '%s'", sm.ARN), Status: 400}
	}
	if _, exists := s.nameToARN[sfnNameKey(sm.ARN, sm.Name)]; exists {
		return &SFNError{Code: "StateMachineAlreadyExists", Message: fmt.Sprintf("State machine already exists: '%s'", sm.Name), Status: 400}
	}
	if len(s.machines) >= 10000 {
		return &SFNError{Code: "StateMachineLimitExceeded", Message: "Maximum number of state machines reached", Status: 400}
	}
	clone := cloneSM(sm)
	if clone.Versions == nil {
		clone.Versions = make(map[int64]*StateMachineVersion)
	}
	if clone.Aliases == nil {
		clone.Aliases = make(map[string]*StateMachineAlias)
	}
	s.machines[clone.ARN] = clone
	s.nameToARN[sfnNameKey(clone.ARN, clone.Name)] = clone.ARN
	if clone.Tags != nil {
		s.tags[clone.ARN] = cloneStringMap(clone.Tags)
	}
	return nil
}

func (s *MemoryStepFunctionsStore) GetStateMachine(arn string) (*StateMachine, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sm, ok := s.machines[arn]
	if !ok {
		// Support lookup by name too
		if a, ok2 := s.nameToARN[arn]; ok2 {
			sm = s.machines[a]
		} else {
			return nil, &SFNError{Code: "StateMachineDoesNotExist", Message: fmt.Sprintf("State machine does not exist: '%s'", arn), Status: 400}
		}
	}
	return cloneSM(sm), nil
}

func (s *MemoryStepFunctionsStore) UpdateStateMachine(arn, def, roleARN string, logging *LoggingConfiguration, tracing *TracingConfiguration, description string, revisionID string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sm, ok := s.machines[arn]
	if !ok {
		return "", &SFNError{Code: "StateMachineDoesNotExist", Message: fmt.Sprintf("State machine does not exist: '%s'", arn), Status: 400}
	}
	if revisionID != "" && sm.RevisionID != revisionID {
		return "", &SFNError{Code: "ConflictException", Message: "Revision ID does not match current revision", Status: 400}
	}
	newRevision := newUUID()
	if def != "" {
		sm.Definition = def
	}
	if roleARN != "" {
		sm.RoleARN = roleARN
	}
	if logging != nil {
		sm.LoggingConfiguration = logging
	}
	if tracing != nil {
		sm.TracingConfiguration = tracing
	}
	if description != "" {
		sm.Description = description
	}
	sm.RevisionID = newRevision
	return newRevision, nil
}

func (s *MemoryStepFunctionsStore) DeleteStateMachine(arn string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sm, ok := s.machines[arn]
	if !ok {
		return &SFNError{Code: "StateMachineDoesNotExist", Message: fmt.Sprintf("State machine does not exist: '%s'", arn), Status: 400}
	}
	delete(s.nameToARN, sfnNameKey(arn, sm.Name))
	delete(s.machines, arn)
	delete(s.tags, arn)
	return nil
}

// ListStateMachines returns machines for accountID (when non-empty) or all machines (when empty).
func (s *MemoryStepFunctionsStore) ListStateMachines(accountID string) []*StateMachine {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*StateMachine, 0, len(s.machines))
	for _, sm := range s.machines {
		if accountID != "" && !strings.Contains(sm.ARN, ":"+accountID+":") {
			continue
		}
		out = append(out, cloneSM(sm))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// ─── Versions ─────────────────────────────────────────────────────────────────

func (s *MemoryStepFunctionsStore) PublishVersion(smARN, description, revisionID string) (*StateMachineVersion, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sm, ok := s.machines[smARN]
	if !ok {
		return nil, &SFNError{Code: "StateMachineDoesNotExist", Message: "State machine does not exist", Status: 400}
	}
	if revisionID != "" && sm.RevisionID != revisionID {
		return nil, &SFNError{Code: "ConflictException", Message: "Revision ID does not match current revision", Status: 400}
	}
	sm.NextVersion++
	versionNum := sm.NextVersion
	versionARN := fmt.Sprintf("%s:%d", smARN, versionNum)
	ver := &StateMachineVersion{
		Version:         versionNum,
		ARN:             versionARN,
		StateMachineARN: smARN,
		RevisionID:      sm.RevisionID,
		Definition:      sm.Definition,
		Description:     description,
		CreationDate:    now(),
	}
	sm.Versions[versionNum] = ver
	return ver, nil
}

func (s *MemoryStepFunctionsStore) DeleteVersion(versionARN string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	smARN, versionNum, err := parseVersionARN(versionARN)
	if err != nil {
		return &SFNError{Code: "ResourceNotFound", Message: "Version not found", Status: 400}
	}
	sm, ok := s.machines[smARN]
	if !ok {
		return &SFNError{Code: "ResourceNotFound", Message: "State machine not found", Status: 400}
	}
	if _, exists := sm.Versions[versionNum]; !exists {
		return &SFNError{Code: "ResourceNotFound", Message: "Version not found", Status: 400}
	}
	// Check if any alias references this version
	for _, alias := range sm.Aliases {
		for _, rc := range alias.RoutingConfiguration {
			if rc.StateMachineVersionARN == versionARN {
				return &SFNError{Code: "ConflictException", Message: "Version is referenced by an alias", Status: 400}
			}
		}
	}
	delete(sm.Versions, versionNum)
	return nil
}

func (s *MemoryStepFunctionsStore) ListVersions(smARN string) []*StateMachineVersion {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sm, ok := s.machines[smARN]
	if !ok {
		return nil
	}
	out := make([]*StateMachineVersion, 0, len(sm.Versions))
	for _, v := range sm.Versions {
		cp := *v
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out
}

// ─── Aliases ─────────────────────────────────────────────────────────────────

func (s *MemoryStepFunctionsStore) CreateAlias(smARN, aliasName string, routing []RoutingConfig, description string) (*StateMachineAlias, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sm, ok := s.machines[smARN]
	if !ok {
		return nil, &SFNError{Code: "StateMachineDoesNotExist", Message: "State machine does not exist", Status: 400}
	}
	if _, exists := sm.Aliases[aliasName]; exists {
		return nil, &SFNError{Code: "ConflictException", Message: fmt.Sprintf("Alias '%s' already exists", aliasName), Status: 400}
	}
	if err := validateRoutingConfig(routing, sm); err != nil {
		return nil, err
	}
	aliasARN := fmt.Sprintf("%s:%s", smARN, aliasName)
	alias := &StateMachineAlias{
		Name:                 aliasName,
		ARN:                  aliasARN,
		Description:          description,
		RoutingConfiguration: routing,
		CreationDate:         now(),
		UpdateDate:           now(),
	}
	sm.Aliases[aliasName] = alias
	return alias, nil
}

func (s *MemoryStepFunctionsStore) UpdateAlias(aliasARN string, routing []RoutingConfig, description string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	smARN, aliasName, err := parseAliasARN(aliasARN)
	if err != nil {
		return &SFNError{Code: "ResourceNotFound", Message: "Alias not found", Status: 400}
	}
	sm, ok := s.machines[smARN]
	if !ok {
		return &SFNError{Code: "ResourceNotFound", Message: "State machine not found", Status: 400}
	}
	alias, ok := sm.Aliases[aliasName]
	if !ok {
		return &SFNError{Code: "ResourceNotFound", Message: "Alias not found", Status: 400}
	}
	if err := validateRoutingConfig(routing, sm); err != nil {
		return err
	}
	if routing != nil {
		alias.RoutingConfiguration = routing
	}
	if description != "" {
		alias.Description = description
	}
	alias.UpdateDate = now()
	return nil
}

func (s *MemoryStepFunctionsStore) DeleteAlias(aliasARN string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	smARN, aliasName, err := parseAliasARN(aliasARN)
	if err != nil {
		return &SFNError{Code: "ResourceNotFound", Message: "Alias not found", Status: 400}
	}
	sm, ok := s.machines[smARN]
	if !ok {
		return &SFNError{Code: "ResourceNotFound", Message: "State machine not found", Status: 400}
	}
	if _, ok := sm.Aliases[aliasName]; !ok {
		return &SFNError{Code: "ResourceNotFound", Message: "Alias not found", Status: 400}
	}
	delete(sm.Aliases, aliasName)
	return nil
}

func (s *MemoryStepFunctionsStore) DescribeAlias(aliasARN string) (*StateMachineAlias, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	smARN, aliasName, err := parseAliasARN(aliasARN)
	if err != nil {
		return nil, &SFNError{Code: "ResourceNotFound", Message: "Alias not found", Status: 400}
	}
	sm, ok := s.machines[smARN]
	if !ok {
		return nil, &SFNError{Code: "ResourceNotFound", Message: "State machine not found", Status: 400}
	}
	alias, ok := sm.Aliases[aliasName]
	if !ok {
		return nil, &SFNError{Code: "ResourceNotFound", Message: "Alias not found", Status: 400}
	}
	cp := *alias
	return &cp, nil
}

func (s *MemoryStepFunctionsStore) ListAliases(smARN string) []*StateMachineAlias {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sm, ok := s.machines[smARN]
	if !ok {
		return nil
	}
	out := make([]*StateMachineAlias, 0, len(sm.Aliases))
	for _, a := range sm.Aliases {
		cp := *a
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// ─── Executions ───────────────────────────────────────────────────────────────

func (s *MemoryStepFunctionsStore) StartExecution(exec *Execution) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.executions[exec.ARN]; ok {
		if existing.Input == exec.Input {
			return nil // idempotent
		}
		return &SFNError{Code: "ExecutionAlreadyExists", Message: fmt.Sprintf("Execution already exists: '%s'", exec.ARN), Status: 400}
	}
	if len(s.executions) >= 100000 {
		return &SFNError{Code: "ExecutionLimitExceeded", Message: "Maximum number of executions reached", Status: 400}
	}
	cp := *exec
	if cp.History == nil {
		cp.History = []HistoryEvent{}
	}
	s.executions[cp.ARN] = &cp
	return nil
}

func (s *MemoryStepFunctionsStore) GetExecution(arn string) (*Execution, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.executions[arn]
	if !ok {
		return nil, &SFNError{Code: "ExecutionDoesNotExist", Message: fmt.Sprintf("Execution does not exist: '%s'", arn), Status: 400}
	}
	cp := *e
	cp.History = append([]HistoryEvent(nil), e.History...)
	return &cp, nil
}

func (s *MemoryStepFunctionsStore) StopExecution(arn, errMsg, cause string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.executions[arn]
	if !ok {
		return &SFNError{Code: "ExecutionDoesNotExist", Message: fmt.Sprintf("Execution does not exist: '%s'", arn), Status: 400}
	}
	// No-op if already terminal
	if e.Status != ExecutionStatusRunning {
		return nil
	}
	t := now()
	e.Status = ExecutionStatusAborted
	e.StopDate = &t
	e.Error = errMsg
	e.Cause = cause
	return nil
}

func (s *MemoryStepFunctionsStore) ListExecutions(smARN string, statusFilter ExecutionStatus) []*Execution {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*Execution
	for _, e := range s.executions {
		if e.StateMachineARN != smARN {
			continue
		}
		if statusFilter != "" && e.Status != statusFilter {
			continue
		}
		cp := *e
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartDate.After(out[j].StartDate) })
	return out
}

func (s *MemoryStepFunctionsStore) GetExecutionHistory(arn string, reverseOrder bool) ([]HistoryEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.executions[arn]
	if !ok {
		return nil, &SFNError{Code: "ExecutionDoesNotExist", Message: "Execution does not exist", Status: 400}
	}
	events := append([]HistoryEvent(nil), e.History...)
	if reverseOrder {
		for i, j := 0, len(events)-1; i < j; i, j = i+1, j-1 {
			events[i], events[j] = events[j], events[i]
		}
	}
	return events, nil
}

func (s *MemoryStepFunctionsStore) AppendHistory(execARN string, event HistoryEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.executions[execARN]
	if !ok {
		return &SFNError{Code: "ExecutionDoesNotExist", Message: "Execution does not exist", Status: 400}
	}
	event.ID = int64(len(e.History)) + 1
	if len(e.History) > 0 {
		event.PreviousEventID = e.History[len(e.History)-1].ID
	}
	e.History = append(e.History, event)
	return nil
}

// FinalizeExecution marks an execution as SUCCEEDED or FAILED with output/error.
func (s *MemoryStepFunctionsStore) FinalizeExecution(arn string, status ExecutionStatus, output, errCode, cause string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.executions[arn]
	if !ok {
		return &SFNError{Code: "ExecutionDoesNotExist", Message: "Execution does not exist", Status: 400}
	}
	t := now()
	e.Status = status
	e.StopDate = &t
	e.Output = output
	e.Error = errCode
	e.Cause = cause
	return nil
}

// ─── Activities ───────────────────────────────────────────────────────────────

func (s *MemoryStepFunctionsStore) CreateActivity(act *Activity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.activities[act.ARN]; exists {
		return &SFNError{Code: "ActivityAlreadyExists", Message: fmt.Sprintf("Activity already exists: '%s'", act.Name), Status: 400}
	}
	if len(s.activities) >= 10000 {
		return &SFNError{Code: "ActivityLimitExceeded", Message: "Maximum number of activities reached", Status: 400}
	}
	cp := *act
	if cp.Tags != nil {
		s.tags[cp.ARN] = cloneStringMap(cp.Tags)
	}
	s.activities[cp.ARN] = &cp
	return nil
}

func (s *MemoryStepFunctionsStore) DeleteActivity(arn string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.activities[arn]; !ok {
		return &SFNError{Code: "ActivityDoesNotExist", Message: "Activity does not exist", Status: 400}
	}
	delete(s.activities, arn)
	delete(s.tags, arn)
	return nil
}

func (s *MemoryStepFunctionsStore) GetActivity(arn string) (*Activity, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.activities[arn]
	if !ok {
		return nil, &SFNError{Code: "ActivityDoesNotExist", Message: "Activity does not exist", Status: 400}
	}
	cp := *a
	return &cp, nil
}

func (s *MemoryStepFunctionsStore) ListActivities() []*Activity {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Activity, 0, len(s.activities))
	for _, a := range s.activities {
		cp := *a
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func (s *MemoryStepFunctionsStore) AddTags(resourceARN string, tags map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.resourceExists(resourceARN) {
		return &SFNError{Code: "ResourceNotFound", Message: "Resource not found: " + resourceARN, Status: 400}
	}
	if _, ok := s.tags[resourceARN]; !ok {
		s.tags[resourceARN] = make(map[string]string)
	}
	total := len(s.tags[resourceARN]) + len(tags)
	for k := range tags {
		if _, exists := s.tags[resourceARN][k]; exists {
			total--
		}
	}
	if total > 50 {
		return &SFNError{Code: "TooManyTags", Message: "Maximum number of tags exceeded", Status: 400}
	}
	for k, v := range tags {
		s.tags[resourceARN][k] = v
	}
	return nil
}

func (s *MemoryStepFunctionsStore) RemoveTags(resourceARN string, keys []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.tags[resourceARN]; ok {
		for _, k := range keys {
			delete(t, k)
		}
	}
	return nil
}

func (s *MemoryStepFunctionsStore) ListTags(resourceARN string) map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneStringMap(s.tags[resourceARN])
}

func (s *MemoryStepFunctionsStore) resourceExists(arn string) bool {
	if _, ok := s.machines[arn]; ok {
		return true
	}
	if _, ok := s.activities[arn]; ok {
		return true
	}
	// Check aliases (arn ends with :aliasName)
	smARN, _, err := parseAliasARN(arn)
	if err == nil {
		if sm, ok := s.machines[smARN]; ok {
			_, aliasName, _ := parseAliasARN(arn)
			if _, ok := sm.Aliases[aliasName]; ok {
				return true
			}
		}
	}
	return false
}

// ─── Admin ────────────────────────────────────────────────────────────────────

func (s *MemoryStepFunctionsStore) Reset(ctx context.Context) {
	s.mu.Lock()
	s.machines = make(map[string]*StateMachine)
	s.nameToARN = make(map[string]string)
	s.executions = make(map[string]*Execution)
	s.activities = make(map[string]*Activity)
	s.tags = make(map[string]map[string]string)
	s.mu.Unlock()
}

type sfnSnapshot struct {
	Machines   map[string]*StateMachine `json:"machines"`
	Executions map[string]*Execution   `json:"executions"`
	Activities map[string]*Activity    `json:"activities"`
	Tags       map[string]map[string]string `json:"tags"`
}

func (s *MemoryStepFunctionsStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.machines) == 0, nil
}

func (s *MemoryStepFunctionsStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := sfnSnapshot{
		Machines:   s.machines,
		Executions: s.executions,
		Activities: s.activities,
		Tags:       s.tags,
	}
	return json.NewEncoder(w).Encode(snap)
}

func (s *MemoryStepFunctionsStore) Restore(_ context.Context, r io.Reader) error {
	var snap sfnSnapshot
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.machines = snap.Machines
	if s.machines == nil {
		s.machines = make(map[string]*StateMachine)
	}
	s.nameToARN = make(map[string]string, len(s.machines))
	for _, sm := range s.machines {
		s.nameToARN[sfnNameKey(sm.ARN, sm.Name)] = sm.ARN
		if sm.Versions == nil {
			sm.Versions = make(map[int64]*StateMachineVersion)
		}
		if sm.Aliases == nil {
			sm.Aliases = make(map[string]*StateMachineAlias)
		}
	}
	s.executions = snap.Executions
	if s.executions == nil {
		s.executions = make(map[string]*Execution)
	}
	s.activities = snap.Activities
	if s.activities == nil {
		s.activities = make(map[string]*Activity)
	}
	s.tags = snap.Tags
	if s.tags == nil {
		s.tags = make(map[string]map[string]string)
	}
	return nil
}

// ─── Errors ───────────────────────────────────────────────────────────────────

type SFNError struct {
	Code    string
	Message string
	Status  int
}

func (e *SFNError) Error() string { return e.Code + ": " + e.Message }

// AsSFNError returns the SFNError if err is one, nil otherwise.
func AsSFNError(err error) *SFNError {
	var e *SFNError
	if errors.As(err, &e) {
		return e
	}
	return nil
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func cloneSM(sm *StateMachine) *StateMachine {
	cp := *sm
	cp.Tags = cloneStringMap(sm.Tags)
	cp.Versions = make(map[int64]*StateMachineVersion, len(sm.Versions))
	for k, v := range sm.Versions {
		vcp := *v
		cp.Versions[k] = &vcp
	}
	cp.Aliases = make(map[string]*StateMachineAlias, len(sm.Aliases))
	for k, v := range sm.Aliases {
		acp := *v
		cp.Aliases[k] = &acp
	}
	return &cp
}

func cloneStringMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// ParseVersionARN parses a state machine version ARN into its base SM ARN and version number.
func ParseVersionARN(versionARN string) (smARN string, version int64, err error) {
	return parseVersionARN(versionARN)
}

func parseVersionARN(versionARN string) (smARN string, version int64, err error) {
	// Version ARN: arn:...:stateMachine:Name:N
	parts := strings.Split(versionARN, ":")
	if len(parts) < 2 {
		return "", 0, fmt.Errorf("invalid version ARN")
	}
	last := parts[len(parts)-1]
	var n int64
	if _, err := fmt.Sscanf(last, "%d", &n); err != nil {
		return "", 0, fmt.Errorf("not a version ARN")
	}
	return strings.Join(parts[:len(parts)-1], ":"), n, nil
}

func parseAliasARN(aliasARN string) (smARN string, aliasName string, err error) {
	// Alias ARN: arn:...:stateMachine:Name:AliasName
	parts := strings.Split(aliasARN, ":")
	if len(parts) < 2 {
		return "", "", fmt.Errorf("invalid alias ARN")
	}
	last := parts[len(parts)-1]
	// If last segment is a number, it's a version, not an alias
	var n int64
	if _, scanErr := fmt.Sscanf(last, "%d", &n); scanErr == nil {
		return "", "", fmt.Errorf("ARN is a version, not an alias")
	}
	return strings.Join(parts[:len(parts)-1], ":"), last, nil
}

func validateRoutingConfig(routing []RoutingConfig, sm *StateMachine) error {
	if len(routing) > 2 {
		return &SFNError{Code: "ValidationException", Message: "Routing configuration must have at most 2 entries", Status: 400}
	}
	total := 0
	for _, rc := range routing {
		total += rc.Weight
		// Verify version exists
		_, versionNum, err := parseVersionARN(rc.StateMachineVersionARN)
		if err != nil {
			return &SFNError{Code: "ResourceNotFound", Message: "Version not found: " + rc.StateMachineVersionARN, Status: 400}
		}
		if _, ok := sm.Versions[versionNum]; !ok {
			return &SFNError{Code: "ResourceNotFound", Message: "Version not found: " + rc.StateMachineVersionARN, Status: 400}
		}
	}
	if len(routing) > 0 && total != 100 {
		return &SFNError{Code: "ValidationException", Message: "Routing configuration weights must sum to 100", Status: 400}
	}
	return nil
}
