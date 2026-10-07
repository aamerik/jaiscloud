//go:build gcp_parity

package gcpparity

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/proto"
)

// protoMessage is a short alias so case files can name the gRPC response type
// without importing google.golang.org/protobuf/proto themselves.
type protoMessage = proto.Message

// Step is one step of a service's canonical flow. At most one of Mutate and
// (GRPC + REST) is meaningful:
//
//   - Mutate, when set, performs a state change (create/update/delete) before
//     the next read. It is nil for a pure read step.
//   - GRPC and REST, when both set, read the SAME resource over each transport;
//     the harness normalizes both and diffs them.
//
// A step may set Mutate and GRPC/REST together, e.g. a mutation followed by an
// immediate cross-transport read of the result.
type Step struct {
	Op     string
	Mutate func(ctx context.Context, e *Env) error
	GRPC   func(ctx context.Context, e *Env) (proto.Message, error)
	REST   func(ctx context.Context, e *Env) (json.RawMessage, error)
	// Scope, when true, filters both sides' named-object arrays to the run
	// suffix before diffing. A list step sets it so resources left behind by
	// other suites on a shared emulator cannot pollute the comparison and so a
	// transport-specific field cannot reorder the two sides.
	Scope bool
}

// Scenario is one dual service's canonical create → get → list → mutate →
// delete flow, ordered so each read observes state the preceding mutate steps
// established.
type Scenario struct {
	Service string
	Steps   []Step
}

// Registry returns every service scenario in execution order.
func Registry() []Scenario {
	var s []Scenario
	s = append(s, secretManagerScenario())
	s = append(s, kmsScenario())
	s = append(s, pubSubScenario())
	s = append(s, schedulerScenario())
	s = append(s, tasksScenario())
	s = append(s, workflowsScenario())
	s = append(s, serviceUsageScenario())
	s = append(s, monitoringScenario())
	s = append(s, eventarcScenario())
	s = append(s, managedKafkaScenario())
	s = append(s, metastoreScenario())
	s = append(s, cloudRunScenario())
	s = append(s, loggingScenario())
	s = append(s, functionsScenario())
	return s
}

// ServiceResult is the outcome of one scenario run.
type ServiceResult struct {
	Service   string
	Findings  []Finding
	Steps     int
	Mutations int
	Compared  int
	// Aborted is set when a mutate step failed and the scenario could not
	// continue; Findings then carries the call_error.
	Aborted bool
}

// covered reports whether the scenario compared at least one resource across
// transports.
func (r ServiceResult) covered() bool { return r.Compared > 0 }

// runScenario executes one scenario against the emulator.
func runScenario(ctx context.Context, e *Env, sc Scenario, allowances []Allowance) ServiceResult {
	res := ServiceResult{Service: sc.Service}
	for _, st := range sc.Steps {
		res.Steps++
		if st.Mutate != nil {
			if err := st.Mutate(ctx, e); err != nil {
				res.Findings = append(res.Findings, Finding{
					Service: sc.Service, Op: st.Op, Kind: "call_error", Severity: "high",
					Actual: err.Error(),
				})
				res.Aborted = true
				return res
			}
			res.Mutations++
		}
		if st.GRPC == nil || st.REST == nil {
			continue
		}
		res.Compared++
		gm, gerr := st.GRPC(ctx, e)
		rb, rerr := st.REST(ctx, e)
		if gerr != nil || rerr != nil {
			res.Findings = append(res.Findings, Finding{
				Service: sc.Service, Op: st.Op, Kind: "call_error", Severity: "high",
				Expected: errString(rerr), Actual: errString(gerr),
			})
			continue
		}
		gb, err := e.GrpcBody(gm)
		if err != nil {
			res.Findings = append(res.Findings, Finding{Service: sc.Service, Op: st.Op, Kind: "call_error", Severity: "high", Actual: err.Error()})
			continue
		}
		norm := normalizeJSON
		if st.Scope {
			norm = func(b []byte) (json.RawMessage, error) { return normalizeScoped(b, e.Cfg.Suffix) }
		}
		gn, err := norm(gb)
		if err != nil {
			res.Findings = append(res.Findings, Finding{Service: sc.Service, Op: st.Op, Kind: "call_error", Severity: "high", Actual: err.Error()})
			continue
		}
		rn, err := norm(rb)
		if err != nil {
			res.Findings = append(res.Findings, Finding{Service: sc.Service, Op: st.Op, Kind: "call_error", Severity: "high", Expected: err.Error()})
			continue
		}
		res.Findings = append(res.Findings, compareNormalized(sc.Service, st.Op, rn, gn, allowances)...)
	}
	return res
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf("%v", err)
}
