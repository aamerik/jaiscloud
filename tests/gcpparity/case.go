//go:build gcp_parity

package gcpparity

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/proto"
)

// protoMessage is a short alias so case files can name the gRPC response type
// without importing google.golang.org/protobuf/proto themselves.
type protoMessage = proto.Message

// Step is one step of a service's canonical flow. At most one of Mutation,
// Mutate and (GRPC + REST) is meaningful:
//
//   - Mutation, when set, cross-diffs one mutation's response across transports
//     (AUD3-12): each transport performs the same logical mutation on its own
//     twin resource and the two response bodies are diffed.
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
	// Mutation, when set, is a self-contained head-to-head diff of a mutation
	// response across transports (see MutationParity).
	Mutation *MutationParity
	GRPC     func(ctx context.Context, e *Env) (proto.Message, error)
	REST     func(ctx context.Context, e *Env) (json.RawMessage, error)
	// Scope, when true, filters both sides' named-object arrays to the run
	// suffix before diffing. A list step sets it so resources left behind by
	// other suites on a shared emulator cannot pollute the comparison and so a
	// transport-specific field cannot reorder the two sides.
	Scope bool
	// Project, when set, rewrites each transport's raw body into one canonical
	// logical form before normalization. It exists for a service whose REST JSON
	// and gRPC protojson encode the same logical data differently (Datastore's
	// Value union renders the NullValue enum as its Discovery name over REST and
	// as JSON null through protojson). The transform is applied to both sides, so
	// a genuine logical difference survives it.
	Project func(json.RawMessage) (json.RawMessage, error)
}

// MutationParity is a self-contained cross-transport diff of one mutation's
// response (AUD3-12). Each transport performs the same logical mutation on its
// own twin resource — named through Env.Resource while Env.Side is set to
// "grpc"/"rest", so the two writes cannot collide — and the two response bodies
// are normalized (with the twin token folded back out) and diffed with the same
// engine as a read. This is the symmetric form: Create is diffed against Create
// and Update against Update, not against a read from the other transport.
//
// Cleanup, when set, removes the twin after each side's mutation; it runs with
// the matching Side set and is best-effort (a cleanup failure does not gate).
// It is nil for a resource the real API cannot delete (e.g. a KMS key ring).
type MutationParity struct {
	GRPC    func(ctx context.Context, e *Env) (proto.Message, error)
	REST    func(ctx context.Context, e *Env) (json.RawMessage, error)
	Cleanup func(ctx context.Context, e *Env) error
	// Project is the mutation-response analogue of Step.Project: it canonicalizes
	// each transport's raw body after the twin token is folded out and before
	// normalization.
	Project func(json.RawMessage) (json.RawMessage, error)
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
	s = append(s, datastoreScenario())
	s = append(s, firestoreScenario())
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
	for i, st := range sc.Steps {
		res.Steps++
		if st.Mutation != nil {
			res.Mutations++
			res.Compared++
			e.StepTag = fmt.Sprintf("s%d", i)
			res.Findings = append(res.Findings, runMutationParity(ctx, e, sc.Service, st.Op, st.Mutation, allowances)...)
			continue
		}
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
		gb, err = projectBody(st.Project, gb)
		if err != nil {
			res.Findings = append(res.Findings, Finding{Service: sc.Service, Op: st.Op, Kind: "call_error", Severity: "high", Actual: err.Error()})
			continue
		}
		rb, err = projectBody(st.Project, rb)
		if err != nil {
			res.Findings = append(res.Findings, Finding{Service: sc.Service, Op: st.Op, Kind: "call_error", Severity: "high", Expected: err.Error()})
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

// runMutationParity drives one mutation over each transport on its own twin and
// diffs the two normalized response bodies. The gRPC side runs first, then the
// REST side; each side's Cleanup (if any) runs immediately after it so the
// twins do not outlive the step. Side is always restored so it cannot leak into
// a later read step.
func runMutationParity(ctx context.Context, e *Env, service, op string, mp *MutationParity, allowances []Allowance) []Finding {
	defer func() { e.Side, e.StepTag = "", "" }()

	e.Side = "grpc"
	gm, gerr := mp.GRPC(ctx, e)
	var gb json.RawMessage
	if gerr == nil {
		gb, gerr = e.GrpcBody(gm)
	}
	cleanupTwin(ctx, e, mp)

	e.Side = "rest"
	rb, rerr := mp.REST(ctx, e)
	cleanupTwin(ctx, e, mp)

	if gerr != nil || rerr != nil {
		return []Finding{{
			Service: service, Op: op, Kind: "call_error", Severity: "high",
			Expected: errString(rerr), Actual: errString(gerr),
		}}
	}
	gn, err := mutationNormalize(gb, mp.Project)
	if err != nil {
		return []Finding{{Service: service, Op: op, Kind: "call_error", Severity: "high", Actual: err.Error()}}
	}
	rn, err := mutationNormalize(rb, mp.Project)
	if err != nil {
		return []Finding{{Service: service, Op: op, Kind: "call_error", Severity: "high", Expected: err.Error()}}
	}
	return compareNormalized(service, op, rn, gn, allowances)
}

// projectBody applies an optional per-service projection (Step.Project /
// MutationParity.Project) to a raw response body. A nil projection or an empty
// body is returned unchanged.
func projectBody(project func(json.RawMessage) (json.RawMessage, error), raw json.RawMessage) (json.RawMessage, error) {
	if project == nil || len(bytes.TrimSpace(raw)) == 0 {
		return raw, nil
	}
	return project(raw)
}

// cleanupTwin removes one side's twin after its mutation, best-effort: a
// cleanup failure (e.g. a resource the real API cannot delete) must not gate.
func cleanupTwin(ctx context.Context, e *Env, mp *MutationParity) {
	if mp.Cleanup != nil {
		_ = mp.Cleanup(ctx, e)
	}
}
