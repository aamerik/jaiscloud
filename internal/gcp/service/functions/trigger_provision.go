package functions

import (
	"context"
	"log/slog"

	"jaiscloud/internal/gcp/eventing"
	functionsstore "jaiscloud/internal/gcp/store/functions"
)

// ensureTrigger materializes the backing Eventarc trigger (and its transport
// Pub/Sub subscription) for a function's Pub/Sub event trigger, returning a
// copy of f with EventTrigger.Trigger/Subscription populated. It is a no-op for
// a nil provisioner, an HTTP-triggered function, or a non-Pub/Sub event source.
//
// A provisioning failure is non-fatal: the function still receives events via
// the direct delivery engine (FD4); it just has no user-configurable
// dead-letter subscription, so the failure is logged rather than returned.
func (s *Service) ensureTrigger(ctx context.Context, project, location, id string, f functionsstore.Function) functionsstore.Function {
	if s.triggerProvisioner == nil || !isPubSubTrigger(f.EventTrigger) {
		return f
	}
	triggerName, sub, err := s.triggerProvisioner.EnsureFunctionTrigger(ctx, eventing.FunctionTriggerSpec{
		Project:    project,
		Location:   location,
		FunctionID: id,
		Topic:      f.EventTrigger.Resource,
	})
	if err != nil {
		slog.Warn("functions: provision backing event trigger", "function", id, "err", err)
		return f
	}
	// Deep-copy the trigger: the memory store shares the pointer, so mutating in
	// place would silently edit stored state without the Postgres path seeing
	// the same change.
	et := *f.EventTrigger
	et.Trigger = triggerName
	et.Subscription = sub
	f.EventTrigger = &et
	return f
}

// persistTriggerFields stores the provisioned trigger name/subscription on the
// function row, tolerating a missing function (a concurrent delete).
func (s *Service) persistTriggerFields(ctx context.Context, project, location, id string, f functionsstore.Function) {
	if f.EventTrigger == nil {
		return
	}
	_, err := s.functions.UpdateFunctionAtomic(ctx, project, location, id, func(cur functionsstore.Function) (functionsstore.Function, error) {
		if cur.EventTrigger == nil {
			return cur, nil
		}
		et := *cur.EventTrigger
		et.Trigger = f.EventTrigger.Trigger
		et.Subscription = f.EventTrigger.Subscription
		cur.EventTrigger = &et
		return cur, nil
	})
	if err != nil {
		slog.Warn("functions: persist backing trigger", "function", id, "err", err)
	}
}

// deleteTrigger removes a function's backing Eventarc trigger and its transport
// subscription, tolerating a nil provisioner.
func (s *Service) deleteTrigger(ctx context.Context, project, location, id string) {
	if s.triggerProvisioner == nil {
		return
	}
	if err := s.triggerProvisioner.DeleteFunctionTrigger(ctx, project, location, id); err != nil {
		slog.Warn("functions: delete backing event trigger", "function", id, "err", err)
	}
}

// isPubSubTrigger reports whether a trigger observes a Pub/Sub topic. The
// emulator materializes a backing Eventarc trigger only for Pub/Sub-triggered
// functions: its transport subscription carries the Pub/Sub dead-letter surface.
// GCS triggers are delivered directly by the storage producer and have no
// backing subscription.
func isPubSubTrigger(et *functionsstore.EventTrigger) bool {
	if et == nil {
		return false
	}
	return eventing.NormalizeEventType(et.EventType) == eventing.TypePubSubPublish
}

// triggerEqual reports whether two event triggers are materially the same
// (event type, source resource, and retry policy), ignoring the provisioning
// outputs (Trigger/Subscription).
func triggerEqual(a, b *functionsstore.EventTrigger) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.EventType == b.EventType && a.Resource == b.Resource && a.Retries() == b.Retries()
}
