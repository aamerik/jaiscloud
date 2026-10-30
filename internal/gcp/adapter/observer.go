package gcp

import "context"

// GCPAdapter implements gateway.AccountObserver so the gateway can report every
// served (project, service) pair. The sink is optional: with no observer
// installed the method is a no-op and the adapter behaves exactly as before.
//
// The GCP binary wires it to the Resource Manager core, which keeps the
// observed-project registry the console uses to discover projects a workload
// wrote to but never registered (see internal/gcp/service/resourcemanager/
// observed.go).

// SetAccountObserver installs the sink the gateway calls for every served
// request. Passing nil disables observation.
func (a *GCPAdapter) SetAccountObserver(fn func(ctx context.Context, account, service string)) {
	a.accountObserver = fn
}

// ObserveAccount implements gateway.AccountObserver.
func (a *GCPAdapter) ObserveAccount(ctx context.Context, account, service string) {
	if a.accountObserver != nil && account != "" {
		a.accountObserver(ctx, account, service)
	}
}
