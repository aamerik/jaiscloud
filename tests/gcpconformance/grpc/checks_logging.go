package grpcconformance

import (
	"context"
	"fmt"

	logging "cloud.google.com/go/logging/apiv2"
	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	monitoredres "google.golang.org/genproto/googleapis/api/monitoredres"
	ltype "google.golang.org/genproto/googleapis/logging/type"
)

// loggingChecks covers the Cloud Logging v2 surface
// (google.logging.v2.LoggingServiceV2) via the official generated
// cloud.google.com/go/logging/apiv2 client.
//
// The high-level cloud.google.com/go/logging client buffers Logger.Log and
// flushes WriteLogEntries asynchronously, which would make a check
// nondeterministic; the generated client issues each RPC synchronously (its
// list RPCs are client-side synchronous iterators), so every probe below is
// deterministic.
//
// The probes share one run-unique log: WriteLogEntries leaves the entry in place
// for ListLogEntries and ListLogs, and DeleteLog removes it last. Names are
// run-unique via cfg.ResourceName, so a long-lived emulator never sees
// cross-run collisions.
//
// TailLogEntries (bidirectional streaming) is intentionally not covered: the
// emulator serves a bounded, store-polling approximation whose delivery latency
// is derived from buffer_window, so there is no deterministic, non-flaky
// assertion available (a timing-dependent probe would either flake or have to
// treat a timeout as success). It stays unverified and therefore `limited` in
// the fidelity matrix.
func loggingChecks() []Check {
	return []Check{
		{Service: "logging", RPC: "WriteLogEntries", Method: "WriteLogEntries", KeyField: "entry round-trips (logName/textPayload)", Run: checkLoggingWrite},
		{Service: "logging", RPC: "ListLogEntries", Method: "ListLogEntries", KeyField: "entries[].logName/textPayload", Run: checkLoggingListEntries},
		{Service: "logging", RPC: "ListLogs", Method: "ListLogs", KeyField: "logNames[]", Run: checkLoggingListLogs},
		{Service: "logging", RPC: "ListMonitoredResourceDescriptors", Method: "ListMonitoredResourceDescriptors", KeyField: "resourceDescriptors[].type/displayName/labels", Run: checkLoggingListDescriptors},
		{Service: "logging", RPC: "DeleteLog", Method: "DeleteLog", KeyField: "log absent from ListLogs after delete", Run: checkLoggingDelete},
	}
}

// newLoggingClient dials the emulator and returns the official generated
// Logging client. It mirrors the KMS/Secret Manager probes: an explicit
// insecure endpoint with authentication disabled.
func newLoggingClient(ctx context.Context, cfg Config) (*logging.Client, error) {
	return logging.NewClient(ctx,
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
}

func loggingParent(cfg Config) string { return "projects/" + cfg.Project }

func loggingLogID(cfg Config) string { return cfg.ResourceName("gcpc-grpc-log") }

func loggingLogName(cfg Config) string {
	return fmt.Sprintf("%s/logs/%s", loggingParent(cfg), loggingLogID(cfg))
}

// loggingEntryText is the run-unique text payload shared by the write and the
// reads that round-trip it.
func loggingEntryText(cfg Config) string { return cfg.ResourceName("gcpc-grpc-entry") }

// collectLogEntries drains a ListLogEntries iterator, failing on the first page
// error.
func collectLogEntries(it *logging.LogEntryIterator) ([]*loggingpb.LogEntry, error) {
	var entries []*loggingpb.LogEntry
	for {
		e, err := it.Next()
		if err == iterator.Done {
			return entries, nil
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
}

// Check 1: WriteLogEntries must accept a run-unique entry and make it
// immediately readable back through ListLogEntries (a synchronous round-trip,
// so the probe does not depend on any async flush).
func checkLoggingWrite(ctx context.Context, cfg Config) error {
	client, err := newLoggingClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	resp, err := client.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
		LogName: loggingLogName(cfg),
		Resource: &monitoredres.MonitoredResource{
			Type:   "global",
			Labels: map[string]string{"project_id": cfg.Project},
		},
		Entries: []*loggingpb.LogEntry{{
			LogName:  loggingLogName(cfg),
			Severity: ltype.LogSeverity_INFO,
			Payload:  &loggingpb.LogEntry_TextPayload{TextPayload: loggingEntryText(cfg)},
		}},
	})
	if err != nil {
		return fmt.Errorf("WriteLogEntries: %w", err)
	}
	if resp == nil {
		return fmt.Errorf("WriteLogEntries returned a nil response")
	}

	it := client.ListLogEntries(ctx, &loggingpb.ListLogEntriesRequest{
		ResourceNames: []string{loggingParent(cfg)},
		Filter:        fmt.Sprintf("logName=%q", loggingLogName(cfg)),
	})
	entries, err := collectLogEntries(it)
	if err != nil {
		return fmt.Errorf("round-trip ListLogEntries: %w", err)
	}
	if len(entries) != 1 {
		return fmt.Errorf("round-trip ListLogEntries returned %d entries, want 1", len(entries))
	}
	if got := entries[0].GetLogName(); got != loggingLogName(cfg) {
		return fmt.Errorf("round-tripped logName = %q, want %q", got, loggingLogName(cfg))
	}
	if got := entries[0].GetTextPayload(); got != loggingEntryText(cfg) {
		return fmt.Errorf("round-tripped textPayload = %q, want %q", got, loggingEntryText(cfg))
	}
	return nil
}

// Check 2: ListLogEntries filtered by the run-unique log name must return
// exactly the entry written by check 1, with its log name and payload intact.
func checkLoggingListEntries(ctx context.Context, cfg Config) error {
	client, err := newLoggingClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	it := client.ListLogEntries(ctx, &loggingpb.ListLogEntriesRequest{
		ResourceNames: []string{loggingParent(cfg)},
		Filter:        fmt.Sprintf("logName=%q", loggingLogName(cfg)),
	})
	entries, err := collectLogEntries(it)
	if err != nil {
		return fmt.Errorf("ListLogEntries: %w", err)
	}
	if len(entries) != 1 {
		return fmt.Errorf("ListLogEntries returned %d entries, want 1", len(entries))
	}
	e := entries[0]
	if e.GetLogName() != loggingLogName(cfg) {
		return fmt.Errorf("ListLogEntries logName = %q, want %q", e.GetLogName(), loggingLogName(cfg))
	}
	if e.GetTextPayload() != loggingEntryText(cfg) {
		return fmt.Errorf("ListLogEntries textPayload = %q, want %q", e.GetTextPayload(), loggingEntryText(cfg))
	}
	if e.GetSeverity() != ltype.LogSeverity_INFO {
		return fmt.Errorf("ListLogEntries severity = %v, want INFO", e.GetSeverity())
	}
	return nil
}

// Check 3: ListLogs under the project parent must include the run-unique log
// name created by check 1.
func checkLoggingListLogs(ctx context.Context, cfg Config) error {
	client, err := newLoggingClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	it := client.ListLogs(ctx, &loggingpb.ListLogsRequest{Parent: loggingParent(cfg)})
	for {
		name, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListLogs did not include %q", loggingLogName(cfg))
		}
		if err != nil {
			return fmt.Errorf("ListLogs: %w", err)
		}
		if name == loggingLogName(cfg) {
			return nil
		}
	}
}

// Check 4: ListMonitoredResourceDescriptors must return a non-empty catalog
// with the expected shape: every descriptor carries a type, display name and
// label set, and (unlike Cloud Monitoring) leaves the resource name unset.
func checkLoggingListDescriptors(ctx context.Context, cfg Config) error {
	client, err := newLoggingClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	it := client.ListMonitoredResourceDescriptors(ctx, &loggingpb.ListMonitoredResourceDescriptorsRequest{})
	count := 0
	for {
		d, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("ListMonitoredResourceDescriptors: %w", err)
		}
		count++
		if d.GetType() == "" {
			return fmt.Errorf("descriptor %d has an empty type", count)
		}
		if d.GetDisplayName() == "" {
			return fmt.Errorf("descriptor %q has an empty display_name", d.GetType())
		}
		if len(d.GetLabels()) == 0 {
			return fmt.Errorf("descriptor %q has no labels", d.GetType())
		}
		if d.GetName() != "" {
			return fmt.Errorf("Logging descriptor %q unexpectedly set name %q", d.GetType(), d.GetName())
		}
	}
	if count == 0 {
		return fmt.Errorf("ListMonitoredResourceDescriptors returned no descriptors")
	}
	return nil
}

// Check 5: DeleteLog must succeed, and a follow-up ListLogs must no longer
// list the log. Runs last because it removes the shared run-unique log.
func checkLoggingDelete(ctx context.Context, cfg Config) error {
	client, err := newLoggingClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err := client.DeleteLog(ctx, &loggingpb.DeleteLogRequest{LogName: loggingLogName(cfg)}); err != nil {
		return fmt.Errorf("DeleteLog: %w", err)
	}

	it := client.ListLogs(ctx, &loggingpb.ListLogsRequest{Parent: loggingParent(cfg)})
	for {
		name, err := it.Next()
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return fmt.Errorf("ListLogs after DeleteLog: %w", err)
		}
		if name == loggingLogName(cfg) {
			return fmt.Errorf("ListLogs still lists %q after DeleteLog", loggingLogName(cfg))
		}
	}
}
