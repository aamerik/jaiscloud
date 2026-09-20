//go:build gcp_persistence

package paritygrpc_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	logging "cloud.google.com/go/logging/apiv2"
	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"
	"cloud.google.com/go/logging/logadmin"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// loggingWrite writes one entry through the official LoggingServiceV2 client.
//
// The RPC is issued directly rather than through the high-level Logger: that
// client appends a one-time instrumentation entry and sets PartialSuccess,
// which this emulator intentionally rejects as Unimplemented, so the first
// high-level write in a process always fails. WriteLogEntries is the same wire
// call the high-level client makes, minus the unsupported flag.
func (d *driver) loggingWrite(ctx context.Context, logName, payload string) error {
	conn, err := dialGRPC(d.target("LOGGING_EMULATOR_HOST"))
	if err != nil {
		return fmt.Errorf("logging dial: %w", err)
	}
	defer conn.Close()
	client, err := logging.NewClient(ctx, option.WithGRPCConn(conn))
	if err != nil {
		return fmt.Errorf("logging client: %w", err)
	}
	defer client.Close()
	_, err = client.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
		Entries: []*loggingpb.LogEntry{{
			LogName: logName,
			Payload: &loggingpb.LogEntry_TextPayload{TextPayload: payload},
		}},
	})
	if err != nil {
		return fmt.Errorf("logging write: %w", err)
	}
	return nil
}

// loggingCount reads entries back through the official logadmin client, which
// is the read side that exposes Client.Entries.
func (d *driver) loggingCount(ctx context.Context, logName string) (int, error) {
	conn, err := dialGRPC(d.target("LOGGING_EMULATOR_HOST"))
	if err != nil {
		return 0, fmt.Errorf("logadmin dial: %w", err)
	}
	defer conn.Close()
	client, err := logadmin.NewClient(ctx, projectID(), option.WithGRPCConn(conn))
	if err != nil {
		return 0, fmt.Errorf("logadmin client: %w", err)
	}
	defer client.Close()

	it := client.Entries(ctx, logadmin.Filter(fmt.Sprintf("logName=%q", logName)))
	n := 0
	for {
		if _, err := it.Next(); err != nil {
			if err == iterator.Done {
				break
			}
			return 0, fmt.Errorf("logadmin next: %w", err)
		}
		n++
	}
	return n, nil
}

// seedLogging writes a log entry and returns checks that read it back after a
// restart and assert its absence after a reset.
func seedLogging(d *driver, suffix string) (func() error, func() error, func() error, error) {
	logName := fmt.Sprintf("projects/%s/logs/%s", projectID(), "parity-log-"+suffix)
	payload := "parity-seed-" + suffix

	seeded := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		n, err := d.loggingCount(ctx, logName)
		if err != nil {
			return fmt.Errorf("logging read after seed: %w", err)
		}
		if n == 0 {
			return errors.New("log entry not visible immediately after write")
		}
		return nil
	}
	survived := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		n, err := d.loggingCount(ctx, logName)
		if err != nil {
			return fmt.Errorf("logging read after restart: %w", err)
		}
		if n == 0 {
			return errors.New("log entry not present after restart")
		}
		return nil
	}
	cleared := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		n, err := d.loggingCount(ctx, logName)
		if err != nil {
			return fmt.Errorf("logging read after reset: %w", err)
		}
		if n != 0 {
			return fmt.Errorf("log entries still present after reset: %d", n)
		}
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := d.loggingWrite(ctx, logName, payload); err != nil {
		return nil, nil, nil, err
	}
	if err := seeded(); err != nil {
		return nil, nil, nil, err
	}
	return seeded, survived, cleared, nil
}
