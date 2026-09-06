// Package testutil provides shared test utilities and GCP client factories.
//
// Client factories are configured to target the jaiscloud-gcp emulator
// endpoints (REST http://localhost:8080, gRPC localhost:8081).
package testutil

import (
	"context"
	"os"

	"cloud.google.com/go/datastore"
	logging "cloud.google.com/go/logging/apiv2"
	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ProjectID returns the GCP project ID from the environment or a default.
func ProjectID() string {
	if p := os.Getenv("GCP_EMULATOR_PROJECT"); p != "" {
		return p
	}
	return "test-project"
}

// StorageGRPCClient returns a Cloud Storage v2 gRPC client configured for the
// emulator. Reads STORAGE_EMULATOR_HOST (e.g. localhost:8081).
func StorageGRPCClient(ctx context.Context) *storage.Client {
	client, err := storage.NewGRPCClient(ctx)
	if err != nil {
		panic("failed to create gRPC storage client: " + err.Error())
	}
	return client
}

// DatastoreClient returns a Datastore client configured for the emulator.
// Reads DATASTORE_EMULATOR_HOST (e.g. localhost:8081).
func DatastoreClient(ctx context.Context) *datastore.Client {
	client, err := datastore.NewClient(ctx, ProjectID())
	if err != nil {
		panic("failed to create datastore client: " + err.Error())
	}
	return client
}

// LoggingClient returns a Cloud Logging (v2) client configured for the emulator.
// Reads LOGGING_EMULATOR_HOST (e.g. localhost:8081).
func LoggingClient(ctx context.Context) *logging.Client {
	host := os.Getenv("LOGGING_EMULATOR_HOST")
	if host == "" {
		host = "localhost:8081"
	}
	conn, err := grpc.NewClient(host, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic("failed to create gRPC connection: " + err.Error())
	}
	client, err := logging.NewClient(ctx, option.WithGRPCConn(conn))
	if err != nil {
		panic("failed to create logging client: " + err.Error())
	}
	return client
}
