// Package testutil provides shared test utilities and GCP client factories.
//
// Client factories are configured to target the jaiscloud-gcp emulator
// endpoints (REST http://localhost:8080, gRPC localhost:8081).
package testutil

import (
	"context"
	"os"
	"strings"

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
// emulator. Reads STORAGE_EMULATOR_HOST_GRPC (e.g. localhost:8081) — the
// gRPC-specific var, distinct from STORAGE_EMULATOR_HOST which targets the REST
// JSON API. The endpoint is dialed explicitly because the storage gRPC client
// only honors STORAGE_EMULATOR_HOST_GRPC natively.
func StorageGRPCClient(ctx context.Context) *storage.Client {
	host := os.Getenv("STORAGE_EMULATOR_HOST_GRPC")
	if host == "" {
		host = "localhost:8081"
	}
	host = stripScheme(host)
	client, err := storage.NewGRPCClient(ctx,
		option.WithEndpoint(host),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
		storage.WithDisabledClientMetrics(),
	)
	if err != nil {
		panic("failed to create gRPC storage client: " + err.Error())
	}
	return client
}

// DatastoreClient returns a Datastore client configured for the emulator.
// Relies on the datastore SDK's native DATASTORE_EMULATOR_HOST env handling
// (e.g. localhost:8081), so the CI env block must set it.
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

// stripScheme removes a leading "scheme://" from an endpoint so it can be used
// as a bare grpc target.
func stripScheme(host string) string {
	if i := strings.Index(host, "://"); i >= 0 {
		return host[i+3:]
	}
	return host
}
