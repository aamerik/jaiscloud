//go:build gcp_persistence

package secretmanager

import (
	"bytes"
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	gcpstore "jaiscloud/internal/gcp/store"
	"jaiscloud/internal/store"
)

// TestPostgresStoreSnapshotPreservesCMEK verifies that a secret version's
// per-version CMEK state (kms_key_name + wrapped_dek) survives a Postgres
// Snapshot/Restore round trip. Without the wrapped DEK the version cannot be
// decrypted, and without both fields it would report no
// replicationStatus.customerManagedEncryption (DC44) after a restore.
func TestPostgresStoreSnapshotPreservesCMEK(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres snapshot test")
	}
	ctx := context.Background()

	pg, err := store.NewPostgresResourceStore(ctx, dsn, "gcp")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pg.Close()
	if err := store.RunMigrations(ctx, pg.Pool(), "gcp", gcpstore.MigrationFS, "gcp"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	s := NewPostgresStore(pg.Pool())
	s.Reset(ctx)

	const key = "projects/proj/locations/global/keyRings/r/cryptoKeys/k"
	created := time.Now().UTC().Truncate(time.Microsecond)
	userManaged := &Replication{UserManaged: &UserManagedReplication{Replicas: []Replica{
		{Location: "us-east1", CustomerManagedEncryption: &CustomerManagedEncryption{
			KmsKeyName: "projects/proj/locations/us-east1/keyRings/r/cryptoKeys/k",
		}},
		{Location: "us-central1"},
	}}}
	umSecret := Secret{ID: "um", CreateTime: created, NextVer: 1}
	umSecret.SetReplication(userManaged)
	if err := s.CreateSecret(ctx, "proj", "s", Secret{
		ID: "s", CreateTime: created, NextVer: 2, KmsKeyName: key,
	}); err != nil {
		t.Fatalf("create secret: %v", err)
	}
	if err := s.CreateSecret(ctx, "proj", "um", umSecret); err != nil {
		t.Fatalf("create user-managed secret: %v", err)
	}
	wrapped := []byte("1:ciphertext")
	if err := s.CreateVersion(ctx, "proj", Version{
		SecretID: "s", VersionID: "1", State: "ENABLED", CreateTime: created,
		Data: "aGk=", KmsKeyName: key, WrappedDEK: wrapped,
	}); err != nil {
		t.Fatalf("create version: %v", err)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if err := s.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}

	sec, err := s.GetSecret(ctx, "proj", "s")
	if err != nil {
		t.Fatalf("get secret after restore: %v", err)
	}
	if sec.KmsKeyName != key {
		t.Fatalf("secret kms_key_name = %q, want %q", sec.KmsKeyName, key)
	}
	restoredUM, err := s.GetSecret(ctx, "proj", "um")
	if err != nil {
		t.Fatalf("get user-managed secret after restore: %v", err)
	}
	if !reflect.DeepEqual(restoredUM.Replication, userManaged) {
		t.Fatalf("restored replication = %#v, want %#v", restoredUM.Replication, userManaged)
	}
	if restoredUM.KmsKeyName != userManaged.WrapKey() {
		t.Fatalf("restored effective key = %q, want %q", restoredUM.KmsKeyName, userManaged.WrapKey())
	}
	v, err := s.GetVersion(ctx, "proj", "s", "1")
	if err != nil {
		t.Fatalf("get version after restore: %v", err)
	}
	if v.KmsKeyName != key {
		t.Fatalf("version kms_key_name = %q, want %q", v.KmsKeyName, key)
	}
	if !bytes.Equal(v.WrappedDEK, wrapped) {
		t.Fatalf("version wrapped_dek = %q, want %q", v.WrappedDEK, wrapped)
	}
	if v.Data != "aGk=" {
		t.Fatalf("version data = %q, want %q", v.Data, "aGk=")
	}
}
