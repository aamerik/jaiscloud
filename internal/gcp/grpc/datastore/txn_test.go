package datastore

import (
	"context"
	"testing"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// readTxn builds ReadOptions carrying an explicit transaction selector, as the
// real proto does (read_options.transaction).
func readTxn(txn []byte) *datastorepb.ReadOptions {
	return &datastorepb.ReadOptions{
		ConsistencyType: &datastorepb.ReadOptions_Transaction{Transaction: txn},
	}
}

func beginTxn(t *testing.T, client datastorepb.DatastoreClient) []byte {
	t.Helper()
	resp, err := client.BeginTransaction(context.Background(), &datastorepb.BeginTransactionRequest{ProjectId: "test"})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if len(resp.GetTransaction()) == 0 {
		t.Fatal("begin returned an empty transaction")
	}
	return resp.GetTransaction()
}

func upsertTask(t *testing.T, client datastorepb.DatastoreClient, name string, n int64) int64 {
	t.Helper()
	resp, err := client.Commit(context.Background(), &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Upsert{Upsert: entity(nameKey("Task", name), map[string]*datastorepb.Value{"n": intVal(n)})},
		}},
	})
	if err != nil {
		t.Fatalf("upsert %s: %v", name, err)
	}
	return resp.GetMutationResults()[0].GetVersion()
}

func lookupTask(t *testing.T, client datastorepb.DatastoreClient, name string, txn []byte) *datastorepb.EntityResult {
	t.Helper()
	req := &datastorepb.LookupRequest{ProjectId: "test", Keys: []*datastorepb.Key{nameKey("Task", name)}}
	if txn != nil {
		req.ReadOptions = readTxn(txn)
	}
	resp, err := client.Lookup(context.Background(), req)
	if err != nil {
		t.Fatalf("lookup %s: %v", name, err)
	}
	if len(resp.GetFound()) == 1 {
		return resp.GetFound()[0]
	}
	return nil
}

func txnCommit(t *testing.T, client datastorepb.DatastoreClient, txn []byte, muts ...*datastorepb.Mutation) (*datastorepb.CommitResponse, error) {
	t.Helper()
	return client.Commit(context.Background(), &datastorepb.CommitRequest{
		ProjectId:           "test",
		Mode:                datastorepb.CommitRequest_TRANSACTIONAL,
		TransactionSelector: &datastorepb.CommitRequest_Transaction{Transaction: txn},
		Mutations:           muts,
	})
}

func TestTransactionLookupThenCommitSucceeds(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()

	upsertTask(t, client, "a", 1)
	txn := beginTxn(t, client)

	found := lookupTask(t, client, "a", txn)
	if found == nil {
		t.Fatal("transactional lookup did not find the entity")
	}
	if found.GetVersion() != 1 {
		t.Fatalf("read version = %d, want 1", found.GetVersion())
	}

	resp, err := txnCommit(t, client, txn, &datastorepb.Mutation{
		Operation: &datastorepb.Mutation_Update{Update: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"n": intVal(2)})},
	})
	if err != nil {
		t.Fatalf("transactional commit: %v", err)
	}
	if resp.GetCommitTime() == nil {
		t.Fatal("transactional commit must set commit_time")
	}
	if got := resp.GetMutationResults()[0].GetVersion(); got != 2 {
		t.Fatalf("commit result version = %d, want 2", got)
	}
	if got := lookupTask(t, client, "a", nil).GetEntity().GetProperties()["n"].GetIntegerValue(); got != 2 {
		t.Fatalf("n after commit = %d, want 2", got)
	}
}

func TestTransactionReadConflictAbortsAndNothingApplied(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()

	upsertTask(t, client, "a", 1)
	txn := beginTxn(t, client)
	if found := lookupTask(t, client, "a", txn); found == nil {
		t.Fatal("transactional lookup did not find the entity")
	}

	// A concurrent non-transactional writer modifies the entity after the read.
	upsertTask(t, client, "a", 2)

	_, err := txnCommit(t, client, txn, &datastorepb.Mutation{
		Operation: &datastorepb.Mutation_Update{Update: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"n": intVal(99)})},
	})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("commit err = %v, want Aborted", err)
	}

	// The aborted transaction's write must not have applied.
	if got := lookupTask(t, client, "a", nil).GetEntity().GetProperties()["n"].GetIntegerValue(); got != 2 {
		t.Fatalf("n = %d after aborted commit, want 2", got)
	}

	// Any commit attempt terminates the transaction: reusing the token is now
	// invalid (real Datastore requires a fresh BeginTransaction to retry).
	if _, err := txnCommit(t, client, txn, &datastorepb.Mutation{
		Operation: &datastorepb.Mutation_Update{Update: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"n": intVal(3)})},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("reused transaction err = %v, want InvalidArgument", err)
	}
}

func TestTransactionRollbackDiscards(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()
	_ = ctx

	txn := beginTxn(t, client)
	if _, err := client.Rollback(ctx, &datastorepb.RollbackRequest{ProjectId: "test", Transaction: txn}); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	if _, err := txnCommit(t, client, txn, &datastorepb.Mutation{
		Operation: &datastorepb.Mutation_Upsert{Upsert: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"n": intVal(1)})},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("commit after rollback err = %v, want InvalidArgument", err)
	}

	// Rollback is idempotent even for an unknown token.
	if _, err := client.Rollback(ctx, &datastorepb.RollbackRequest{ProjectId: "test", Transaction: []byte("nonexistent")}); err != nil {
		t.Fatalf("rollback unknown: %v", err)
	}
}

func TestTransactionPreconditionMismatchFailsPrecondition(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()

	upsertTask(t, client, "a", 1)
	txn := beginTxn(t, client)

	_, err := txnCommit(t, client, txn, &datastorepb.Mutation{
		Operation:                 &datastorepb.Mutation_Update{Update: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"n": intVal(99)})},
		ConflictDetectionStrategy: &datastorepb.Mutation_BaseVersion{BaseVersion: 12345},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("commit err = %v, want FailedPrecondition", err)
	}
	if got := lookupTask(t, client, "a", nil).GetEntity().GetProperties()["n"].GetIntegerValue(); got != 1 {
		t.Fatalf("n = %d after precondition failure, want 1", got)
	}
}

func TestTransactionalWithoutTransactionIsInvalid(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	_, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mode:      datastorepb.CommitRequest_TRANSACTIONAL,
		// no transaction
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Upsert{Upsert: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"n": intVal(1)})},
		}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err = %v, want InvalidArgument", err)
	}
}

func TestUnknownTransactionCommitIsInvalid(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()

	_, err := txnCommit(t, client, []byte("does-not-exist"), &datastorepb.Mutation{
		Operation: &datastorepb.Mutation_Upsert{Upsert: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"n": intVal(1)})},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err = %v, want InvalidArgument", err)
	}
}

func TestNonTransactionalCommitUnchanged(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	// No mode, no transaction: the historical non-transactional path.
	resp, err := client.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: "test",
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Insert{Insert: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"n": intVal(1)})},
		}},
	})
	if err != nil {
		t.Fatalf("non-transactional commit: %v", err)
	}
	if resp.GetCommitTime() != nil {
		t.Fatal("non-transactional commit must not set commit_time")
	}
	if got := resp.GetMutationResults()[0].GetVersion(); got != 1 {
		t.Fatalf("version = %d, want 1", got)
	}
}

// TestTwoTransactionsSameEntityOneAborts is the no-lost-update guarantee at the
// service level: two transactions that both read the same version race to
// commit; one wins and the other aborts.
func TestTwoTransactionsSameEntityOneAborts(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()

	upsertTask(t, client, "a", 1)
	txnA := beginTxn(t, client)
	txnB := beginTxn(t, client)
	if lookupTask(t, client, "a", txnA) == nil || lookupTask(t, client, "a", txnB) == nil {
		t.Fatal("both transactions must observe the entity")
	}

	if _, err := txnCommit(t, client, txnA, &datastorepb.Mutation{
		Operation: &datastorepb.Mutation_Upsert{Upsert: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"n": intVal(100)})},
	}); err != nil {
		t.Fatalf("txn A commit: %v", err)
	}
	if _, err := txnCommit(t, client, txnB, &datastorepb.Mutation{
		Operation: &datastorepb.Mutation_Upsert{Upsert: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"n": intVal(200)})},
	}); status.Code(err) != codes.Aborted {
		t.Fatalf("txn B commit err = %v, want Aborted", err)
	}

	// A's write survived; B's did not (no lost update).
	if got := lookupTask(t, client, "a", nil).GetEntity().GetProperties()["n"].GetIntegerValue(); got != 100 {
		t.Fatalf("n = %d, want 100 (B must not have overwritten A)", got)
	}
}

func TestTransactionLookupMissingThenCreatedAborts(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()

	txn := beginTxn(t, client)
	if got := lookupTask(t, client, "a", txn); got != nil {
		t.Fatal("entity should not exist yet")
	}

	// A concurrent create of the key read as missing.
	upsertTask(t, client, "a", 1)

	if _, err := txnCommit(t, client, txn, &datastorepb.Mutation{
		Operation: &datastorepb.Mutation_Upsert{Upsert: entity(nameKey("Task", "a"), map[string]*datastorepb.Value{"n": intVal(99)})},
	}); status.Code(err) != codes.Aborted {
		t.Fatalf("commit err = %v, want Aborted", err)
	}
}

func TestTransactionRunQueryRecordsReads(t *testing.T) {
	client, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	upsertTask(t, client, "a", 1)
	upsertTask(t, client, "b", 1)

	txn := beginTxn(t, client)
	q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "Task"}}}
	if _, err := client.RunQuery(ctx, &datastorepb.RunQueryRequest{
		ProjectId:   "test",
		ReadOptions: readTxn(txn),
		QueryType:   &datastorepb.RunQueryRequest_Query{Query: q},
	}); err != nil {
		t.Fatalf("transactional runquery: %v", err)
	}

	// Modify one of the entities the query returned.
	upsertTask(t, client, "b", 2)

	// A commit with no mutations still validates the read-set and must abort.
	if _, err := txnCommit(t, client, txn); status.Code(err) != codes.Aborted {
		t.Fatalf("commit err = %v, want Aborted", err)
	}
}
