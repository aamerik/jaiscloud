package firestore

import (
	"context"
	"io"
	"testing"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

func intPb(v int64) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_IntegerValue{IntegerValue: v}}
}

// TestUnaryDocumentCRUD exercises the unary document RPCs end to end through the
// generated client/server, so the transport handlers (and their transcoders) are
// covered without depending on an external integration suite.
func TestUnaryDocumentCRUD(t *testing.T) {
	client, _, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()
	const db = "projects/test/databases/(default)"

	created, err := client.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       listenParent,
		CollectionId: "things",
		DocumentId:   "d1",
		Document:     &firestorepb.Document{Fields: map[string]*firestorepb.Value{"n": intPb(1)}},
	})
	if err != nil {
		t.Fatalf("CreateDocument: %v", err)
	}
	if created.GetName() != listenParent+"/things/d1" {
		t.Fatalf("created name = %q", created.GetName())
	}

	got, err := client.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: listenParent + "/things/d1"})
	if err != nil {
		t.Fatalf("GetDocument: %v", err)
	}
	if got.GetFields()["n"].GetIntegerValue() != 1 {
		t.Fatalf("GetDocument n = %v", got.GetFields()["n"])
	}

	list, err := client.ListDocuments(ctx, &firestorepb.ListDocumentsRequest{
		Parent: listenParent, CollectionId: "things",
	})
	if err != nil {
		t.Fatalf("ListDocuments: %v", err)
	}
	if len(list.GetDocuments()) != 1 {
		t.Fatalf("ListDocuments returned %d documents", len(list.GetDocuments()))
	}

	ids, err := client.ListCollectionIds(ctx, &firestorepb.ListCollectionIdsRequest{Parent: listenParent})
	if err != nil {
		t.Fatalf("ListCollectionIds: %v", err)
	}
	if len(ids.GetCollectionIds()) != 1 || ids.GetCollectionIds()[0] != "things" {
		t.Fatalf("ListCollectionIds = %v", ids.GetCollectionIds())
	}

	stream, err := client.RunQuery(ctx, &firestorepb.RunQueryRequest{
		Parent: listenParent,
		QueryType: &firestorepb.RunQueryRequest_StructuredQuery{StructuredQuery: &firestorepb.StructuredQuery{
			From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "things"}},
		}},
	})
	if err != nil {
		t.Fatalf("RunQuery: %v", err)
	}
	var results int
	for {
		msg, rerr := stream.Recv()
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			t.Fatalf("RunQuery Recv: %v", rerr)
		}
		if msg.GetDocument() != nil {
			results++
		}
	}
	if results != 1 {
		t.Fatalf("RunQuery returned %d documents, want 1", results)
	}

	upd, err := client.UpdateDocument(ctx, &firestorepb.UpdateDocumentRequest{
		Document: &firestorepb.Document{
			Name:   listenParent + "/things/d1",
			Fields: map[string]*firestorepb.Value{"n": intPb(2)},
		},
		UpdateMask: &firestorepb.DocumentMask{FieldPaths: []string{"n"}},
	})
	if err != nil {
		t.Fatalf("UpdateDocument: %v", err)
	}
	if upd.GetFields()["n"].GetIntegerValue() != 2 {
		t.Fatalf("UpdateDocument n = %v", upd.GetFields()["n"])
	}

	if _, err := client.DeleteDocument(ctx, &firestorepb.DeleteDocumentRequest{Name: listenParent + "/things/d1"}); err != nil {
		t.Fatalf("DeleteDocument: %v", err)
	}
	_ = db
}

// TestUnaryTransactionsAndBatches exercises the transaction and non-atomic batch
// RPCs through the client.
func TestUnaryTransactionsAndBatches(t *testing.T) {
	client, _, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()
	const db = "projects/test/databases/(default)"

	// Commit a write inside a started transaction.
	begin, err := client.BeginTransaction(ctx, &firestorepb.BeginTransactionRequest{Database: db})
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	commit, err := client.Commit(ctx, &firestorepb.CommitRequest{
		Database:    db,
		Transaction: begin.GetTransaction(),
		Writes: []*firestorepb.Write{{
			Operation: &firestorepb.Write_Update{Update: &firestorepb.Document{
				Name:   listenParent + "/things/t1",
				Fields: map[string]*firestorepb.Value{"n": intPb(9)},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if len(commit.GetWriteResults()) != 1 {
		t.Fatalf("Commit returned %d write results", len(commit.GetWriteResults()))
	}

	// Begin again, then roll it back.
	begin2, err := client.BeginTransaction(ctx, &firestorepb.BeginTransactionRequest{Database: db})
	if err != nil {
		t.Fatalf("BeginTransaction 2: %v", err)
	}
	if _, err := client.Rollback(ctx, &firestorepb.RollbackRequest{Database: db, Transaction: begin2.GetTransaction()}); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// BatchGet the committed document.
	bg, err := client.BatchGetDocuments(ctx, &firestorepb.BatchGetDocumentsRequest{
		Database:  db,
		Documents: []string{listenParent + "/things/t1"},
	})
	if err != nil {
		t.Fatalf("BatchGetDocuments: %v", err)
	}
	var found int
	for {
		msg, rerr := bg.Recv()
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			t.Fatalf("BatchGetDocuments Recv: %v", rerr)
		}
		if msg.GetFound() != nil {
			found++
		}
	}
	if found != 1 {
		t.Fatalf("BatchGetDocuments found %d documents, want 1", found)
	}

	// Non-atomic BatchWrite: one delete.
	bw, err := client.BatchWrite(ctx, &firestorepb.BatchWriteRequest{
		Database: db,
		Writes: []*firestorepb.Write{{
			Operation: &firestorepb.Write_Delete{Delete: listenParent + "/things/t1"},
		}},
	})
	if err != nil {
		t.Fatalf("BatchWrite: %v", err)
	}
	if len(bw.GetStatus()) != 1 || bw.GetStatus()[0].GetCode() != 0 {
		t.Fatalf("BatchWrite status = %v", bw.GetStatus())
	}
}
