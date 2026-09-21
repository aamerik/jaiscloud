package firestore

import (
	"context"
	"errors"
	"io"
	"testing"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	firestorestore "jaiscloud/internal/gcp/store/firestore"
)

func pipelineDB() string { return "projects/test/databases/(default)" }

func refVal(v string) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_ReferenceValue{ReferenceValue: v}}
}

func intValue(v int64) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_IntegerValue{IntegerValue: v}}
}

func TestExecutePipelineCollectionLimit(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	for _, id := range []string{"a", "b", "c"} {
		if _, err := svc.CreateDocument(ctx, "test", "(default)", "books", id, map[string]*firestorestore.Value{
			"title": firestorestore.StringVal(id),
		}); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}

	stream, err := client.ExecutePipeline(ctx, &firestorepb.ExecutePipelineRequest{
		Database: pipelineDB(),
		PipelineType: &firestorepb.ExecutePipelineRequest_StructuredPipeline{
			StructuredPipeline: &firestorepb.StructuredPipeline{
				Pipeline: &firestorepb.Pipeline{Stages: []*firestorepb.Pipeline_Stage{
					{Name: "collection", Args: []*firestorepb.Value{refVal("/books")}},
					{Name: "limit", Args: []*firestorepb.Value{intValue(2)}},
				}},
			},
		},
	})
	if err != nil {
		t.Fatalf("ExecutePipeline: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if len(resp.GetResults()) != 2 {
		t.Fatalf("expected 2 results, got %d", len(resp.GetResults()))
	}
	if resp.GetExecutionTime() == nil {
		t.Fatal("expected execution_time")
	}
	if got := resp.GetResults()[0].GetFields()["title"].GetStringValue(); got != "a" {
		t.Fatalf("expected first result title a, got %q", got)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF after results, got %v", err)
	}
}

func TestExecutePipelineUnsupportedStage(t *testing.T) {
	client, _, cleanup := listenTestClient(t)
	defer cleanup()

	stream, err := client.ExecutePipeline(context.Background(), &firestorepb.ExecutePipelineRequest{
		Database: pipelineDB(),
		PipelineType: &firestorepb.ExecutePipelineRequest_StructuredPipeline{
			StructuredPipeline: &firestorepb.StructuredPipeline{
				Pipeline: &firestorepb.Pipeline{Stages: []*firestorepb.Pipeline_Stage{
					{Name: "collection", Args: []*firestorepb.Value{refVal("/books")}},
					{Name: "where", Args: []*firestorepb.Value{refVal("title")}},
				}},
			},
		},
	})
	if err != nil {
		t.Fatalf("ExecutePipeline: %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.Unimplemented {
		t.Fatalf("expected Unimplemented for unsupported stage, got %v", err)
	}
}

func TestExecutePipelineRequiresPipeline(t *testing.T) {
	client, _, cleanup := listenTestClient(t)
	defer cleanup()

	stream, err := client.ExecutePipeline(context.Background(), &firestorepb.ExecutePipelineRequest{
		Database: pipelineDB(),
	})
	if err != nil {
		t.Fatalf("ExecutePipeline: %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for a missing pipeline, got %v", err)
	}
}

func TestPartitionQuerySplitsResults(t *testing.T) {
	client, svc, cleanup := listenTestClient(t)
	defer cleanup()
	ctx := context.Background()

	for _, id := range []string{"p0", "p1", "p2", "p3", "p4", "p5"} {
		if _, err := svc.CreateDocument(ctx, "test", "(default)", "items", id, map[string]*firestorestore.Value{
			"n": firestorestore.IntVal(1),
		}); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}

	resp, err := client.PartitionQuery(ctx, &firestorepb.PartitionQueryRequest{
		Parent: writeParent,
		QueryType: &firestorepb.PartitionQueryRequest_StructuredQuery{
			StructuredQuery: &firestorepb.StructuredQuery{
				From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "items", AllDescendants: true}},
				OrderBy: []*firestorepb.StructuredQuery_Order{{
					Field:     &firestorepb.StructuredQuery_FieldReference{FieldPath: "__name__"},
					Direction: firestorepb.StructuredQuery_ASCENDING,
				}},
			},
		},
		PartitionCount: 3,
	})
	if err != nil {
		t.Fatalf("PartitionQuery: %v", err)
	}
	if len(resp.GetPartitions()) != 2 {
		t.Fatalf("expected 2 partitions for 6 documents / count 3, got %d", len(resp.GetPartitions()))
	}
	for i, part := range resp.GetPartitions() {
		if got := part.GetValues()[0].GetReferenceValue(); got == "" {
			t.Fatalf("partition %d cursor has no reference value", i)
		}
	}
}
