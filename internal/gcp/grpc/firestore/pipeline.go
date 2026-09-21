package firestore

import (
	"context"
	"strings"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"jaiscloud/internal/clock"
	firestoreprovider "jaiscloud/internal/gcp/provider/firestore"
	firestorestore "jaiscloud/internal/gcp/store/firestore"
	"jaiscloud/internal/model"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ExecutePipeline implements the server-streaming Firestore.ExecutePipeline RPC
// over the existing transport-agnostic query engine.
//
// The pinned proto models the pipeline as an open-ended ordered list of stages
// whose arguments are arbitrary Value expression trees (the pipeline DSL). The
// emulator implements the read-only relational subset needed to express a
// collection / collection-group / database / document-set source followed by
// limit and offset:
//
//	source stages: collection, collection_group, database, documents, literals
//	transform:     limit, offset
//
// Every stage outside that vocabulary fails loudly with codes.Unimplemented
// rather than fabricating a result, matching the emulator's convention of
// refusing to silently diverge. The whole pipeline is evaluated eagerly (one
// response carrying all results plus execution_time), which is a valid batching
// of the stream.
func (s *Service) ExecutePipeline(req *firestorepb.ExecutePipelineRequest, stream firestorepb.Firestore_ExecutePipelineServer) error {
	ctx := stream.Context()
	project, database, _, ok := splitParent(req.GetDatabase())
	if !ok {
		return mapError(model.NewProviderError("InvalidArgument", "invalid database resource name", 400))
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	sp := req.GetStructuredPipeline()
	if sp == nil || sp.GetPipeline() == nil || len(sp.GetPipeline().GetStages()) == 0 {
		return mapError(model.NewProviderError("InvalidArgument", "structured_pipeline.pipeline.stages is required", 400))
	}

	txn := req.GetTransaction()
	resp := &firestorepb.ExecutePipelineResponse{ExecutionTime: timestamppb.New(clock.Now())}
	if req.GetNewTransaction() != nil {
		newTxn, err := s.svc.BeginTransaction(ctx)
		if err != nil {
			return mapError(err)
		}
		resp.Transaction = newTxn
		txn = newTxn
	}

	docs, err := s.runPipeline(ctx, project, database, sp.GetPipeline().GetStages(), txn)
	if err != nil {
		return err
	}
	resp.Results = encodeDocuments(docs)
	return stream.Send(resp)
}

// runPipeline evaluates the ordered stage list. The first stage must be a
// source; every later stage transforms the document stream.
func (s *Service) runPipeline(ctx context.Context, project, database string, stages []*firestorepb.Pipeline_Stage, txn []byte) ([]firestorestore.Document, error) {
	var docs []firestorestore.Document
	sourced := false
	for _, stage := range stages {
		name := stage.GetName()
		switch name {
		case "collection", "collection_group", "database", "documents", "literals":
			if sourced {
				return nil, status.Errorf(codes.InvalidArgument, "pipeline stage %q may only be used as the first stage", name)
			}
			out, err := s.runPipelineSource(ctx, project, database, stage, txn)
			if err != nil {
				return nil, err
			}
			docs = out
			sourced = true
		case "limit":
			docs = pipelineLimit(docs, stage)
		case "offset":
			docs = pipelineOffset(docs, stage)
		default:
			return nil, status.Errorf(codes.Unimplemented, "ExecutePipeline stage %q is not supported by this emulator", name)
		}
	}
	if !sourced {
		return nil, status.Errorf(codes.InvalidArgument, "pipeline must begin with a source stage")
	}
	return docs, nil
}

// runPipelineSource resolves one source stage into its document set.
func (s *Service) runPipelineSource(ctx context.Context, project, database string, stage *firestorepb.Pipeline_Stage, txn []byte) ([]firestorestore.Document, error) {
	switch stage.GetName() {
	case "collection":
		ref := referenceArg(stage, 0)
		if ref == "" {
			return nil, status.Error(codes.InvalidArgument, "collection stage requires a collection reference argument")
		}
		parentRel, collectionID, ok := pipelineCollectionPath(ref)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "collection stage has an empty collection path")
		}
		return s.runPipelineQuery(ctx, project, database, parentRel, &firestoreprovider.StructuredQuery{
			From: []firestoreprovider.CollectionSelector{{CollectionID: collectionID}},
		}, txn)
	case "collection_group":
		ancestor := referenceArg(stage, 0)
		collectionID := stringArg(stage, 1)
		if collectionID == "" {
			return nil, status.Error(codes.InvalidArgument, "collection_group stage requires a collection id argument")
		}
		parentRel := strings.TrimPrefix(ancestor, "/")
		return s.runPipelineQuery(ctx, project, database, parentRel, &firestoreprovider.StructuredQuery{
			From: []firestoreprovider.CollectionSelector{{CollectionID: collectionID, AllDescendants: true}},
		}, txn)
	case "database":
		return s.runPipelineQuery(ctx, project, database, "", &firestoreprovider.StructuredQuery{}, txn)
	case "documents":
		out := make([]firestorestore.Document, 0, len(stage.GetArgs()))
		for i := range stage.GetArgs() {
			ref := referenceArg(stage, i)
			if ref == "" {
				return nil, status.Error(codes.InvalidArgument, "documents stage arguments must be document references")
			}
			name := pipelineDocumentName(project, database, ref)
			doc, err := s.svc.GetDocument(ctx, name, txn, nil)
			if err != nil {
				return nil, mapError(err)
			}
			out = append(out, doc)
		}
		return out, nil
	case "literals":
		out := make([]firestorestore.Document, 0, len(stage.GetArgs()))
		for _, arg := range stage.GetArgs() {
			fields, err := decodeFields(&firestorepb.Document{Fields: arg.GetMapValue().GetFields()})
			if err != nil {
				return nil, mapError(model.NewProviderError("InvalidArgument", err.Error(), 400))
			}
			out = append(out, firestorestore.Document{Fields: fields})
		}
		return out, nil
	}
	return nil, status.Errorf(codes.Unimplemented, "ExecutePipeline source stage %q is not supported by this emulator", stage.GetName())
}

// runPipelineQuery runs a structured query through the shared provider engine.
func (s *Service) runPipelineQuery(ctx context.Context, project, database, rel string, q *firestoreprovider.StructuredQuery, txn []byte) ([]firestorestore.Document, error) {
	docs, err := s.svc.RunQuery(ctx, project, database, rel, q, txn)
	if err != nil {
		return nil, mapError(err)
	}
	return docs, nil
}

// pipelineLimit truncates the stream to the stage's integer argument (a
// non-positive limit means "no limit", matching StructuredQuery.Limit).
func pipelineLimit(docs []firestorestore.Document, stage *firestorepb.Pipeline_Stage) []firestorestore.Document {
	n := intArg(stage, 0)
	if n <= 0 || n >= len(docs) {
		return docs
	}
	return docs[:n]
}

// pipelineOffset drops the first stage-argument documents.
func pipelineOffset(docs []firestorestore.Document, stage *firestorepb.Pipeline_Stage) []firestorestore.Document {
	n := intArg(stage, 0)
	if n <= 0 {
		return docs
	}
	if n >= len(docs) {
		return nil
	}
	return docs[n:]
}

// referenceArg returns a reference-typed stage argument, tolerating a
// string-typed argument for callers that build stages by hand.
func referenceArg(stage *firestorepb.Pipeline_Stage, i int) string {
	args := stage.GetArgs()
	if i >= len(args) {
		return ""
	}
	if ref := args[i].GetReferenceValue(); ref != "" {
		return ref
	}
	return args[i].GetStringValue()
}

// stringArg returns a string-typed stage argument.
func stringArg(stage *firestorepb.Pipeline_Stage, i int) string {
	args := stage.GetArgs()
	if i >= len(args) {
		return ""
	}
	return args[i].GetStringValue()
}

// intArg returns an integer-typed stage argument.
func intArg(stage *firestorepb.Pipeline_Stage, i int) int {
	args := stage.GetArgs()
	if i >= len(args) {
		return 0
	}
	return int(args[i].GetIntegerValue())
}

// pipelineCollectionPath splits a collection reference ("/a/b/coll") into the
// parent document path relative to the documents root ("a/b") and the
// collection id ("coll"). A full resource name is accepted too.
func pipelineCollectionPath(ref string) (parentRel, collectionID string, ok bool) {
	if idx := strings.Index(ref, "/documents/"); idx >= 0 {
		ref = ref[idx+len("/documents"):]
	}
	ref = strings.TrimPrefix(ref, "/")
	if ref == "" {
		return "", "", false
	}
	parts := strings.Split(ref, "/")
	collectionID = parts[len(parts)-1]
	parentRel = strings.Join(parts[:len(parts)-1], "/")
	return parentRel, collectionID, true
}

// pipelineDocumentName expands a documents-stage reference ("/a/b/doc", or a
// full resource name) into a full document name.
func pipelineDocumentName(project, database, ref string) string {
	if strings.HasPrefix(ref, "projects/") {
		return ref
	}
	base := "projects/" + project + "/databases/" + database + "/documents"
	ref = strings.TrimPrefix(ref, "/")
	if ref == "" {
		return base
	}
	return base + "/" + ref
}
