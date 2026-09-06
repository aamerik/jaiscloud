package firestore

import (
	"context"
	"math"
	"strconv"
	"strings"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"jaiscloud/internal/clock"
	firestorestore "jaiscloud/internal/gcp/store/firestore"
	"jaiscloud/internal/model"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// RunAggregationQuery executes a StructuredAggregationQuery over the existing
// query engine and streams a single AggregationResult (count/sum/avg) back.
func (s *Service) RunAggregationQuery(req *firestorepb.RunAggregationQueryRequest, stream firestorepb.Firestore_RunAggregationQueryServer) error {
	ctx := stream.Context()
	aq := req.GetStructuredAggregationQuery()
	if aq == nil {
		return mapError(model.NewProviderError("InvalidArgument", "structured_aggregation_query is required", 400))
	}
	project, database, rel, ok := splitParent(req.GetParent())
	if !ok {
		return mapError(model.NewProviderError("InvalidArgument", "invalid parent resource name", 400))
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	q, err := decodeStructuredQuery(aq.GetStructuredQuery())
	if err != nil {
		return mapError(model.NewProviderError("InvalidArgument", err.Error(), 400))
	}
	docs, err := s.svc.RunQuery(ctx, project, database, rel, q, req.GetTransaction())
	if err != nil {
		return mapError(err)
	}

	fields := make(map[string]*firestorepb.Value, len(aq.GetAggregations()))
	nextAlias := 1
	for _, agg := range aq.GetAggregations() {
		alias := agg.GetAlias()
		if alias == "" {
			alias = "field_" + strconv.Itoa(nextAlias)
			nextAlias++
		}
		fields[alias] = encodeValue(evalAggregation(docs, agg))
	}

	return stream.Send(&firestorepb.RunAggregationQueryResponse{
		Result:   &firestorepb.AggregationResult{AggregateFields: fields},
		ReadTime: timestamppb.New(clock.Now()),
	})
}

// evalAggregation computes a single aggregation over the matching documents.
func evalAggregation(docs []firestorestore.Document, agg *firestorepb.StructuredAggregationQuery_Aggregation) *firestorestore.Value {
	switch op := agg.GetOperator().(type) {
	case *firestorepb.StructuredAggregationQuery_Aggregation_Count_:
		n := int64(len(docs))
		if up := op.Count.GetUpTo(); up != nil && up.Value < n {
			n = up.Value
		}
		return firestorestore.IntVal(n)
	case *firestorepb.StructuredAggregationQuery_Aggregation_Sum_:
		return sumField(docs, op.Sum.GetField().GetFieldPath())
	case *firestorepb.StructuredAggregationQuery_Aggregation_Avg_:
		return avgField(docs, op.Avg.GetField().GetFieldPath())
	}
	return firestorestore.NullVal()
}

// docFieldValue resolves a dot-delimited field path on a document, returning
// nil when the field is absent (mirrors the provider's fieldValue helper).
func docFieldValue(doc *firestorestore.Document, fieldPath string) *firestorestore.Value {
	if doc == nil || doc.Fields == nil {
		return nil
	}
	parts := strings.Split(fieldPath, ".")
	cur, ok := doc.Fields[parts[0]]
	if !ok {
		return nil
	}
	for _, p := range parts[1:] {
		if cur == nil || cur.MapValue == nil {
			return nil
		}
		cur = cur.MapValue.Fields[p]
	}
	return cur
}

// sumField aggregates the numeric values of fieldPath across docs. Non-numeric
// values are skipped; an empty set yields integer 0; all-integer input yields an
// integer (overflow ignored); mixed input yields a double; NaN propagates.
func sumField(docs []firestorestore.Document, fieldPath string) *firestorestore.Value {
	var intSum int64
	var floatSum float64
	allInt := true
	any := false
	nan := false
	for i := range docs {
		v := docFieldValue(&docs[i], fieldPath)
		switch {
		case v == nil:
			continue
		case v.IntegerValue != nil:
			intSum += *v.IntegerValue
			floatSum += float64(*v.IntegerValue)
			any = true
		case v.DoubleValue != nil:
			if math.IsNaN(*v.DoubleValue) {
				nan = true
			}
			floatSum += *v.DoubleValue
			allInt = false
			any = true
		}
	}
	if nan {
		return firestorestore.DoubleVal(math.NaN())
	}
	if !any {
		return firestorestore.IntVal(0)
	}
	if allInt {
		return firestorestore.IntVal(intSum)
	}
	return firestorestore.DoubleVal(floatSum)
}

// avgField averages the numeric values of fieldPath across docs. Non-numeric
// values are skipped; an empty set yields null; the result is always a double.
func avgField(docs []firestorestore.Document, fieldPath string) *firestorestore.Value {
	var sum float64
	count := 0
	nan := false
	for i := range docs {
		v := docFieldValue(&docs[i], fieldPath)
		switch {
		case v == nil:
			continue
		case v.IntegerValue != nil:
			sum += float64(*v.IntegerValue)
			count++
		case v.DoubleValue != nil:
			if math.IsNaN(*v.DoubleValue) {
				nan = true
			}
			sum += *v.DoubleValue
			count++
		}
	}
	if nan {
		return firestorestore.DoubleVal(math.NaN())
	}
	if count == 0 {
		return firestorestore.NullVal()
	}
	return firestorestore.DoubleVal(sum / float64(count))
}

// PartitionQuery returns partition cursors for parallel reads. The emulator does
// not partition: it returns an empty partitions list, which is the proto's
// "not supported for partitioning" signal and lets the SDK fall back to a single
// full-range partition.
func (s *Service) PartitionQuery(ctx context.Context, req *firestorepb.PartitionQueryRequest) (*firestorepb.PartitionQueryResponse, error) {
	return &firestorepb.PartitionQueryResponse{}, nil
}

// ExecutePipeline fails loudly with Unimplemented, mirroring the AWS emulator's
// "notImplemented" convention (fail rather than silently succeed). The pinned
// firestorepb models it as a server-streaming RPC over a general-purpose
// Pipeline DSL (StructuredPipeline → Pipeline → stages with arbitrary names and
// args encoded as Value trees) whose stage vocabulary is open-ended and
// version-dependent, so a partial implementation would fabricate an incorrect
// wire shape.
func (s *Service) ExecutePipeline(req *firestorepb.ExecutePipelineRequest, stream firestorepb.Firestore_ExecutePipelineServer) error {
	return status.Error(codes.Unimplemented, "ExecutePipeline is not supported by this emulator")
}
