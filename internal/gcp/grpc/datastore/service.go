// Package datastore implements the Cloud Datastore (v1) gRPC service
// (google.datastore.v1.Datastore) over the shared datastorestore.Store, so
// entities written via the Go SDK are stored and queried consistently.
//
// Transactions follow real Datastore's read-set optimistic-concurrency model.
// BeginTransaction returns an opaque token and registers an empty read-set;
// Lookup/RunQuery calls carrying that token record the entities they observe;
// and a TRANSACTIONAL Commit re-validates every observed entity's version and
// then applies all mutations atomically (see the Service.Commit logic and the
// store's Commit method). A conflict aborts the commit with ABORTED and applies
// nothing; a per-mutation Precondition mismatch aborts it with
// FAILED_PRECONDITION.
//
// Two internal approximations are documented and never surface as new RPCs or
// fields:
//
//   - RunQuery read validation: real Datastore validates a query's read
//     *range* at commit. The emulator records the version of every entity the
//     query returned and re-validates those entities, which catches a
//     concurrent modification to any returned entity but not the appearance or
//     disappearance of a would-be match outside the result set.
//   - Transaction TTL: real read-write transactions expire after ~270s. The
//     emulator lazily evicts a read-set once it is older than txnTTL, returning
//     InvalidArgument for any later use (the same as an unknown token).
package datastore

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"

	"jaiscloud/internal/clock"
	grpcutil "jaiscloud/internal/gcp/grpc"
	datastorestore "jaiscloud/internal/gcp/store/datastore"
	"jaiscloud/internal/model"

	"google.golang.org/genproto/googleapis/type/latlng"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// txnTTL bounds a transaction's lifetime, mirroring real Datastore's ~270s
// read-write transaction timeout. Expiry is enforced lazily on each access
// rather than by a background sweeper.
const txnTTL = 270 * time.Second

// Service implements datastorepb.DatastoreServer over the shared store.
type Service struct {
	datastorepb.UnimplementedDatastoreServer

	store       datastorestore.Store
	defaultProj string

	// txnMu guards readSets: the in-memory transaction read-set registry
	// (transactions are ephemeral and never persisted).
	txnMu    sync.Mutex
	readSets map[string]*readSet
}

// readSet is an open transaction's observed entities (canonical key → observed
// state) plus its start time for TTL expiry.
type readSet struct {
	reads map[string]datastorestore.ReadRef
	start time.Time
}

// NewService returns a Datastore gRPC service backed by the shared store.
// defaultProj is the config-default project used when a request carries none.
func NewService(store datastorestore.Store, defaultProj string) *Service {
	return &Service{
		store:       store,
		defaultProj: defaultProj,
		readSets:    make(map[string]*readSet),
	}
}

// ─── transaction registry ─────────────────────────────────────────────────────

// txnLocked returns the read-set for an *active* transaction, evicting it if
// its TTL has elapsed, or nil when it is unknown/expired. The caller must hold
// txnMu.
func (s *Service) txnLocked(txn string) *readSet {
	rs := s.readSets[txn]
	if rs == nil {
		return nil
	}
	if clock.Now().Sub(rs.start) > txnTTL {
		delete(s.readSets, txn)
		return nil
	}
	return rs
}

// recordRead registers an entity observation in a transaction's read-set.
// Non-transactional calls (empty token) and unknown/expired transactions are
// ignored. Missing entities are recorded with Exists=false (Version 0) so a
// concurrent create aborts the eventual commit.
func (s *Service) recordRead(txn []byte, key string, exists bool, version int64) {
	if len(txn) == 0 {
		return
	}
	s.txnMu.Lock()
	defer s.txnMu.Unlock()
	rs := s.txnLocked(string(txn))
	if rs == nil {
		return
	}
	if rs.reads == nil {
		rs.reads = make(map[string]datastorestore.ReadRef)
	}
	rs.reads[key] = datastorestore.ReadRef{Key: key, Exists: exists, Version: version}
}

// readSetFor returns the transaction's observations as a slice sorted by key,
// or nil when the token is empty/unknown/expired/there were no reads.
func (s *Service) readSetFor(txn []byte) []datastorestore.ReadRef {
	if len(txn) == 0 {
		return nil
	}
	s.txnMu.Lock()
	defer s.txnMu.Unlock()
	rs := s.txnLocked(string(txn))
	if rs == nil || len(rs.reads) == 0 {
		return nil
	}
	out := make([]datastorestore.ReadRef, 0, len(rs.reads))
	for _, r := range rs.reads {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// clearReadSet discards a transaction's read-set after commit/rollback.
func (s *Service) clearReadSet(txn []byte) {
	if len(txn) == 0 {
		return
	}
	s.txnMu.Lock()
	defer s.txnMu.Unlock()
	delete(s.readSets, string(txn))
}

// requireActive returns an InvalidArgument error when transaction is non-empty
// but not currently active (rolled back, already committed, expired, or never
// begun). An empty transaction denotes a non-transactional operation and always
// passes, so it still works for unknown-token detection in Commit.
func (s *Service) requireActive(transaction []byte) error {
	if len(transaction) == 0 {
		return nil
	}
	s.txnMu.Lock()
	defer s.txnMu.Unlock()
	if s.txnLocked(string(transaction)) == nil {
		return model.NewProviderError("InvalidArgument",
			"transaction is no longer active (rolled back, committed, expired, or never begun)", 400)
	}
	return nil
}

// mapTxnStoreError translates a store.Commit sentinel into the transactional
// commit statuses real Datastore uses: a read-set conflict is ABORTED (retry
// the whole transaction), a per-mutation Precondition mismatch is
// FAILED_PRECONDITION, and an existence mismatch matches the non-transactional
// mapping. A conflict or precondition failure aborts the whole commit.
func mapTxnStoreError(err error) error {
	switch {
	case errors.Is(err, datastorestore.ErrAborted):
		return mapError(model.NewProviderError("Aborted", "transaction was aborted due to concurrent modification", 409))
	case errors.Is(err, datastorestore.ErrConflict):
		return mapError(model.NewProviderError("FailedPrecondition", "precondition failed", 400))
	case errors.Is(err, datastorestore.ErrEntityExists):
		return mapError(model.NewProviderError("AlreadyExists", "entity already exists", 409))
	case errors.Is(err, datastorestore.ErrEntityNotFound):
		return mapError(model.NewProviderError("FailedPrecondition", "entity not found", 404))
	default:
		return mapError(err)
	}
}

// mapError translates a store/service error into a gRPC status error. The
// store's ErrInvalidKey sentinel is not a ProviderError, so it is mapped to
// InvalidArgument here rather than falling through to Internal.
func mapError(err error) error {
	if errors.Is(err, datastorestore.ErrInvalidKey) {
		return grpcutil.GRPCStatus(model.NewProviderError("InvalidArgument", "invalid key", 400))
	}
	return grpcutil.GRPCStatus(err)
}

// txnSeq mints the opaque transaction tokens returned by BeginTransaction.
// Real handles are opaque bytes; a process-unique token is all the registry
// needs to key a transaction's read-set.
var txnSeq int64

func (s *Service) project(ctx context.Context, reqProject string) string {
	if reqProject != "" {
		return reqProject
	}
	return grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
}

// ─── Datastore service ────────────────────────────────────────────────────────

func (s *Service) Commit(ctx context.Context, req *datastorepb.CommitRequest) (*datastorepb.CommitResponse, error) {
	project := s.project(ctx, req.GetProjectId())
	txn := req.GetTransaction()

	// Dispatch on the proto's commit mode. MODE_UNSPECIFIED (the zero value) is
	// treated as non-transactional unless a transaction selector is present —
	// the behavior non-transactional SDK commits rely on; a TRANSACTIONAL
	// commit always requires a transaction handle.
	transactional := false
	switch req.GetMode() {
	case datastorepb.CommitRequest_TRANSACTIONAL:
		if len(txn) == 0 {
			return nil, mapError(model.NewProviderError("InvalidArgument", "TRANSACTIONAL commit requires a transaction", 400))
		}
		transactional = true
	case datastorepb.CommitRequest_NON_TRANSACTIONAL:
		transactional = false
	default:
		transactional = len(txn) > 0
	}
	if transactional {
		return s.commitTransactional(ctx, project, txn, req.GetMutations())
	}

	results := make([]*datastorepb.MutationResult, 0, len(req.GetMutations()))
	for _, m := range req.GetMutations() {
		pre := mutationPrecondition(m)
		mr := &datastorepb.MutationResult{Version: 1}
		switch op := m.GetOperation().(type) {
		case *datastorepb.Mutation_Insert:
			e, allocated, err := s.resolveEntity(ctx, project, op.Insert)
			if err != nil {
				return nil, mapError(err)
			}
			applied, err := s.store.ApplyMutation(ctx, project, datastorestore.MutationInsert, e, pre)
			switch {
			case errors.Is(err, datastorestore.ErrConflict):
				mr.ConflictDetected = true
				mr.Version = applied.Version
			case errors.Is(err, datastorestore.ErrEntityExists):
				return nil, mapError(model.NewProviderError("AlreadyExists", "entity already exists", 409))
			case err != nil:
				return nil, mapError(err)
			default:
				mr.Version = applied.Version
				if allocated {
					mr.Key = keyProto(e.Key, project)
				}
			}
		case *datastorepb.Mutation_Upsert:
			e, allocated, err := s.resolveEntity(ctx, project, op.Upsert)
			if err != nil {
				return nil, mapError(err)
			}
			applied, err := s.store.ApplyMutation(ctx, project, datastorestore.MutationUpsert, e, pre)
			switch {
			case errors.Is(err, datastorestore.ErrConflict):
				mr.ConflictDetected = true
				mr.Version = applied.Version
			case err != nil:
				return nil, mapError(err)
			default:
				mr.Version = applied.Version
				if allocated {
					mr.Key = keyProto(e.Key, project)
				}
			}
		case *datastorepb.Mutation_Update:
			e, err := entityFromProto(op.Update)
			if err != nil {
				return nil, mapError(err)
			}
			if e.Key == "" {
				return nil, mapError(model.NewProviderError("InvalidArgument", "update key is incomplete", 400))
			}
			applied, err := s.store.ApplyMutation(ctx, project, datastorestore.MutationUpdate, e, pre)
			switch {
			case errors.Is(err, datastorestore.ErrConflict):
				mr.ConflictDetected = true
				mr.Version = applied.Version
			case errors.Is(err, datastorestore.ErrEntityNotFound):
				return nil, mapError(model.NewProviderError("FailedPrecondition", "entity not found", 404))
			case err != nil:
				return nil, mapError(err)
			default:
				mr.Version = applied.Version
			}
		case *datastorepb.Mutation_Delete:
			key, err := deleteKey(op.Delete)
			if err != nil {
				return nil, mapError(err)
			}
			if err := s.store.DeleteConflictChecked(ctx, project, key, pre); err != nil {
				if errors.Is(err, datastorestore.ErrConflict) {
					mr.ConflictDetected = true
				} else {
					return nil, mapError(err)
				}
			}
		default:
			return nil, mapError(model.NewProviderError("InvalidArgument", "mutation has no operation", 400))
		}
		results = append(results, mr)
	}
	return &datastorepb.CommitResponse{
		MutationResults: results,
		// CommitTime is not set for non-transactional commits (see the proto).
	}, nil
}

// commitTransactional implements the Datastore transaction commit protocol. It
// re-validates the transaction's read-set and applies every mutation
// atomically: any read-set conflict aborts the whole commit with ABORTED and
// applies nothing, while a per-mutation Precondition mismatch aborts it with
// FAILED_PRECONDITION. The transaction is terminated on every attempt that
// reaches the store (real Datastore requires a fresh BeginTransaction to retry
// an aborted one).
//
// Entity resolution (key parsing and auto-ID allocation) happens before the
// store commit. ID allocation is not rolled back if the commit aborts — an
// internal approximation matching real Datastore, where allocated IDs are
// monotonic and may be skipped.
func (s *Service) commitTransactional(ctx context.Context, project string, txn []byte, mutations []*datastorepb.Mutation) (*datastorepb.CommitResponse, error) {
	if err := s.requireActive(txn); err != nil {
		return nil, mapError(err)
	}
	reads := s.readSetFor(txn)

	writes := make([]datastorestore.Write, 0, len(mutations))
	allocatedKeys := make([]*datastorepb.Key, len(mutations))
	for i, m := range mutations {
		pre := mutationPrecondition(m)
		switch op := m.GetOperation().(type) {
		case *datastorepb.Mutation_Insert:
			e, allocated, err := s.resolveEntity(ctx, project, op.Insert)
			if err != nil {
				return nil, mapError(err)
			}
			writes = append(writes, datastorestore.Write{Op: datastorestore.WriteInsert, Key: e.Key, Entity: e, Precondition: pre})
			if allocated {
				allocatedKeys[i] = keyProto(e.Key, project)
			}
		case *datastorepb.Mutation_Upsert:
			e, allocated, err := s.resolveEntity(ctx, project, op.Upsert)
			if err != nil {
				return nil, mapError(err)
			}
			writes = append(writes, datastorestore.Write{Op: datastorestore.WriteUpsert, Key: e.Key, Entity: e, Precondition: pre})
			if allocated {
				allocatedKeys[i] = keyProto(e.Key, project)
			}
		case *datastorepb.Mutation_Update:
			e, err := entityFromProto(op.Update)
			if err != nil {
				return nil, mapError(err)
			}
			if e.Key == "" {
				return nil, mapError(model.NewProviderError("InvalidArgument", "update key is incomplete", 400))
			}
			writes = append(writes, datastorestore.Write{Op: datastorestore.WriteUpdate, Key: e.Key, Entity: e, Precondition: pre})
		case *datastorepb.Mutation_Delete:
			key, err := deleteKey(op.Delete)
			if err != nil {
				return nil, mapError(err)
			}
			writes = append(writes, datastorestore.Write{Op: datastorestore.WriteDelete, Key: key, Precondition: pre})
		default:
			return nil, mapError(model.NewProviderError("InvalidArgument", "mutation has no operation", 400))
		}
	}

	commitTime := clock.Now()
	applied, err := s.store.Commit(ctx, project, reads, writes)
	s.clearReadSet(txn)
	if err != nil {
		return nil, mapTxnStoreError(err)
	}

	results := make([]*datastorepb.MutationResult, 0, len(applied))
	for i := range applied {
		mr := &datastorepb.MutationResult{Version: applied[i].Version}
		if allocatedKeys[i] != nil {
			mr.Key = allocatedKeys[i]
		}
		results = append(results, mr)
	}
	return &datastorepb.CommitResponse{
		MutationResults: results,
		CommitTime:      timestamppb.New(commitTime),
	}, nil
}

// mutationPrecondition translates a Mutation's conflict_detection_strategy
// oneof (base_version or update_time — real Datastore's per-mutation
// optimistic-concurrency precondition) into a store Precondition. Returns nil
// when the mutation carries neither (the common case — no precondition).
func mutationPrecondition(m *datastorepb.Mutation) *datastorestore.Precondition {
	switch v := m.GetConflictDetectionStrategy().(type) {
	case *datastorepb.Mutation_BaseVersion:
		bv := v.BaseVersion
		return &datastorestore.Precondition{BaseVersion: &bv}
	case *datastorepb.Mutation_UpdateTime:
		ut := v.UpdateTime.AsTime()
		return &datastorestore.Precondition{UpdateTime: &ut}
	default:
		return nil
	}
}

func (s *Service) Lookup(ctx context.Context, req *datastorepb.LookupRequest) (*datastorepb.LookupResponse, error) {
	// The transaction selector lives in ReadOptions (read_options.transaction),
	// matching the real proto. ReadOptions.new_transaction (implicit begin) is
	// not supported; such a read is treated as non-transactional.
	txn := req.GetReadOptions().GetTransaction()
	if err := s.requireActive(txn); err != nil {
		return nil, mapError(err)
	}
	project := s.project(ctx, req.GetProjectId())
	resp := &datastorepb.LookupResponse{}
	for _, k := range req.GetKeys() {
		key, kind, complete, err := canonicalKey(k)
		if err != nil {
			return nil, mapError(err)
		}
		if !complete {
			return nil, mapError(model.NewProviderError("InvalidArgument", "lookup key is incomplete", 400))
		}
		e, err := s.store.Get(ctx, project, key)
		switch {
		case errors.Is(err, datastorestore.ErrEntityNotFound):
			// Record the miss (version 0) so a concurrent create aborts the
			// eventual commit — real Datastore records absent keys in the
			// read-set too.
			s.recordRead(txn, key, false, 0)
			resp.Missing = append(resp.Missing, &datastorepb.EntityResult{
				Entity:  &datastorepb.Entity{Key: keyProto(key, project)},
				Version: 1,
			})
		case err != nil:
			return nil, mapError(err)
		default:
			_ = kind
			s.recordRead(txn, key, true, e.Version)
			resp.Found = append(resp.Found, &datastorepb.EntityResult{
				Entity: entityToProto(e, project),
				// Real, per-entity version — clients read this and pass it
				// back as a Mutation's base_version for a conditional write;
				// a hardcoded 1 would make that OCC contract meaningless.
				Version: e.Version,
			})
		}
	}
	return resp, nil
}

func (s *Service) RunQuery(ctx context.Context, req *datastorepb.RunQueryRequest) (*datastorepb.RunQueryResponse, error) {
	// See Lookup: the transaction selector lives in ReadOptions.
	txn := req.GetReadOptions().GetTransaction()
	if err := s.requireActive(txn); err != nil {
		return nil, mapError(err)
	}
	project := s.project(ctx, req.GetProjectId())
	batch := &datastorepb.QueryResultBatch{
		EntityResultType: datastorepb.EntityResult_FULL,
		MoreResults:      datastorepb.QueryResultBatch_NO_MORE_RESULTS,
	}

	var q *datastorepb.Query
	switch qt := req.GetQueryType().(type) {
	case *datastorepb.RunQueryRequest_Query:
		q = qt.Query
	case *datastorepb.RunQueryRequest_GqlQuery:
		return nil, mapError(model.NewProviderError("InvalidArgument", "GQL queries are not supported", 400))
	default:
		return nil, mapError(model.NewProviderError("InvalidArgument", "run query request has no query", 400))
	}

	kind := ""
	if q != nil && len(q.GetKind()) > 0 {
		kind = q.GetKind()[0].GetName()
	}
	entities, err := s.store.ListKind(ctx, project, kind)
	if err != nil {
		return nil, mapError(err)
	}
	for _, e := range entities {
		if q != nil {
			match, err := matchesFilter(e, q.GetFilter())
			if err != nil {
				return nil, mapError(err)
			}
			if !match {
				continue
			}
		}
		// Internal approximation: real Datastore validates the query's read
		// *range* at commit; the emulator records the version of every entity
		// the query returned and re-validates exactly those entities (see the
		// package doc).
		s.recordRead(txn, e.Key, true, e.Version)
		batch.EntityResults = append(batch.EntityResults, &datastorepb.EntityResult{
			Entity:  entityToProto(e, project),
			Version: e.Version,
		})
	}
	return &datastorepb.RunQueryResponse{Batch: batch}, nil
}

// RunAggregationQuery implements the Datastore aggregation-query RPC over the
// structured AggregationQuery form. It runs the wrapped nested query with the
// same engine RunQuery uses (ListKind + matchesFilter) and reduces the matching
// entities to one result per aggregation (count/sum/avg), keyed by alias.
//
// The GQL aggregation form is not supported by the emulator's query engine and
// fails loud with InvalidArgument rather than returning a silent empty result,
// mirroring RunQuery. Zero aggregations is rejected as well: the proto requires
// a minimum of one, so a request without any is malformed.
//
// Transaction-aware: a ReadOptions.transaction selector must name an *active*
// transaction, and every entity the nested query returns is recorded in the
// transaction's read-set — exactly as RunQuery does — so a transactional
// commit re-validates them (real Datastore's RunAggregationQuery participates
// in transactions). Non-transactional calls are unaffected.
func (s *Service) RunAggregationQuery(ctx context.Context, req *datastorepb.RunAggregationQueryRequest) (*datastorepb.RunAggregationQueryResponse, error) {
	if err := rejectDatabaseID(req.GetDatabaseId()); err != nil {
		return nil, mapError(err)
	}
	// See RunQuery: the transaction selector lives in ReadOptions.
	txn := req.GetReadOptions().GetTransaction()
	if err := s.requireActive(txn); err != nil {
		return nil, mapError(err)
	}
	project := s.project(ctx, req.GetProjectId())

	var aq *datastorepb.AggregationQuery
	switch qt := req.GetQueryType().(type) {
	case *datastorepb.RunAggregationQueryRequest_AggregationQuery:
		aq = qt.AggregationQuery
	case *datastorepb.RunAggregationQueryRequest_GqlQuery:
		return nil, mapError(model.NewProviderError("InvalidArgument", "GQL aggregation queries are not supported", 400))
	default:
		return nil, mapError(model.NewProviderError("InvalidArgument", "run aggregation query request has no query", 400))
	}
	if aq == nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", "aggregation query is missing", 400))
	}
	nested := aq.GetNestedQuery()
	if nested == nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", "aggregation query is missing its nested query", 400))
	}
	aggs := aq.GetAggregations()
	if len(aggs) == 0 {
		return nil, mapError(model.NewProviderError("InvalidArgument", "aggregation query must contain at least one aggregation", 400))
	}
	if len(aggs) > maxAggregations {
		return nil, mapError(model.NewProviderError("InvalidArgument", "aggregation query supports at most five aggregations", 400))
	}

	kind := ""
	if len(nested.GetKind()) > 0 {
		kind = nested.GetKind()[0].GetName()
	}
	entities, err := s.store.ListKind(ctx, project, kind)
	if err != nil {
		return nil, mapError(err)
	}

	// Reduce the nested query once, recording every matching entity in the
	// transaction read-set (the same approximation RunQuery makes: the version
	// of each returned entity is re-validated at commit).
	matching := make([]datastorestore.Entity, 0, len(entities))
	for _, e := range entities {
		match, err := matchesFilter(e, nested.GetFilter())
		if err != nil {
			return nil, mapError(err)
		}
		if !match {
			continue
		}
		s.recordRead(txn, e.Key, true, e.Version)
		matching = append(matching, e)
	}

	// A non-grouped aggregation query returns a single AggregationResult whose
	// aggregate_properties map holds one entry per aggregation.
	result := &datastorepb.AggregationResult{AggregateProperties: make(map[string]*datastorepb.Value, len(aggs))}
	unnamed := 0
	for _, agg := range aggs {
		alias := agg.GetAlias()
		if alias == "" {
			// Real Datastore auto-names an unaliased aggregation
			// "property_<incremental_id>", sharing one counter across the
			// whole query (proto AggregationQuery.Aggregation.alias docs).
			unnamed++
			alias = "property_" + strconv.Itoa(unnamed)
		}
		if _, dup := result.AggregateProperties[alias]; dup {
			return nil, mapError(model.NewProviderError("InvalidArgument", "duplicate aggregation alias: "+alias, 400))
		}
		val, err := aggregate(agg, matching, project)
		if err != nil {
			return nil, mapError(err)
		}
		result.AggregateProperties[alias] = val
	}

	return &datastorepb.RunAggregationQueryResponse{
		Batch: &datastorepb.AggregationResultBatch{
			AggregationResults: []*datastorepb.AggregationResult{result},
			MoreResults:        datastorepb.QueryResultBatch_NO_MORE_RESULTS,
			ReadTime:           timestamppb.New(clock.Now()),
		},
	}, nil
}

// maxAggregations is the proto cap on aggregations per AggregationQuery.
const maxAggregations = 5

// aggregate computes one aggregation over the entities the nested query
// returned and returns its wire Value.
func aggregate(agg *datastorepb.AggregationQuery_Aggregation, entities []datastorestore.Entity, project string) (*datastorepb.Value, error) {
	switch op := agg.GetOperator().(type) {
	case *datastorepb.AggregationQuery_Aggregation_Count_:
		n := int64(len(entities))
		if upTo := op.Count.GetUpTo(); upTo != nil {
			if upTo.GetValue() < 0 {
				return nil, model.NewProviderError("InvalidArgument", "count up_to must be non-negative", 400)
			}
			if n > upTo.GetValue() {
				n = upTo.GetValue()
			}
		}
		return valueToProto(datastorestore.Value{IntegerValue: &n}, project), nil
	case *datastorepb.AggregationQuery_Aggregation_Sum_:
		return aggregateSum(op.Sum.GetProperty().GetName(), entities, project)
	case *datastorepb.AggregationQuery_Aggregation_Avg_:
		return aggregateAvg(op.Avg.GetProperty().GetName(), entities, project)
	default:
		return nil, model.NewProviderError("InvalidArgument", "aggregation has no operator", 400)
	}
}

// aggregateSum sums the named property over entities, following the proto's
// documented Sum behavior:
//   - only integer and double values contribute; a missing property or a
//     non-numeric value (string, bool, null, key, array, ...) is skipped;
//   - an empty contributing set yields integer 0;
//   - the result is a 64-bit integer when every contributing value is an
//     integer and the sum does not overflow int64; otherwise it is a double.
func aggregateSum(name string, entities []datastorestore.Entity, project string) (*datastorepb.Value, error) {
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "sum aggregation requires a property", 400)
	}
	var (
		isum     int64
		fsum     float64
		allInt   = true
		overflow bool
	)
	for _, e := range entities {
		v, ok := e.Properties[name]
		if !ok {
			continue
		}
		switch {
		case v.IntegerValue != nil:
			iv := *v.IntegerValue
			if (iv > 0 && isum+iv < isum) || (iv < 0 && isum+iv > isum) {
				overflow = true
			}
			isum += iv
			fsum += float64(iv)
		case v.DoubleValue != nil:
			allInt = false
			fsum += *v.DoubleValue
		default:
			continue
		}
	}
	if allInt && !overflow {
		return valueToProto(datastorestore.Value{IntegerValue: &isum}, project), nil
	}
	return valueToProto(datastorestore.Value{DoubleValue: &fsum}, project), nil
}

// aggregateAvg averages the named property over entities, following the
// proto's documented Avg behavior: only integer and double values contribute;
// a missing property or a non-numeric value is skipped; an empty contributing
// set yields NULL; and the result is always a double.
func aggregateAvg(name string, entities []datastorestore.Entity, project string) (*datastorepb.Value, error) {
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "avg aggregation requires a property", 400)
	}
	var (
		sum float64
		n   int64
	)
	for _, e := range entities {
		v, ok := e.Properties[name]
		if !ok {
			continue
		}
		switch {
		case v.IntegerValue != nil:
			sum += float64(*v.IntegerValue)
			n++
		case v.DoubleValue != nil:
			sum += *v.DoubleValue
			n++
		default:
			continue
		}
	}
	if n == 0 {
		s := "NULL_VALUE"
		return valueToProto(datastorestore.Value{NullValue: &s}, project), nil
	}
	avg := sum / float64(n)
	return valueToProto(datastorestore.Value{DoubleValue: &avg}, project), nil
}

func (s *Service) BeginTransaction(ctx context.Context, req *datastorepb.BeginTransactionRequest) (*datastorepb.BeginTransactionResponse, error) {
	txn := []byte("txn-" + strconv.FormatInt(atomic.AddInt64(&txnSeq, 1), 10))
	s.txnMu.Lock()
	if s.readSets == nil {
		s.readSets = make(map[string]*readSet)
	}
	s.readSets[string(txn)] = &readSet{reads: make(map[string]datastorestore.ReadRef), start: clock.Now()}
	s.txnMu.Unlock()
	return &datastorepb.BeginTransactionResponse{Transaction: txn}, nil
}

func (s *Service) Rollback(ctx context.Context, req *datastorepb.RollbackRequest) (*datastorepb.RollbackResponse, error) {
	// Idempotent for an unknown/expired transaction, matching real Datastore.
	s.clearReadSet(req.GetTransaction())
	return &datastorepb.RollbackResponse{}, nil
}

func (s *Service) AllocateIds(ctx context.Context, req *datastorepb.AllocateIdsRequest) (*datastorepb.AllocateIdsResponse, error) {
	project := s.project(ctx, req.GetProjectId())
	// AllocateIds only accepts incomplete keys; a complete key is rejected.
	for _, k := range req.GetKeys() {
		_, _, complete, err := canonicalKey(k)
		if err != nil {
			return nil, mapError(err)
		}
		if complete {
			return nil, mapError(model.NewProviderError("InvalidArgument", "allocate ids requires incomplete keys", 400))
		}
	}
	ids, err := s.store.AllocateIDs(ctx, project, len(req.GetKeys()))
	if err != nil {
		return nil, mapError(err)
	}
	resp := &datastorepb.AllocateIdsResponse{}
	for i, k := range req.GetKeys() {
		completed, err := completeKey(k, project, ids[i])
		if err != nil {
			return nil, mapError(err)
		}
		resp.Keys = append(resp.Keys, completed)
	}
	return resp, nil
}

// ReserveIds implements the Datastore ReserveIds RPC: the supplied complete
// keys' numeric IDs will never be handed out by a later AllocateIds. Each key
// is canonicalized; a numeric-ID key advances the project's ID allocator past
// that ID (store.AdvanceIDs), while a name key is a no-op because names never
// collide with the numeric-ID space (the same split advanceAllocator makes).
// An incomplete key is rejected with InvalidArgument, as real Datastore
// requires complete key paths.
//
// The emulator is single-project and single-database; a non-empty DatabaseId
// is rejected (see rejectDatabaseID). The response is empty by design.
func (s *Service) ReserveIds(ctx context.Context, req *datastorepb.ReserveIdsRequest) (*datastorepb.ReserveIdsResponse, error) {
	if err := rejectDatabaseID(req.GetDatabaseId()); err != nil {
		return nil, mapError(err)
	}
	project := s.project(ctx, req.GetProjectId())
	for _, k := range req.GetKeys() {
		key, _, complete, err := canonicalKey(k)
		if err != nil {
			return nil, mapError(err)
		}
		if !complete {
			return nil, mapError(model.NewProviderError("InvalidArgument", "reserve ids requires complete keys", 400))
		}
		if err := s.advanceAllocator(ctx, project, key); err != nil {
			return nil, mapError(err)
		}
	}
	return &datastorepb.ReserveIdsResponse{}, nil
}

// rejectDatabaseID rejects a non-empty request DatabaseId. The emulator
// serves a single default database per project, so a named database cannot be
// honored; failing loud mirrors canonicalKey's rejection of database-scoped
// keys rather than silently reading/writing the default database.
func rejectDatabaseID(databaseID string) error {
	if databaseID != "" {
		return model.NewProviderError("InvalidArgument", "database-scoped requests are not supported", 400)
	}
	return nil
}

// resolveEntity transcodes a mutation entity and, when its key path is
// incomplete, allocates a numeric ID. The bool reports whether an ID was
// allocated (so Commit can echo the resolved key back in MutationResult.Key).
// For an explicitly-keyed entity, the per-project ID allocator is advanced past
// the explicit numeric ID so AllocateIds never reissues an ID already in use.
func (s *Service) resolveEntity(ctx context.Context, project string, p *datastorepb.Entity) (datastorestore.Entity, bool, error) {
	e, err := entityFromProto(p)
	if err != nil {
		return e, false, err
	}
	if e.Key == "" {
		ids, err := s.store.AllocateIDs(ctx, project, 1)
		if err != nil {
			return e, false, err
		}
		e.Key = datastorestore.KeyOfID(e.Kind, ids[0])
		return e, true, nil
	}
	if err := s.advanceAllocator(ctx, project, e.Key); err != nil {
		return e, false, err
	}
	return e, false, nil
}

// advanceAllocator advances the project's ID allocator past an explicitly-used
// numeric ID (name keys are ignored; they never collide with allocated IDs).
func (s *Service) advanceAllocator(ctx context.Context, project, key string) error {
	_, idOrName, ok := datastorestore.SplitKey(key)
	if !ok {
		return nil
	}
	id, _, isID := datastorestore.ParseIDOrName(idOrName)
	if !isID {
		return nil
	}
	return s.store.AdvanceIDs(ctx, project, id)
}

// deleteKey requires a complete key and returns its canonical string.
func deleteKey(k *datastorepb.Key) (string, error) {
	key, _, complete, err := canonicalKey(k)
	if err != nil {
		return "", err
	}
	if !complete {
		return "", model.NewProviderError("InvalidArgument", "delete key is incomplete", 400)
	}
	return key, nil
}

// ─── key transcoding ──────────────────────────────────────────────────────────

// canonicalKey converts a proto Key to the store's canonical key string
// "kind/id-or-name" using the (single, last) path element's kind and
// id-or-name. complete is false for an incomplete key path. Ancestor
// (multi-element) paths and non-default namespace/database scopes are not
// supported and return InvalidArgument rather than silently collapsing to the
// last path element.
func canonicalKey(k *datastorepb.Key) (key, kind string, complete bool, err error) {
	if k == nil {
		return "", "", false, nil
	}
	if pid := k.GetPartitionId(); pid != nil {
		if pid.GetNamespaceId() != "" {
			return "", "", false, model.NewProviderError("InvalidArgument", "namespaced keys are not supported", 400)
		}
		if pid.GetDatabaseId() != "" {
			return "", "", false, model.NewProviderError("InvalidArgument", "database-scoped keys are not supported", 400)
		}
	}
	path := k.GetPath()
	if len(path) == 0 {
		return "", "", false, nil
	}
	if len(path) > 1 {
		return "", "", false, model.NewProviderError("InvalidArgument", "ancestor keys are not supported", 400)
	}
	el := path[0]
	kind = el.GetKind()
	if id, ok := el.GetIdType().(*datastorepb.Key_PathElement_Id); ok {
		return datastorestore.KeyOfID(kind, id.Id), kind, true, nil
	}
	if name, ok := el.GetIdType().(*datastorepb.Key_PathElement_Name); ok {
		return datastorestore.KeyOfName(kind, name.Name), kind, true, nil
	}
	return "", kind, false, nil
}

// keyProto reconstructs a single-element proto Key from the canonical key
// string, tagging the partition with the owning project.
func keyProto(key, project string) *datastorepb.Key {
	kind, idOrName, ok := datastorestore.SplitKey(key)
	if !ok {
		return nil
	}
	el := &datastorepb.Key_PathElement{Kind: kind}
	id, name, isID := datastorestore.ParseIDOrName(idOrName)
	if isID {
		el.IdType = &datastorepb.Key_PathElement_Id{Id: id}
	} else {
		el.IdType = &datastorepb.Key_PathElement_Name{Name: name}
	}
	return &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{ProjectId: project},
		Path:        []*datastorepb.Key_PathElement{el},
	}
}

// completeKey fills the last path element of an incomplete key with the given
// numeric ID, preserving partition and ancestor path elements.
func completeKey(k *datastorepb.Key, project string, id int64) (*datastorepb.Key, error) {
	if k == nil || len(k.GetPath()) == 0 {
		return nil, model.NewProviderError("InvalidArgument", "cannot allocate an id for an empty key path", 400)
	}
	out := &datastorepb.Key{
		PartitionId: k.GetPartitionId(),
		Path:        make([]*datastorepb.Key_PathElement, len(k.GetPath())),
	}
	for i, el := range k.GetPath() {
		out.Path[i] = &datastorepb.Key_PathElement{Kind: el.GetKind()}
		if i < len(k.GetPath())-1 {
			out.Path[i].IdType = el.GetIdType()
		} else {
			out.Path[i].IdType = &datastorepb.Key_PathElement_Id{Id: id}
		}
	}
	if out.PartitionId == nil {
		out.PartitionId = &datastorepb.PartitionId{ProjectId: project}
	} else if out.PartitionId.GetProjectId() == "" {
		out.PartitionId.ProjectId = project
	}
	return out, nil
}

// ─── entity transcoding ───────────────────────────────────────────────────────

func entityFromProto(p *datastorepb.Entity) (datastorestore.Entity, error) {
	e := datastorestore.Entity{Properties: map[string]datastorestore.Value{}}
	if p == nil {
		return e, nil
	}
	if k := p.GetKey(); k != nil {
		key, kind, complete, err := canonicalKey(k)
		if err != nil {
			return e, err
		}
		e.Kind = kind
		if complete {
			e.Key = key
		}
	}
	for name, v := range p.GetProperties() {
		sv, err := valueFromProto(v)
		if err != nil {
			return e, err
		}
		e.Properties[name] = sv
	}
	return e, nil
}

func entityToProto(e datastorestore.Entity, project string) *datastorepb.Entity {
	props := make(map[string]*datastorepb.Value, len(e.Properties))
	for name, v := range e.Properties {
		props[name] = valueToProto(v, project)
	}
	out := &datastorepb.Entity{Properties: props}
	if e.Key != "" {
		out.Key = keyProto(e.Key, project)
	}
	return out
}

// ─── value transcoding ────────────────────────────────────────────────────────

func valueFromProto(p *datastorepb.Value) (datastorestore.Value, error) {
	if p == nil {
		return datastorestore.Value{}, nil
	}
	switch v := p.GetValueType().(type) {
	case *datastorepb.Value_NullValue:
		s := "NULL_VALUE"
		return datastorestore.Value{NullValue: &s}, nil
	case *datastorepb.Value_BooleanValue:
		b := v.BooleanValue
		return datastorestore.Value{BooleanValue: &b}, nil
	case *datastorepb.Value_IntegerValue:
		n := v.IntegerValue
		return datastorestore.Value{IntegerValue: &n}, nil
	case *datastorepb.Value_DoubleValue:
		f := v.DoubleValue
		return datastorestore.Value{DoubleValue: &f}, nil
	case *datastorepb.Value_TimestampValue:
		s := v.TimestampValue.AsTime().UTC().Format(time.RFC3339Nano)
		return datastorestore.Value{TimestampValue: &s}, nil
	case *datastorepb.Value_KeyValue:
		key, _, complete, err := canonicalKey(v.KeyValue)
		if err != nil {
			return datastorestore.Value{}, err
		}
		if !complete {
			return datastorestore.Value{}, model.NewProviderError("InvalidArgument", "key value is incomplete", 400)
		}
		return datastorestore.Value{KeyValue: &key}, nil
	case *datastorepb.Value_StringValue:
		s := v.StringValue
		return datastorestore.Value{StringValue: &s}, nil
	case *datastorepb.Value_BlobValue:
		return datastorestore.Value{BlobValue: v.BlobValue}, nil
	case *datastorepb.Value_GeoPointValue:
		g := datastorestore.GeoPoint{
			Latitude:  v.GeoPointValue.GetLatitude(),
			Longitude: v.GeoPointValue.GetLongitude(),
		}
		return datastorestore.Value{GeoPointValue: &g}, nil
	case *datastorepb.Value_EntityValue:
		e, err := entityFromProto(v.EntityValue)
		if err != nil {
			return datastorestore.Value{}, err
		}
		return datastorestore.Value{EntityValue: &e}, nil
	case *datastorepb.Value_ArrayValue:
		arr := datastorestore.ArrayValue{Values: make([]datastorestore.Value, 0, len(v.ArrayValue.GetValues()))}
		for _, x := range v.ArrayValue.GetValues() {
			xv, err := valueFromProto(x)
			if err != nil {
				return datastorestore.Value{}, err
			}
			arr.Values = append(arr.Values, xv)
		}
		return datastorestore.Value{ArrayValue: &arr}, nil
	}
	return datastorestore.Value{}, nil
}

func valueToProto(v datastorestore.Value, project string) *datastorepb.Value {
	switch {
	case v.NullValue != nil:
		return &datastorepb.Value{ValueType: &datastorepb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}
	case v.BooleanValue != nil:
		return &datastorepb.Value{ValueType: &datastorepb.Value_BooleanValue{BooleanValue: *v.BooleanValue}}
	case v.IntegerValue != nil:
		return &datastorepb.Value{ValueType: &datastorepb.Value_IntegerValue{IntegerValue: *v.IntegerValue}}
	case v.DoubleValue != nil:
		return &datastorepb.Value{ValueType: &datastorepb.Value_DoubleValue{DoubleValue: *v.DoubleValue}}
	case v.TimestampValue != nil:
		t, err := time.Parse(time.RFC3339Nano, *v.TimestampValue)
		if err != nil {
			t = time.Time{}
		}
		return &datastorepb.Value{ValueType: &datastorepb.Value_TimestampValue{TimestampValue: timestamppb.New(t)}}
	case v.KeyValue != nil:
		return &datastorepb.Value{ValueType: &datastorepb.Value_KeyValue{KeyValue: keyProto(*v.KeyValue, project)}}
	case v.StringValue != nil:
		return &datastorepb.Value{ValueType: &datastorepb.Value_StringValue{StringValue: *v.StringValue}}
	case v.BlobValue != nil:
		return &datastorepb.Value{ValueType: &datastorepb.Value_BlobValue{BlobValue: v.BlobValue}}
	case v.GeoPointValue != nil:
		return &datastorepb.Value{ValueType: &datastorepb.Value_GeoPointValue{GeoPointValue: &latlng.LatLng{
			Latitude:  v.GeoPointValue.Latitude,
			Longitude: v.GeoPointValue.Longitude,
		}}}
	case v.EntityValue != nil:
		return &datastorepb.Value{ValueType: &datastorepb.Value_EntityValue{EntityValue: entityToProto(*v.EntityValue, project)}}
	case v.ArrayValue != nil:
		arr := &datastorepb.ArrayValue{Values: make([]*datastorepb.Value, 0, len(v.ArrayValue.Values))}
		for _, x := range v.ArrayValue.Values {
			arr.Values = append(arr.Values, valueToProto(x, project))
		}
		return &datastorepb.Value{ValueType: &datastorepb.Value_ArrayValue{ArrayValue: arr}}
	}
	return &datastorepb.Value{}
}

// ─── query filter evaluation ──────────────────────────────────────────────────

// matchesFilter evaluates a Query.Filter against an entity. It supports
// PropertyFilter EQUAL, NOT_EQUAL, IN, and the four comparison operators
// (LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL), and
// CompositeFilter AND over those property filters. Unsupported operators
// (HAS_ANCESTOR, NOT_IN, unspecified/unknown) and CompositeFilter OR return an
// InvalidArgument error rather than matching everything.
func matchesFilter(e datastorestore.Entity, f *datastorepb.Filter) (bool, error) {
	if f == nil {
		return true, nil
	}
	switch ft := f.GetFilterType().(type) {
	case *datastorepb.Filter_PropertyFilter:
		return matchesPropertyFilter(e, ft.PropertyFilter)
	case *datastorepb.Filter_CompositeFilter:
		cf := ft.CompositeFilter
		if cf == nil {
			return true, nil
		}
		switch cf.GetOp() {
		case datastorepb.CompositeFilter_AND:
			for _, sub := range cf.GetFilters() {
				match, err := matchesFilter(e, sub)
				if err != nil {
					return false, err
				}
				if !match {
					return false, nil
				}
			}
			return true, nil
		default:
			return false, unsupportedQueryOperator(cf.GetOp().String())
		}
	default:
		return false, unsupportedQueryOperator("unknown filter")
	}
}

// unsupportedQueryOperator returns an InvalidArgument error for a filter
// operator the emulator does not implement (fail closed, never match-all).
func unsupportedQueryOperator(op string) error {
	return model.NewProviderError("InvalidArgument", "unsupported query operator: "+op, 400)
}

func matchesPropertyFilter(e datastorestore.Entity, pf *datastorepb.PropertyFilter) (bool, error) {
	if pf == nil || pf.GetProperty() == nil {
		return false, unsupportedQueryOperator("missing property filter")
	}
	prop := pf.GetProperty().GetName()
	val, ok := e.Properties[prop]

	filterVal, err := valueFromProto(pf.GetValue())
	if err != nil {
		return false, err
	}

	switch pf.GetOp() {
	case datastorepb.PropertyFilter_EQUAL:
		return ok && valueEqual(val, filterVal), nil
	case datastorepb.PropertyFilter_NOT_EQUAL:
		return ok && !valueEqual(val, filterVal), nil
	case datastorepb.PropertyFilter_IN:
		if !ok || filterVal.ArrayValue == nil {
			return false, nil
		}
		for _, el := range filterVal.ArrayValue.Values {
			if valueEqual(val, el) {
				return true, nil
			}
		}
		return false, nil
	case datastorepb.PropertyFilter_LESS_THAN,
		datastorepb.PropertyFilter_LESS_THAN_OR_EQUAL,
		datastorepb.PropertyFilter_GREATER_THAN,
		datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL:
		if !ok {
			return false, nil
		}
		c, comparable := valueCompare(val, filterVal)
		if !comparable {
			return false, nil
		}
		switch pf.GetOp() {
		case datastorepb.PropertyFilter_LESS_THAN:
			return c < 0, nil
		case datastorepb.PropertyFilter_LESS_THAN_OR_EQUAL:
			return c <= 0, nil
		case datastorepb.PropertyFilter_GREATER_THAN:
			return c > 0, nil
		default:
			return c >= 0, nil
		}
	default:
		return false, unsupportedQueryOperator(pf.GetOp().String())
	}
}

// numericValue reports the numeric value of v (integer or double) as a float64.
func numericValue(v datastorestore.Value) (float64, bool) {
	if v.IntegerValue != nil {
		return float64(*v.IntegerValue), true
	}
	if v.DoubleValue != nil {
		return *v.DoubleValue, true
	}
	return 0, false
}

// valueCompare compares two values for ordering, returning -1, 0, or 1. ok is
// false when the two values are not orderable (different types, or an
// unsupported type such as geo point/entity/array). Integers and doubles
// compare numerically, matching Datastore semantics.
func valueCompare(a, b datastorestore.Value) (int, bool) {
	an, aNum := numericValue(a)
	bn, bNum := numericValue(b)
	if aNum || bNum {
		if !aNum || !bNum {
			return 0, false
		}
		switch {
		case an < bn:
			return -1, true
		case an > bn:
			return 1, true
		default:
			return 0, true
		}
	}
	switch {
	case a.StringValue != nil || b.StringValue != nil:
		if a.StringValue == nil || b.StringValue == nil {
			return 0, false
		}
		return strings.Compare(*a.StringValue, *b.StringValue), true
	case a.BooleanValue != nil || b.BooleanValue != nil:
		if a.BooleanValue == nil || b.BooleanValue == nil {
			return 0, false
		}
		switch {
		case *a.BooleanValue == *b.BooleanValue:
			return 0, true
		case !*a.BooleanValue && *b.BooleanValue:
			return -1, true
		default:
			return 1, true
		}
	case a.TimestampValue != nil || b.TimestampValue != nil:
		if a.TimestampValue == nil || b.TimestampValue == nil {
			return 0, false
		}
		at, errA := time.Parse(time.RFC3339Nano, *a.TimestampValue)
		bt, errB := time.Parse(time.RFC3339Nano, *b.TimestampValue)
		if errA != nil || errB != nil {
			return 0, false
		}
		switch {
		case at.Before(bt):
			return -1, true
		case at.After(bt):
			return 1, true
		default:
			return 0, true
		}
	case a.KeyValue != nil || b.KeyValue != nil:
		if a.KeyValue == nil || b.KeyValue == nil {
			return 0, false
		}
		return strings.Compare(*a.KeyValue, *b.KeyValue), true
	case a.BlobValue != nil || b.BlobValue != nil:
		if a.BlobValue == nil || b.BlobValue == nil {
			return 0, false
		}
		return bytes.Compare(a.BlobValue, b.BlobValue), true
	}
	return 0, false
}

func valueEqual(a, b datastorestore.Value) bool {
	an, aNum := numericValue(a)
	bn, bNum := numericValue(b)
	if aNum || bNum {
		return aNum && bNum && an == bn
	}
	switch {
	case a.NullValue != nil || b.NullValue != nil:
		return a.NullValue != nil && b.NullValue != nil
	case a.BooleanValue != nil || b.BooleanValue != nil:
		return a.BooleanValue != nil && b.BooleanValue != nil && *a.BooleanValue == *b.BooleanValue
	case a.StringValue != nil || b.StringValue != nil:
		return a.StringValue != nil && b.StringValue != nil && *a.StringValue == *b.StringValue
	case a.TimestampValue != nil || b.TimestampValue != nil:
		return a.TimestampValue != nil && b.TimestampValue != nil && *a.TimestampValue == *b.TimestampValue
	case a.KeyValue != nil || b.KeyValue != nil:
		return a.KeyValue != nil && b.KeyValue != nil && *a.KeyValue == *b.KeyValue
	case a.BlobValue != nil || b.BlobValue != nil:
		return a.BlobValue != nil && b.BlobValue != nil && bytes.Equal(a.BlobValue, b.BlobValue)
	case a.GeoPointValue != nil || b.GeoPointValue != nil:
		return a.GeoPointValue != nil && b.GeoPointValue != nil && *a.GeoPointValue == *b.GeoPointValue
	case a.EntityValue != nil || b.EntityValue != nil:
		if a.EntityValue == nil || b.EntityValue == nil {
			return false
		}
		return entityEqual(*a.EntityValue, *b.EntityValue)
	case a.ArrayValue != nil || b.ArrayValue != nil:
		if a.ArrayValue == nil || b.ArrayValue == nil {
			return false
		}
		if len(a.ArrayValue.Values) != len(b.ArrayValue.Values) {
			return false
		}
		for i := range a.ArrayValue.Values {
			if !valueEqual(a.ArrayValue.Values[i], b.ArrayValue.Values[i]) {
				return false
			}
		}
		return true
	}
	return false
}

func entityEqual(a, b datastorestore.Entity) bool {
	if a.Key != b.Key {
		return false
	}
	if len(a.Properties) != len(b.Properties) {
		return false
	}
	for k, av := range a.Properties {
		bv, ok := b.Properties[k]
		if !ok || !valueEqual(av, bv) {
			return false
		}
	}
	return true
}
