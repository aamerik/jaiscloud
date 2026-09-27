package functions

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"

	"jaiscloud/internal/clock"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore implements Store against jc_functions.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore returns a Postgres-backed store.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

// nullableJSON marshals v to JSONB, or returns nil (SQL NULL) when v is nil.
// A typed-nil pointer (e.g. a nil *EventTrigger or *Function) is also nil: it
// would otherwise marshal to the JSON literal "null", which reads back as a
// non-nil zero value (a delete operation would then render a Function response
// instead of google.protobuf.Empty).
func nullableJSON(v any) any {
	if v == nil {
		return nil
	}
	if rv := reflect.ValueOf(v); rv.Kind() == reflect.Ptr && rv.IsNil() {
		return nil
	}
	b, _ := json.Marshal(v)
	return json.RawMessage(b)
}

func (s *PostgresStore) CreateFunction(ctx context.Context, projectID, location, id string, f Function) error {
	if f.CreateTime.IsZero() {
		f.CreateTime = clock.Now()
	}
	if f.UpdateTime.IsZero() {
		f.UpdateTime = f.CreateTime
	}
	env, _ := json.Marshal(f.EnvironmentVariables)
	labels, _ := json.Marshal(f.Labels)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_functions
			(project_id, location, function_id, runtime, entry_point, source_upload_url, source_archive_url,
			 https_trigger_url, event_trigger, environment_variables, status, create_time, update_time, labels,
			 available_memory_mb, timeout, description, source_sha256, source_size, source_blob_key)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
	`, projectID, location, id, f.Runtime, f.EntryPoint, f.SourceUploadURL, f.SourceArchiveURL,
		f.HttpsTriggerURL, nullableJSON(f.EventTrigger), json.RawMessage(env), f.Status, f.CreateTime, f.UpdateTime,
		json.RawMessage(labels), f.AvailableMemoryMB, f.Timeout, f.Description,
		f.SourceSHA256, f.SourceSize, f.SourceBlobKey)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

func scanFunction(row pgx.Row) (Function, error) {
	var f Function
	var eventTrigger, env, labels []byte
	err := row.Scan(&f.ID, &f.Location, &f.Runtime, &f.EntryPoint, &f.SourceUploadURL, &f.SourceArchiveURL,
		&f.HttpsTriggerURL, &eventTrigger, &env, &f.Status, &f.CreateTime, &f.UpdateTime, &labels,
		&f.AvailableMemoryMB, &f.Timeout, &f.Description,
		&f.SourceSHA256, &f.SourceSize, &f.SourceBlobKey)
	if err != nil {
		return Function{}, err
	}
	if len(eventTrigger) > 0 {
		json.Unmarshal(eventTrigger, &f.EventTrigger)
	}
	json.Unmarshal(env, &f.EnvironmentVariables)
	json.Unmarshal(labels, &f.Labels)
	return f, nil
}

func (s *PostgresStore) GetFunction(ctx context.Context, projectID, location, id string) (Function, error) {
	f, err := scanFunction(s.pool.QueryRow(ctx, `
		SELECT function_id, location, runtime, entry_point, source_upload_url, source_archive_url,
		       https_trigger_url, event_trigger, environment_variables, status, create_time, update_time, labels,
		       available_memory_mb, timeout, description, source_sha256, source_size, source_blob_key
		FROM jc_functions WHERE project_id=$1 AND location=$2 AND function_id=$3
	`, projectID, location, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Function{}, ErrNoSuchFunction
	}
	return f, err
}

func (s *PostgresStore) UpdateFunction(ctx context.Context, projectID, location, id string, f Function) error {
	env, _ := json.Marshal(f.EnvironmentVariables)
	labels, _ := json.Marshal(f.Labels)
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_functions SET runtime=$4, entry_point=$5, source_upload_url=$6, source_archive_url=$7,
		       https_trigger_url=$8, event_trigger=$9, environment_variables=$10, status=$11, update_time=$12, labels=$13,
		       available_memory_mb=$14, timeout=$15, description=$16,
		       source_sha256=$17, source_size=$18, source_blob_key=$19
		WHERE project_id=$1 AND location=$2 AND function_id=$3
	`, projectID, location, id, f.Runtime, f.EntryPoint, f.SourceUploadURL, f.SourceArchiveURL,
		f.HttpsTriggerURL, nullableJSON(f.EventTrigger), json.RawMessage(env), f.Status, f.UpdateTime,
		json.RawMessage(labels), f.AvailableMemoryMB, f.Timeout, f.Description,
		f.SourceSHA256, f.SourceSize, f.SourceBlobKey)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchFunction
	}
	return nil
}

// UpdateFunctionAtomic mirrors MemoryStore's version: a Serializable
// transaction with SELECT ... FOR UPDATE row-locks the function for the
// duration of mutate, so a concurrent UpdateFunctionAtomic on the same
// function blocks until this transaction commits or rolls back, instead of
// racing to silently overwrite this call's write. See
// store/firestore/postgres.go's Commit for the same convention.
func (s *PostgresStore) UpdateFunctionAtomic(ctx context.Context, projectID, location, id string, mutate func(Function) (Function, error)) (Function, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Function{}, err
	}
	defer tx.Rollback(ctx)

	current, err := scanFunction(tx.QueryRow(ctx, `
		SELECT function_id, location, runtime, entry_point, source_upload_url, source_archive_url,
		       https_trigger_url, event_trigger, environment_variables, status, create_time, update_time, labels,
		       available_memory_mb, timeout, description, source_sha256, source_size, source_blob_key
		FROM jc_functions WHERE project_id=$1 AND location=$2 AND function_id=$3 FOR UPDATE
	`, projectID, location, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Function{}, ErrNoSuchFunction
	}
	if err != nil {
		return Function{}, err
	}

	next, err := mutate(current)
	if err != nil {
		return Function{}, err
	}

	env, _ := json.Marshal(next.EnvironmentVariables)
	labels, _ := json.Marshal(next.Labels)
	tag, err := tx.Exec(ctx, `
		UPDATE jc_functions SET runtime=$4, entry_point=$5, source_upload_url=$6, source_archive_url=$7,
		       https_trigger_url=$8, event_trigger=$9, environment_variables=$10, status=$11, update_time=$12, labels=$13,
		       available_memory_mb=$14, timeout=$15, description=$16,
		       source_sha256=$17, source_size=$18, source_blob_key=$19
		WHERE project_id=$1 AND location=$2 AND function_id=$3
	`, projectID, location, id, next.Runtime, next.EntryPoint, next.SourceUploadURL, next.SourceArchiveURL,
		next.HttpsTriggerURL, nullableJSON(next.EventTrigger), json.RawMessage(env), next.Status, next.UpdateTime,
		json.RawMessage(labels), next.AvailableMemoryMB, next.Timeout, next.Description,
		next.SourceSHA256, next.SourceSize, next.SourceBlobKey)
	if err != nil {
		return Function{}, err
	}
	if tag.RowsAffected() == 0 {
		return Function{}, ErrNoSuchFunction
	}
	if err := tx.Commit(ctx); err != nil {
		return Function{}, err
	}
	next.ID = id
	next.Location = location
	return next, nil
}

func (s *PostgresStore) DeleteFunction(ctx context.Context, projectID, location, id string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM jc_functions WHERE project_id=$1 AND location=$2 AND function_id=$3`, projectID, location, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchFunction
	}
	return nil
}

func (s *PostgresStore) ListFunctions(ctx context.Context, projectID, location string) ([]Function, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT function_id, location, runtime, entry_point, source_upload_url, source_archive_url,
		       https_trigger_url, event_trigger, environment_variables, status, create_time, update_time, labels,
		       available_memory_mb, timeout, description, source_sha256, source_size, source_blob_key
		FROM jc_functions WHERE project_id=$1 AND location=$2 ORDER BY function_id
	`, projectID, location)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Function
	for rows.Next() {
		f, err := scanFunction(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, f)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, rows.Err()
}

func (s *PostgresStore) ListFunctionsAllLocations(ctx context.Context, projectID string) ([]Function, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT function_id, location, runtime, entry_point, source_upload_url, source_archive_url,
		       https_trigger_url, event_trigger, environment_variables, status, create_time, update_time, labels,
		       available_memory_mb, timeout, description, source_sha256, source_size, source_blob_key
		FROM jc_functions WHERE project_id=$1 ORDER BY location, function_id
	`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Function
	for rows.Next() {
		f, err := scanFunction(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, f)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Location != result[j].Location {
			return result[i].Location < result[j].Location
		}
		return result[i].ID < result[j].ID
	})
	return result, rows.Err()
}

func (s *PostgresStore) Reset(ctx context.Context) {
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_functions`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_functions_operations`)
}

// --- Operations ---

func (s *PostgresStore) CreateOperation(ctx context.Context, projectID, location string, op Operation) error {
	if op.CreateTime.IsZero() {
		op.CreateTime = clock.Now()
	}
	if op.EndTime.IsZero() {
		op.EndTime = op.CreateTime
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_functions_operations
			(project_id, location, operation_id, done, verb, target, function, create_time, end_time)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
	`, projectID, location, op.ID, op.Done, op.Verb, op.Target, nullableJSON(op.Function), op.CreateTime, op.EndTime)
	return err
}

func scanOperation(row pgx.Row) (Operation, error) {
	var op Operation
	var function []byte
	err := row.Scan(&op.ID, &op.Location, &op.Done, &op.Verb, &op.Target, &function, &op.CreateTime, &op.EndTime)
	if err != nil {
		return Operation{}, err
	}
	if len(function) > 0 {
		// Unmarshal into a pointer so a JSON null (a delete operation's absent
		// response snapshot) stays nil rather than yielding a zero Function.
		var f *Function
		if err := json.Unmarshal(function, &f); err != nil {
			return Operation{}, err
		}
		op.Function = f
	}
	return op, nil
}

func (s *PostgresStore) GetOperation(ctx context.Context, projectID, location, id string) (Operation, error) {
	op, err := scanOperation(s.pool.QueryRow(ctx, `
		SELECT operation_id, location, done, verb, target, function, create_time, end_time
		FROM jc_functions_operations WHERE project_id=$1 AND location=$2 AND operation_id=$3
	`, projectID, location, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Operation{}, ErrNoSuchOperation
	}
	return op, err
}

func (s *PostgresStore) DeleteOperation(ctx context.Context, projectID, location, id string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM jc_functions_operations WHERE project_id=$1 AND location=$2 AND operation_id=$3`, projectID, location, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchOperation
	}
	return nil
}

func (s *PostgresStore) ListOperations(ctx context.Context, projectID, location string) ([]Operation, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT operation_id, location, done, verb, target, function, create_time, end_time
		FROM jc_functions_operations WHERE project_id=$1 AND location=$2 ORDER BY operation_id
	`, projectID, location)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Operation
	for rows.Next() {
		op, err := scanOperation(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, op)
	}
	return result, rows.Err()
}
