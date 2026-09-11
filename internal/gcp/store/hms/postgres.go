package hms

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	"jaiscloud/internal/clock"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore implements Store against jc_hms_databases / jc_hms_tables /
// jc_hms_locks.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore returns a Postgres-backed store.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

func jsonObj(v any) any {
	if v == nil {
		return json.RawMessage("{}")
	}
	b, _ := json.Marshal(v)
	return json.RawMessage(b)
}

// --- Databases ---

func (s *PostgresStore) CreateDatabase(ctx context.Context, db Database) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_hms_databases (name, location_uri, parameters, description, owner)
		VALUES ($1,$2,$3,$4,$5)
	`, db.Name, db.LocationURI, jsonObj(db.Parameters), db.Description, db.Owner)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrDatabaseExists
		}
		return err
	}
	return nil
}

func scanDatabase(row pgx.Row) (Database, error) {
	var db Database
	var params []byte
	err := row.Scan(&db.Name, &db.LocationURI, &params, &db.Description, &db.Owner)
	if err != nil {
		return Database{}, err
	}
	db.Parameters = scanStrMap(params)
	return db, nil
}

func scanStrMap(b []byte) map[string]string {
	if len(b) == 0 {
		return map[string]string{}
	}
	m := map[string]string{}
	_ = json.Unmarshal(b, &m)
	if m == nil {
		m = map[string]string{}
	}
	return m
}

const dbColumns = `name, location_uri, parameters, description, owner`

func (s *PostgresStore) GetDatabase(ctx context.Context, name string) (Database, error) {
	db, err := scanDatabase(s.pool.QueryRow(ctx, `
		SELECT `+dbColumns+` FROM jc_hms_databases WHERE name=$1
	`, name))
	if errors.Is(err, pgx.ErrNoRows) {
		return Database{}, ErrDatabaseNotFound
	}
	return db, err
}

func (s *PostgresStore) ListDatabases(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT name FROM jc_hms_databases ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		names = append(names, n)
	}
	sort.Strings(names)
	return names, rows.Err()
}

func (s *PostgresStore) DropDatabase(ctx context.Context, name string, cascade bool) error {
	// Non-cascade drop relies on the FK (ON DELETE RESTRICT) for atomicity:
	// a DELETE on a database still referenced by a table fails with 23503,
	// closing the check-then-act race. Cascade drops tables first in a
	// transaction, then the database.
	if cascade {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, `DELETE FROM jc_hms_tables WHERE db_name=$1`, name); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `DELETE FROM jc_hms_databases WHERE name=$1`, name)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrDatabaseNotFound
		}
		return tx.Commit(ctx)
	}
	tag, err := s.pool.Exec(ctx, `DELETE FROM jc_hms_databases WHERE name=$1`, name)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			return ErrDatabaseNotEmpty
		}
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrDatabaseNotFound
	}
	return nil
}

func (s *PostgresStore) AlterDatabase(ctx context.Context, name string, db Database) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_hms_databases SET location_uri=$2, parameters=$3, description=$4, owner=$5
		WHERE name=$1
	`, name, db.LocationURI, jsonObj(db.Parameters), db.Description, db.Owner)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrDatabaseNotFound
	}
	return nil
}

// --- Tables ---

func (s *PostgresStore) CreateTable(ctx context.Context, dbName, tableName string, t Table) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_hms_tables (db_name, table_name, table_json)
		VALUES ($1,$2,$3)
	`, dbName, tableName, jsonObj(json.RawMessage(t.TableJSON)))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrTableExists
		}
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			return ErrDatabaseNotFound
		}
		return err
	}
	return nil
}

func scanTable(row pgx.Row) (Table, error) {
	var t Table
	var b []byte
	err := row.Scan(&t.DBName, &t.TableName, &b)
	if err != nil {
		return Table{}, err
	}
	t.TableJSON = json.RawMessage(b)
	return t, nil
}

const tableColumns = `db_name, table_name, table_json`

func (s *PostgresStore) GetTable(ctx context.Context, dbName, tableName string) (Table, error) {
	t, err := scanTable(s.pool.QueryRow(ctx, `
		SELECT `+tableColumns+` FROM jc_hms_tables WHERE db_name=$1 AND table_name=$2
	`, dbName, tableName))
	if errors.Is(err, pgx.ErrNoRows) {
		return Table{}, ErrTableNotFound
	}
	return t, err
}

func (s *PostgresStore) ListTables(ctx context.Context, dbName string) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT table_name FROM jc_hms_tables WHERE db_name=$1 ORDER BY table_name
	`, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		names = append(names, n)
	}
	sort.Strings(names)
	return names, rows.Err()
}

func (s *PostgresStore) DropTable(ctx context.Context, dbName, tableName string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM jc_hms_tables WHERE db_name=$1 AND table_name=$2`, dbName, tableName)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrTableNotFound
	}
	return nil
}

// AlterTable mirrors MemoryStore's version: a Serializable transaction with
// SELECT ... FOR UPDATE row-locks the table for the duration of mutate, so a
// concurrent commit to the same table blocks instead of silently losing the
// other's update.
func (s *PostgresStore) AlterTable(ctx context.Context, dbName, tableName string, mutate func(Table) (Table, error)) (Table, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Table{}, err
	}
	defer tx.Rollback(ctx)

	current, err := scanTable(tx.QueryRow(ctx, `
		SELECT `+tableColumns+` FROM jc_hms_tables WHERE db_name=$1 AND table_name=$2 FOR UPDATE
	`, dbName, tableName))
	if errors.Is(err, pgx.ErrNoRows) {
		return Table{}, ErrTableNotFound
	}
	if err != nil {
		return Table{}, err
	}

	next, err := mutate(current)
	if err != nil {
		return Table{}, err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE jc_hms_tables SET table_json=$3 WHERE db_name=$1 AND table_name=$2
	`, dbName, tableName, jsonObj(json.RawMessage(next.TableJSON))); err != nil {
		return Table{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Table{}, err
	}
	next.DBName = dbName
	next.TableName = tableName
	return next, nil
}

// RenameTable is atomic: a Serializable transaction row-locks the source (and
// probes the destination) so a concurrent commit/rename can't race it.
func (s *PostgresStore) RenameTable(ctx context.Context, srcDB, srcName, dstDB, dstName string, t Table) (Table, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Table{}, err
	}
	defer tx.Rollback(ctx)

	var one int
	if err := tx.QueryRow(ctx, `SELECT 1 FROM jc_hms_tables WHERE db_name=$1 AND table_name=$2 FOR UPDATE`, srcDB, srcName).Scan(&one); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Table{}, ErrTableNotFound
		}
		return Table{}, err
	}
	var dbOne int
	if err := tx.QueryRow(ctx, `SELECT 1 FROM jc_hms_databases WHERE name=$1 FOR UPDATE`, dstDB).Scan(&dbOne); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Table{}, ErrDatabaseNotFound
		}
		return Table{}, err
	}

	err = tx.QueryRow(ctx, `SELECT 1 FROM jc_hms_tables WHERE db_name=$1 AND table_name=$2 FOR UPDATE`, dstDB, dstName).Scan(&one)
	if err == nil {
		return Table{}, ErrTableExists
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Table{}, err
	}

	if _, err := tx.Exec(ctx, `DELETE FROM jc_hms_tables WHERE db_name=$1 AND table_name=$2`, srcDB, srcName); err != nil {
		return Table{}, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO jc_hms_tables (db_name, table_name, table_json)
		VALUES ($1,$2,$3)
	`, dstDB, dstName, jsonObj(json.RawMessage(t.TableJSON))); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return Table{}, ErrTableExists
		}
		return Table{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Table{}, err
	}
	t.DBName = dstDB
	t.TableName = dstName
	return t, nil
}

// --- Locks ---

func (s *PostgresStore) Lock(ctx context.Context, l Lock) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO jc_hms_locks (state, db_name, table_name, user_name, hostname, create_time)
		VALUES ($1,$2,$3,$4,$5,$6) RETURNING lock_id
	`, int32(LockStateAcquired), l.DBName, l.TableName, l.User, l.Hostname, clock.Now()).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (s *PostgresStore) CheckLock(ctx context.Context, id int64) (LockState, error) {
	var state int32
	err := s.pool.QueryRow(ctx, `SELECT state FROM jc_hms_locks WHERE lock_id=$1`, id).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return LockStateNotAcquired, nil
	}
	if err != nil {
		return 0, err
	}
	return LockState(state), nil
}

func (s *PostgresStore) Unlock(ctx context.Context, id int64) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM jc_hms_locks WHERE lock_id=$1`, id)
	return err
}

func (s *PostgresStore) Reset(ctx context.Context) {
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_hms_tables`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_hms_databases`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_hms_locks`)
}
