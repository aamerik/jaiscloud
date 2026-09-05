package kms

import (
	"context"
	"errors"
	"sort"
	"strconv"

	"jaiscloud/internal/clock"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore implements Store against jc_kms_keyrings / jc_kms_cryptokeys /
// jc_kms_cryptokey_versions.
type PostgresStore struct {
	pool *pgxpool.Pool
	kek  []byte // optional 32-byte KEK; when set the DEK is wrapped at rest
}

// NewPostgresStore returns a Postgres-backed store.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

// SetKEK sets the 32-byte KEK used to wrap the server DEK at rest. When unset
// the DEK is stored plaintext (dev mode), mirroring AWS key bootstrap.
func (s *PostgresStore) SetKEK(kek []byte) { s.kek = kek }

// dek loads the server DEK from jc_kms_dek, creating it on first use. It
// protects key material at rest (mirrors AWS key bootstrap.LoadOrCreateDEK),
// wrapping the DEK with the KEK when one is configured.
func (s *PostgresStore) dek(ctx context.Context) ([]byte, error) {
	var blob []byte
	err := s.pool.QueryRow(ctx, `SELECT dek FROM jc_kms_dek WHERE id = 1`).Scan(&blob)
	if err == nil {
		return s.unwrapDEK(blob)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	dek, err := Generate32()
	if err != nil {
		return nil, err
	}
	storeBlob := plaintextDEKBlob(dek)
	if len(s.kek) > 0 {
		if storeBlob, err = WrapDEK(s.kek, dek); err != nil {
			return nil, err
		}
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO jc_kms_dek (id, dek) VALUES (1, $1)`, storeBlob); err != nil {
		// Concurrent process created it first — re-read and unwrap.
		if rerr := s.pool.QueryRow(ctx, `SELECT dek FROM jc_kms_dek WHERE id = 1`).Scan(&blob); rerr != nil {
			return nil, rerr
		}
		return s.unwrapDEK(blob)
	}
	return dek, nil
}

// unwrapDEK returns the raw DEK from a stored blob, honoring the KEK and the
// version byte (plaintext 0x00 vs AES-GCM 0x01).
func (s *PostgresStore) unwrapDEK(blob []byte) ([]byte, error) {
	if len(s.kek) > 0 {
		return UnwrapDEK(s.kek, blob)
	}
	if len(blob) > 0 && blob[0] == versionPlaintext {
		return blob[1:], nil
	}
	return blob, nil // legacy raw plaintext
}

func (s *PostgresStore) CreateKeyRing(ctx context.Context, projectID, location, id string, kr KeyRing) error {
	if kr.CreateTime.IsZero() {
		kr.CreateTime = clock.Now()
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_kms_keyrings (project_id, location, keyring_id, create_time)
		VALUES ($1,$2,$3,$4)
	`, projectID, location, id, kr.CreateTime)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

func (s *PostgresStore) GetKeyRing(ctx context.Context, projectID, location, id string) (KeyRing, error) {
	var kr KeyRing
	err := s.pool.QueryRow(ctx, `
		SELECT location, keyring_id, create_time FROM jc_kms_keyrings WHERE project_id=$1 AND location=$2 AND keyring_id=$3
	`, projectID, location, id).Scan(&kr.Location, &kr.ID, &kr.CreateTime)
	if errors.Is(err, pgx.ErrNoRows) {
		return KeyRing{}, ErrNoSuchKeyRing
	}
	if err != nil {
		return KeyRing{}, err
	}
	return kr, nil
}

func (s *PostgresStore) ListKeyRings(ctx context.Context, projectID, location string) ([]KeyRing, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT location, keyring_id, create_time FROM jc_kms_keyrings WHERE project_id=$1 AND location=$2 ORDER BY keyring_id
	`, projectID, location)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []KeyRing
	for rows.Next() {
		var kr KeyRing
		if err := rows.Scan(&kr.Location, &kr.ID, &kr.CreateTime); err != nil {
			return nil, err
		}
		result = append(result, kr)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, rows.Err()
}

func (s *PostgresStore) CreateCryptoKey(ctx context.Context, projectID, location, keyringID, id string, ck CryptoKey) error {
	if ck.CreateTime.IsZero() {
		ck.CreateTime = clock.Now()
	}
	dek, err := s.dek(ctx)
	if err != nil {
		return err
	}
	v, err := wrapVersionMaterial(dek, id, ck.Algorithm)
	if err != nil {
		return err
	}
	if ck.Algorithm == "" {
		ck.Algorithm = v.Algorithm
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		INSERT INTO jc_kms_cryptokeys (project_id, location, keyring_id, key_id, purpose, create_time, primary_version, algorithm, next_version)
		VALUES ($1,$2,$3,$4,$5,$6,'1',$7,2)
	`, projectID, location, keyringID, id, ck.Purpose, ck.CreateTime, ck.Algorithm); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO jc_kms_cryptokey_versions (project_id, location, keyring_id, key_id, version, state, algorithm, create_time, key_material, private_key, public_key)
		VALUES ($1,$2,$3,$4,'1','ENABLED',$5,$6,$7,$8,$9)
	`, projectID, location, keyringID, id, v.Algorithm, ck.CreateTime, v.KeyMaterial, v.PrivateKey, v.PublicKey); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *PostgresStore) GetCryptoKey(ctx context.Context, projectID, location, keyringID, id string) (CryptoKey, error) {
	var ck CryptoKey
	err := s.pool.QueryRow(ctx, `
		SELECT location, keyring_id, key_id, purpose, create_time, primary_version, algorithm
		FROM jc_kms_cryptokeys WHERE project_id=$1 AND location=$2 AND keyring_id=$3 AND key_id=$4
	`, projectID, location, keyringID, id).Scan(&ck.Location, &ck.KeyRingID, &ck.ID, &ck.Purpose, &ck.CreateTime, &ck.PrimaryVersion, &ck.Algorithm)
	if errors.Is(err, pgx.ErrNoRows) {
		return CryptoKey{}, ErrNoSuchCryptoKey
	}
	if err != nil {
		return CryptoKey{}, err
	}
	return ck, nil
}

func (s *PostgresStore) ListCryptoKeys(ctx context.Context, projectID, location, keyringID string) ([]CryptoKey, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT location, keyring_id, key_id, purpose, create_time, primary_version, algorithm
		FROM jc_kms_cryptokeys WHERE project_id=$1 AND location=$2 AND keyring_id=$3 ORDER BY key_id
	`, projectID, location, keyringID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []CryptoKey
	for rows.Next() {
		var ck CryptoKey
		if err := rows.Scan(&ck.Location, &ck.KeyRingID, &ck.ID, &ck.Purpose, &ck.CreateTime, &ck.PrimaryVersion, &ck.Algorithm); err != nil {
			return nil, err
		}
		result = append(result, ck)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, rows.Err()
}

func (s *PostgresStore) CreateVersion(ctx context.Context, projectID, location, keyringID, keyID string, v Version) (string, error) {
	dek, err := s.dek(ctx)
	if err != nil {
		return "", err
	}
	wrapped, err := wrapVersionMaterial(dek, keyID, v.Algorithm)
	if err != nil {
		return "", err
	}
	if v.State == "" {
		v.State = "ENABLED"
	}
	if v.CreateTime.IsZero() {
		v.CreateTime = clock.Now()
	}

	var next int
	err = s.pool.QueryRow(ctx, `
		UPDATE jc_kms_cryptokeys SET next_version = next_version + 1
		WHERE project_id=$1 AND location=$2 AND keyring_id=$3 AND key_id=$4
		RETURNING next_version - 1
	`, projectID, location, keyringID, keyID).Scan(&next)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNoSuchCryptoKey
	}
	if err != nil {
		return "", err
	}
	version := strconv.Itoa(next)
	_, err = s.pool.Exec(ctx, `
		INSERT INTO jc_kms_cryptokey_versions (project_id, location, keyring_id, key_id, version, state, algorithm, create_time, key_material, private_key, public_key)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
	`, projectID, location, keyringID, keyID, version, v.State, wrapped.Algorithm, v.CreateTime, wrapped.KeyMaterial, wrapped.PrivateKey, wrapped.PublicKey)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return "", ErrAlreadyExists
		}
		return "", err
	}
	return version, nil
}

func (s *PostgresStore) GetVersion(ctx context.Context, projectID, location, keyringID, keyID, version string) (Version, error) {
	var v Version
	err := s.pool.QueryRow(ctx, `
		SELECT key_id, version, state, algorithm, create_time, key_material, private_key, public_key
		FROM jc_kms_cryptokey_versions
		WHERE project_id=$1 AND location=$2 AND keyring_id=$3 AND key_id=$4 AND version=$5
	`, projectID, location, keyringID, keyID, version).Scan(&v.KeyID, &v.Version, &v.State, &v.Algorithm, &v.CreateTime, &v.KeyMaterial, &v.PrivateKey, &v.PublicKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return Version{}, ErrNoSuchVersion
	}
	if err != nil {
		return Version{}, err
	}
	return v, nil
}

func (s *PostgresStore) ListVersions(ctx context.Context, projectID, location, keyringID, keyID string) ([]Version, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT key_id, version, state, algorithm, create_time, key_material, private_key, public_key
		FROM jc_kms_cryptokey_versions
		WHERE project_id=$1 AND location=$2 AND keyring_id=$3 AND key_id=$4
	`, projectID, location, keyringID, keyID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Version
	for rows.Next() {
		var v Version
		if err := rows.Scan(&v.KeyID, &v.Version, &v.State, &v.Algorithm, &v.CreateTime, &v.KeyMaterial, &v.PrivateKey, &v.PublicKey); err != nil {
			return nil, err
		}
		result = append(result, v)
	}
	sort.Slice(result, func(i, j int) bool { return atoi(result[i].Version) < atoi(result[j].Version) })
	return result, rows.Err()
}

func (s *PostgresStore) UpdateVersionState(ctx context.Context, projectID, location, keyringID, keyID, version, state string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_kms_cryptokey_versions SET state=$6
		WHERE project_id=$1 AND location=$2 AND keyring_id=$3 AND key_id=$4 AND version=$5
	`, projectID, location, keyringID, keyID, version, state)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchVersion
	}
	return nil
}

func (s *PostgresStore) UpdatePrimaryVersion(ctx context.Context, projectID, location, keyringID, keyID, version string) error {
	var exists bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM jc_kms_cryptokey_versions
			WHERE project_id=$1 AND location=$2 AND keyring_id=$3 AND key_id=$4 AND version=$5)
	`, projectID, location, keyringID, keyID, version).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return ErrNoSuchVersion
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_kms_cryptokeys SET primary_version=$5
		WHERE project_id=$1 AND location=$2 AND keyring_id=$3 AND key_id=$4
	`, projectID, location, keyringID, keyID, version)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchCryptoKey
	}
	return nil
}

func (s *PostgresStore) KeyMaterial(ctx context.Context, projectID, location, keyringID, keyID, version string) ([]byte, error) {
	dek, err := s.dek(ctx)
	if err != nil {
		return nil, err
	}
	var wrapped []byte
	err = s.pool.QueryRow(ctx, `
		SELECT key_material FROM jc_kms_cryptokey_versions
		WHERE project_id=$1 AND location=$2 AND keyring_id=$3 AND key_id=$4 AND version=$5
	`, projectID, location, keyringID, keyID, version).Scan(&wrapped)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNoSuchVersion
	}
	if err != nil {
		return nil, err
	}
	if len(wrapped) == 0 {
		return nil, ErrNoSuchVersion
	}
	return DecryptData(dek, wrapped, []byte(keyID))
}

func (s *PostgresStore) PrivateKey(ctx context.Context, projectID, location, keyringID, keyID, version string) ([]byte, error) {
	dek, err := s.dek(ctx)
	if err != nil {
		return nil, err
	}
	var wrapped []byte
	err = s.pool.QueryRow(ctx, `
		SELECT private_key FROM jc_kms_cryptokey_versions
		WHERE project_id=$1 AND location=$2 AND keyring_id=$3 AND key_id=$4 AND version=$5
	`, projectID, location, keyringID, keyID, version).Scan(&wrapped)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNoSuchVersion
	}
	if err != nil {
		return nil, err
	}
	if len(wrapped) == 0 {
		return nil, ErrNoSuchVersion
	}
	return DecryptData(dek, wrapped, []byte(keyID))
}

func (s *PostgresStore) PublicKey(ctx context.Context, projectID, location, keyringID, keyID, version string) ([]byte, error) {
	dek, err := s.dek(ctx)
	if err != nil {
		return nil, err
	}
	var wrapped []byte
	err = s.pool.QueryRow(ctx, `
		SELECT public_key FROM jc_kms_cryptokey_versions
		WHERE project_id=$1 AND location=$2 AND keyring_id=$3 AND key_id=$4 AND version=$5
	`, projectID, location, keyringID, keyID, version).Scan(&wrapped)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNoSuchVersion
	}
	if err != nil {
		return nil, err
	}
	if len(wrapped) == 0 {
		return nil, ErrNoSuchVersion
	}
	return DecryptData(dek, wrapped, []byte(keyID))
}

func (s *PostgresStore) Reset(ctx context.Context) {
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_kms_cryptokey_versions`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_kms_cryptokeys`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_kms_keyrings`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_kms_dek`)
}
