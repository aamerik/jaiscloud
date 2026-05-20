package s3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresS3ObjectMetaStore implements S3ObjectMetaStore against PostgreSQL.
type PostgresS3ObjectMetaStore struct {
	pool *pgxpool.Pool
}

func NewPostgresS3ObjectMetaStore(pool *pgxpool.Pool) *PostgresS3ObjectMetaStore {
	return &PostgresS3ObjectMetaStore{pool: pool}
}

func (s *PostgresS3ObjectMetaStore) CreateBucket(ctx context.Context, bucket string, meta map[string]any) error {
	if meta == nil {
		meta = map[string]any{}
	}
	meta["Name"] = bucket
	meta["CreationDate"] = time.Now().UTC().Format(time.RFC3339)
	ownerAccountID, _ := meta["AccountID"].(string)
	bucketRegion, _ := meta["Region"].(string)
	raw, _ := json.Marshal(meta)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_s3_buckets (name, owner_account_id, region, meta) VALUES ($1, $2, $3, $4)
	`, bucket, ownerAccountID, bucketRegion, json.RawMessage(raw))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return fmt.Errorf("bucket already exists")
		}
		return fmt.Errorf("CreateBucket: %w", err)
	}
	return nil
}

func (s *PostgresS3ObjectMetaStore) GetBucket(ctx context.Context, bucket string) (map[string]any, error) {
	var raw []byte
	err := s.pool.QueryRow(ctx, `SELECT meta FROM jc_s3_buckets WHERE name=$1`, bucket).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("NoSuchBucket")
	}
	if err != nil {
		return nil, err
	}
	var meta map[string]any
	return meta, json.Unmarshal(raw, &meta)
}

func (s *PostgresS3ObjectMetaStore) UpdateBucketMeta(ctx context.Context, bucket string, meta map[string]any) error {
	raw, _ := json.Marshal(meta)
	tag, err := s.pool.Exec(ctx, `UPDATE jc_s3_buckets SET meta=$2 WHERE name=$1`, bucket, json.RawMessage(raw))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("NoSuchBucket")
	}
	return nil
}

func (s *PostgresS3ObjectMetaStore) DeleteBucket(ctx context.Context, bucket string) error {
	// Check not empty.
	var count int
	s.pool.QueryRow(ctx, `SELECT count(*) FROM jc_s3_objects WHERE bucket=$1`, bucket).Scan(&count)
	if count > 0 {
		return fmt.Errorf("BucketNotEmpty")
	}
	tag, err := s.pool.Exec(ctx, `DELETE FROM jc_s3_buckets WHERE name=$1`, bucket)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("NoSuchBucket")
	}
	return nil
}

func (s *PostgresS3ObjectMetaStore) ListBuckets(ctx context.Context, accountID string) ([]map[string]any, error) {
	var rows pgx.Rows
	var err error
	if accountID != "" {
		rows, err = s.pool.Query(ctx, `SELECT meta FROM jc_s3_buckets WHERE meta->>'AccountID'=$1 ORDER BY created_at`, accountID)
	} else {
		rows, err = s.pool.Query(ctx, `SELECT meta FROM jc_s3_buckets ORDER BY created_at`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []map[string]any
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var meta map[string]any
		if json.Unmarshal(raw, &meta) == nil {
			result = append(result, meta)
		}
	}
	return result, rows.Err()
}

func (s *PostgresS3ObjectMetaStore) PutObjectMeta(ctx context.Context, bucket, key string, meta ObjectMeta) error {
	metaRaw, _ := json.Marshal(meta.Metadata)
	tagsRaw, _ := json.Marshal(meta.Tags)
	if meta.ContentType == "" {
		meta.ContentType = "application/octet-stream"
	}
	if meta.StorageClass == "" {
		meta.StorageClass = "STANDARD"
	}
	if meta.LastModified.IsZero() {
		meta.LastModified = time.Now().UTC()
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_s3_objects (bucket, key, etag, crc32, size, content_type, last_modified, metadata, storage_class, version_id, tags, encryption, kms_key_id, ssec_key_md5, lock_mode, lock_retain_until, legal_hold_status, acl, checksum_algorithm, checksum_value)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
		ON CONFLICT (bucket, key) DO UPDATE
			SET etag=$3, crc32=$4, size=$5, content_type=$6, last_modified=$7, metadata=$8, storage_class=$9, version_id=$10,
			    tags=$11, encryption=$12, kms_key_id=$13, ssec_key_md5=$14, lock_mode=$15, lock_retain_until=$16, legal_hold_status=$17, acl=$18,
			    checksum_algorithm=$19, checksum_value=$20
	`, bucket, key, meta.ETag, meta.CRC32, meta.Size, meta.ContentType, meta.LastModified,
		json.RawMessage(metaRaw), meta.StorageClass, meta.VersionID,
		json.RawMessage(tagsRaw), meta.Encryption, meta.KMSKeyID, meta.SSECKeyMD5,
		meta.LockMode, meta.LockRetainUntil, meta.LegalHoldStatus, meta.ACL,
		meta.ChecksumAlgorithm, meta.ChecksumValue)
	return err
}

func (s *PostgresS3ObjectMetaStore) GetObjectMeta(ctx context.Context, bucket, key string) (ObjectMeta, error) {
	var m ObjectMeta
	var metaRaw, tagsRaw []byte
	err := s.pool.QueryRow(ctx, `
		SELECT key, etag, crc32, size, content_type, last_modified, metadata, storage_class, version_id,
		       tags, encryption, kms_key_id, ssec_key_md5, lock_mode, lock_retain_until, legal_hold_status, acl,
		       checksum_algorithm, checksum_value
		FROM jc_s3_objects WHERE bucket=$1 AND key=$2
	`, bucket, key).Scan(
		&m.Key, &m.ETag, &m.CRC32, &m.Size, &m.ContentType, &m.LastModified, &metaRaw, &m.StorageClass, &m.VersionID,
		&tagsRaw, &m.Encryption, &m.KMSKeyID, &m.SSECKeyMD5, &m.LockMode, &m.LockRetainUntil, &m.LegalHoldStatus, &m.ACL,
		&m.ChecksumAlgorithm, &m.ChecksumValue)
	if errors.Is(err, pgx.ErrNoRows) {
		return ObjectMeta{}, fmt.Errorf("NoSuchKey")
	}
	if err != nil {
		return ObjectMeta{}, err
	}
	json.Unmarshal(metaRaw, &m.Metadata)
	json.Unmarshal(tagsRaw, &m.Tags)
	return m, nil
}

func (s *PostgresS3ObjectMetaStore) DeleteObjectMeta(ctx context.Context, bucket, key string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM jc_s3_objects WHERE bucket=$1 AND key=$2`, bucket, key)
	return err
}

// prefixRange returns [low, high) string bounds for a prefix scan.
// All keys with the given prefix satisfy: key >= low AND key < high.
// Returns high="" when the prefix ends in all 0xFF bytes (no finite upper bound).
func prefixRange(prefix string) (low, high string) {
	if prefix == "" {
		return "", ""
	}
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xFF {
			b[i]++
			return prefix, string(b[:i+1])
		}
		b[i] = 0
	}
	return prefix, "" // all bytes were 0xFF
}

func (s *PostgresS3ObjectMetaStore) ListObjectMeta(ctx context.Context, bucket, prefix, delimiter, marker string, maxKeys int) ([]ObjectMeta, []string, bool, string, error) {
	if maxKeys <= 0 {
		maxKeys = 1000
	}

	low, high := prefixRange(prefix)

	// When a delimiter is in use, multiple raw keys can collapse into a single
	// common prefix, so SQL LIMIT would cut the candidate set too early.
	// Without a delimiter each row maps 1:1 to a result, so LIMIT maxKeys+1 is safe.
	const sel = `SELECT key, etag, size, content_type, last_modified, metadata, storage_class FROM jc_s3_objects WHERE bucket=$1`
	var rows pgx.Rows
	var err error
	if delimiter == "" {
		switch {
		case prefix == "" && marker == "":
			rows, err = s.pool.Query(ctx, sel+` ORDER BY key LIMIT $2`, bucket, maxKeys+1)
		case prefix == "" && marker != "":
			rows, err = s.pool.Query(ctx, sel+` AND key > $2 ORDER BY key LIMIT $3`, bucket, marker, maxKeys+1)
		case prefix != "" && marker == "" && high != "":
			rows, err = s.pool.Query(ctx, sel+` AND key >= $2 AND key < $3 ORDER BY key LIMIT $4`, bucket, low, high, maxKeys+1)
		case prefix != "" && marker == "" && high == "":
			rows, err = s.pool.Query(ctx, sel+` AND key >= $2 ORDER BY key LIMIT $3`, bucket, low, maxKeys+1)
		case prefix != "" && marker != "" && high != "":
			rows, err = s.pool.Query(ctx, sel+` AND key > $2 AND key >= $3 AND key < $4 ORDER BY key LIMIT $5`, bucket, marker, low, high, maxKeys+1)
		default:
			rows, err = s.pool.Query(ctx, sel+` AND key > $2 AND key >= $3 ORDER BY key LIMIT $4`, bucket, marker, low, maxKeys+1)
		}
	} else {
		// No SQL LIMIT when delimiter is set; the prefix-range filter (key >= low AND
		// key < high) bounds the candidate set so we can read all and paginate in Go.
		switch {
		case prefix == "" && marker == "":
			rows, err = s.pool.Query(ctx, sel+` ORDER BY key`, bucket)
		case prefix == "" && marker != "":
			rows, err = s.pool.Query(ctx, sel+` AND key > $2 ORDER BY key`, bucket, marker)
		case prefix != "" && marker == "" && high != "":
			rows, err = s.pool.Query(ctx, sel+` AND key >= $2 AND key < $3 ORDER BY key`, bucket, low, high)
		case prefix != "" && marker == "" && high == "":
			rows, err = s.pool.Query(ctx, sel+` AND key >= $2 ORDER BY key`, bucket, low)
		case prefix != "" && marker != "" && high != "":
			rows, err = s.pool.Query(ctx, sel+` AND key > $2 AND key >= $3 AND key < $4 ORDER BY key`, bucket, marker, low, high)
		default:
			rows, err = s.pool.Query(ctx, sel+` AND key > $2 AND key >= $3 ORDER BY key`, bucket, marker, low)
		}
	}
	if err != nil {
		return nil, nil, false, "", err
	}
	defer rows.Close()

	var all []ObjectMeta
	for rows.Next() {
		var m ObjectMeta
		var metaRaw []byte
		if err := rows.Scan(&m.Key, &m.ETag, &m.Size, &m.ContentType, &m.LastModified, &metaRaw, &m.StorageClass); err != nil {
			return nil, nil, false, "", err
		}
		json.Unmarshal(metaRaw, &m.Metadata)
		all = append(all, m)
	}

	commonPrefixes := map[string]bool{}
	var result []ObjectMeta
	truncated := false
	var lastExaminedKey string
	breakIdx := len(all)
	for i, m := range all {
		if len(result)+len(commonPrefixes) >= maxKeys {
			truncated = true
			breakIdx = i
			break
		}
		lastExaminedKey = m.Key
		if delimiter != "" {
			rest := m.Key[len(prefix):]
			if idx := strings.Index(rest, delimiter); idx >= 0 {
				commonPrefixes[prefix+rest[:idx+len(delimiter)]] = true
				continue
			}
		}
		result = append(result, m)
	}

	var cpList []string
	for cp := range commonPrefixes {
		cpList = append(cpList, cp)
	}
	sort.Strings(cpList)

	nextMarker := ""
	if truncated {
		// Advance nextMarker past all keys that share the last common prefix on this
		// page. Without this, the next-page marker would re-emit the final CP.
		if delimiter != "" && len(cpList) > 0 {
			lastCP := cpList[len(cpList)-1]
			endIdx := breakIdx
			for i, m := range all[breakIdx:] {
				if strings.HasPrefix(m.Key, lastCP) {
					lastExaminedKey = m.Key
					endIdx = breakIdx + i + 1
				} else {
					break
				}
			}
			// If the advance consumed every remaining candidate, there are no more
			// objects beyond this page — correct the truncated flag.
			if endIdx >= len(all) {
				truncated = false
			}
		}
		if truncated {
			nextMarker = lastExaminedKey
		}
	}
	return result, cpList, truncated, nextMarker, rows.Err()
}

func (s *PostgresS3ObjectMetaStore) InitMultipart(ctx context.Context, bucket, key, uploadID string, meta map[string]any) error {
	raw, _ := json.Marshal(meta)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_s3_multipart_uploads (upload_id, bucket, key, meta) VALUES ($1,$2,$3,$4)
	`, uploadID, bucket, key, json.RawMessage(raw))
	return err
}

func (s *PostgresS3ObjectMetaStore) PutPart(ctx context.Context, uploadID string, partNumber int, part PartMeta) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_s3_multipart_parts (upload_id, part_number, etag, size) VALUES ($1,$2,$3,$4)
		ON CONFLICT (upload_id, part_number) DO UPDATE SET etag=$3, size=$4
	`, uploadID, partNumber, part.ETag, part.Size)
	return err
}

func (s *PostgresS3ObjectMetaStore) CompleteMultipart(ctx context.Context, bucket, key, uploadID string) ([]PartMeta, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT part_number, etag, size FROM jc_s3_multipart_parts
		WHERE upload_id=$1 ORDER BY part_number
	`, uploadID)
	if err != nil {
		return nil, err
	}
	var parts []PartMeta
	for rows.Next() {
		var p PartMeta
		if err := rows.Scan(&p.PartNumber, &p.ETag, &p.Size); err != nil {
			rows.Close()
			return nil, err
		}
		parts = append(parts, p)
	}
	rows.Close() // explicit close before Err() — required for correct pgx/v5 error state
	if err := rows.Err(); err != nil {
		return nil, err // upload row preserved; caller can retry CompleteMultipart
	}
	s.pool.Exec(ctx, `DELETE FROM jc_s3_multipart_uploads WHERE upload_id=$1`, uploadID)
	return parts, nil
}

func (s *PostgresS3ObjectMetaStore) AbortMultipart(ctx context.Context, uploadID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM jc_s3_multipart_uploads WHERE upload_id=$1`, uploadID)
	return err
}

func (s *PostgresS3ObjectMetaStore) GetMultipartMeta(ctx context.Context, uploadID string) (string, string, map[string]any, error) {
	var bucket, key string
	var raw []byte
	err := s.pool.QueryRow(ctx, `
		SELECT bucket, key, meta FROM jc_s3_multipart_uploads WHERE upload_id=$1
	`, uploadID).Scan(&bucket, &key, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", nil, fmt.Errorf("NoSuchUpload")
	}
	if err != nil {
		return "", "", nil, err
	}
	var meta map[string]any
	json.Unmarshal(raw, &meta)
	return bucket, key, meta, nil
}

func (s *PostgresS3ObjectMetaStore) ListParts(ctx context.Context, uploadID string) ([]PartMeta, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM jc_s3_multipart_uploads WHERE upload_id=$1)`, uploadID).Scan(&exists)
	if err != nil || !exists {
		return nil, fmt.Errorf("NoSuchUpload")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT part_number, etag, size FROM jc_s3_multipart_parts
		WHERE upload_id=$1 ORDER BY part_number
	`, uploadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var parts []PartMeta
	for rows.Next() {
		var p PartMeta
		if err := rows.Scan(&p.PartNumber, &p.ETag, &p.Size); err != nil {
			return nil, err
		}
		parts = append(parts, p)
	}
	return parts, rows.Err()
}

func (s *PostgresS3ObjectMetaStore) ListActiveUploads(ctx context.Context, bucket string) ([]ActiveUpload, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT upload_id, key, created_at FROM jc_s3_multipart_uploads
		WHERE bucket=$1 ORDER BY key, upload_id
	`, bucket)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []ActiveUpload
	for rows.Next() {
		var u ActiveUpload
		u.Bucket = bucket
		if err := rows.Scan(&u.UploadID, &u.Key, &u.Initiated); err != nil {
			return nil, err
		}
		result = append(result, u)
	}
	return result, rows.Err()
}

func (s *PostgresS3ObjectMetaStore) GetBucketVersioning(ctx context.Context, bucket string) (string, error) {
	meta, err := s.GetBucket(ctx, bucket)
	if err != nil {
		return "", err
	}
	status, _ := meta["versioning_status"].(string)
	return status, nil
}

func (s *PostgresS3ObjectMetaStore) SetBucketVersioning(ctx context.Context, bucket, status string) error {
	meta, err := s.GetBucket(ctx, bucket)
	if err != nil {
		return fmt.Errorf("NoSuchBucket")
	}
	meta["versioning_status"] = status
	return s.UpdateBucketMeta(ctx, bucket, meta)
}

func (s *PostgresS3ObjectMetaStore) PutObjectVersion(ctx context.Context, bucket, key string, meta ObjectMeta) (string, error) {
	metaRaw, _ := json.Marshal(meta.Metadata)
	tagsRaw, _ := json.Marshal(meta.Tags)
	if meta.ContentType == "" {
		meta.ContentType = "application/octet-stream"
	}
	if meta.StorageClass == "" {
		meta.StorageClass = "STANDARD"
	}
	if meta.LastModified.IsZero() {
		meta.LastModified = time.Now().UTC()
	}
	if meta.VersionID == "" {
		meta.VersionID = fmt.Sprintf("%016x%016x", time.Now().UnixNano(), time.Now().UnixNano()+1)
	}
	// Mark all existing as not-latest.
	s.pool.Exec(ctx, `UPDATE jc_s3_object_versions SET is_latest=FALSE WHERE bucket=$1 AND key=$2`, bucket, key)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_s3_object_versions
		  (bucket, key, version_id, is_delete_marker, is_latest, etag, crc32, size, content_type, last_modified, metadata, storage_class, encryption, kms_key_id, ssec_key_md5, lock_mode, lock_retain_until, legal_hold_status, acl, tags, checksum_algorithm, checksum_value)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)
		ON CONFLICT (bucket, key, version_id) DO UPDATE
		  SET is_latest=$5, etag=$6, crc32=$7, size=$8, content_type=$9, last_modified=$10, metadata=$11,
		      checksum_algorithm=$21, checksum_value=$22
	`, bucket, key, meta.VersionID, meta.IsDeleteMarker, true,
		meta.ETag, meta.CRC32, meta.Size, meta.ContentType, meta.LastModified,
		json.RawMessage(metaRaw), meta.StorageClass, meta.Encryption, meta.KMSKeyID, meta.SSECKeyMD5,
		meta.LockMode, meta.LockRetainUntil, meta.LegalHoldStatus, meta.ACL, json.RawMessage(tagsRaw),
		meta.ChecksumAlgorithm, meta.ChecksumValue)
	if err != nil {
		return "", err
	}
	return meta.VersionID, nil
}

func (s *PostgresS3ObjectMetaStore) UpdateObjectVersion(ctx context.Context, bucket, key string, meta ObjectMeta) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_s3_object_versions
		SET lock_mode=$1, lock_retain_until=$2, legal_hold_status=$3
		WHERE bucket=$4 AND key=$5 AND version_id=$6
	`, meta.LockMode, meta.LockRetainUntil, meta.LegalHoldStatus, bucket, key, meta.VersionID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("NoSuchVersion")
	}
	return nil
}

func (s *PostgresS3ObjectMetaStore) GetObjectVersion(ctx context.Context, bucket, key, versionID string) (ObjectMeta, error) {
	var m ObjectMeta
	var metaRaw, tagsRaw []byte
	err := s.pool.QueryRow(ctx, `
		SELECT key, etag, crc32, size, content_type, last_modified, metadata, storage_class, version_id,
		       is_delete_marker, is_latest, lock_mode, lock_retain_until, legal_hold_status, acl, encryption, kms_key_id, ssec_key_md5, tags,
		       checksum_algorithm, checksum_value
		FROM jc_s3_object_versions WHERE bucket=$1 AND key=$2 AND version_id=$3
	`, bucket, key, versionID).Scan(
		&m.Key, &m.ETag, &m.CRC32, &m.Size, &m.ContentType, &m.LastModified, &metaRaw, &m.StorageClass, &m.VersionID,
		&m.IsDeleteMarker, &m.IsLatest, &m.LockMode, &m.LockRetainUntil, &m.LegalHoldStatus, &m.ACL, &m.Encryption, &m.KMSKeyID, &m.SSECKeyMD5, &tagsRaw,
		&m.ChecksumAlgorithm, &m.ChecksumValue)
	if errors.Is(err, pgx.ErrNoRows) {
		return ObjectMeta{}, fmt.Errorf("NoSuchVersion")
	}
	if err != nil {
		return ObjectMeta{}, err
	}
	json.Unmarshal(metaRaw, &m.Metadata)
	json.Unmarshal(tagsRaw, &m.Tags)
	return m, nil
}

func (s *PostgresS3ObjectMetaStore) DeleteObjectVersion(ctx context.Context, bucket, key, versionID string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM jc_s3_object_versions WHERE bucket=$1 AND key=$2 AND version_id=$3`, bucket, key, versionID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("NoSuchVersion")
	}
	// Promote latest if needed.
	s.pool.Exec(ctx, `
		UPDATE jc_s3_object_versions SET is_latest=TRUE
		WHERE bucket=$1 AND key=$2
		  AND last_modified = (SELECT MAX(last_modified) FROM jc_s3_object_versions WHERE bucket=$1 AND key=$2)
	`, bucket, key)
	return nil
}

func (s *PostgresS3ObjectMetaStore) ListObjectVersions(ctx context.Context, bucket, prefix, keyMarker, _ string, maxKeys int) ([]ObjectMeta, bool, error) {
	if maxKeys <= 0 {
		maxKeys = 1000
	}
	rows, err := s.pool.Query(ctx, `
		SELECT key, etag, crc32, size, content_type, last_modified, metadata, storage_class, version_id,
		       is_delete_marker, is_latest, lock_mode, lock_retain_until, legal_hold_status, acl, encryption, kms_key_id, ssec_key_md5, tags
		FROM jc_s3_object_versions
		WHERE bucket=$1 AND key LIKE $2 AND key > $3
		ORDER BY key, last_modified DESC
		LIMIT $4
	`, bucket, prefix+"%", keyMarker, maxKeys+1)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	var result []ObjectMeta
	for rows.Next() {
		var m ObjectMeta
		var metaRaw, tagsRaw []byte
		if err := rows.Scan(
			&m.Key, &m.ETag, &m.CRC32, &m.Size, &m.ContentType, &m.LastModified, &metaRaw, &m.StorageClass, &m.VersionID,
			&m.IsDeleteMarker, &m.IsLatest, &m.LockMode, &m.LockRetainUntil, &m.LegalHoldStatus, &m.ACL, &m.Encryption, &m.KMSKeyID, &m.SSECKeyMD5, &tagsRaw); err != nil {
			return nil, false, err
		}
		json.Unmarshal(metaRaw, &m.Metadata)
		json.Unmarshal(tagsRaw, &m.Tags)
		result = append(result, m)
	}
	if len(result) > maxKeys {
		return result[:maxKeys], true, rows.Err()
	}
	return result, false, rows.Err()
}

func (s *PostgresS3ObjectMetaStore) Reset(ctx context.Context) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		slog.Warn("s3 reset: begin transaction failed", "err", err)
		return
	}
	defer tx.Rollback(ctx) // no-op after Commit; safe to defer
	for _, stmt := range []string{
		`DELETE FROM jc_s3_multipart_parts`,
		`DELETE FROM jc_s3_multipart_uploads`,
		`DELETE FROM jc_s3_object_versions`,
		`DELETE FROM jc_s3_objects`,
		`DELETE FROM jc_s3_buckets`,
	} {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			slog.Warn("s3 reset: exec failed", "stmt", stmt, "err", err)
			return
		}
	}
	if err := tx.Commit(ctx); err != nil {
		slog.Warn("s3 reset: commit failed", "err", err)
	}
}

func (s *PostgresS3ObjectMetaStore) ResetScope(account, region string) {
	ctx := context.Background()
	// jc_s3_objects/versions/multipart have no account_id; filter via bucket ownership.
	inBuckets := `(SELECT name FROM jc_s3_buckets WHERE owner_account_id=$1 AND region=$2)`
	s.pool.Exec(ctx, `DELETE FROM jc_s3_multipart_parts WHERE upload_id IN
		(SELECT upload_id FROM jc_s3_multipart_uploads WHERE bucket IN `+inBuckets+`)`, account, region)
	s.pool.Exec(ctx, `DELETE FROM jc_s3_multipart_uploads WHERE bucket IN `+inBuckets, account, region)
	s.pool.Exec(ctx, `DELETE FROM jc_s3_object_versions  WHERE bucket IN `+inBuckets, account, region)
	s.pool.Exec(ctx, `DELETE FROM jc_s3_objects           WHERE bucket IN `+inBuckets, account, region)
	s.pool.Exec(ctx, `DELETE FROM jc_s3_buckets WHERE owner_account_id=$1 AND region=$2`, account, region)
}

func (s *PostgresS3ObjectMetaStore) ResetAccount(account string) {
	ctx := context.Background()
	inBuckets := `(SELECT name FROM jc_s3_buckets WHERE owner_account_id=$1)`
	s.pool.Exec(ctx, `DELETE FROM jc_s3_multipart_parts WHERE upload_id IN
		(SELECT upload_id FROM jc_s3_multipart_uploads WHERE bucket IN `+inBuckets+`)`, account)
	s.pool.Exec(ctx, `DELETE FROM jc_s3_multipart_uploads WHERE bucket IN `+inBuckets, account)
	s.pool.Exec(ctx, `DELETE FROM jc_s3_object_versions  WHERE bucket IN `+inBuckets, account)
	s.pool.Exec(ctx, `DELETE FROM jc_s3_objects           WHERE bucket IN `+inBuckets, account)
	s.pool.Exec(ctx, `DELETE FROM jc_s3_buckets WHERE owner_account_id=$1`, account)
}

// ─── Snapshotter ─────────────────────────────────────────────────────────────

type pgS3BucketRow struct {
	Name           string          `json:"name"`
	OwnerAccountID string          `json:"owner_account_id"`
	Region         string          `json:"region"`
	Meta           json.RawMessage `json:"meta"`
}

type pgS3ObjectRow struct {
	Bucket            string          `json:"bucket"`
	Key               string          `json:"key"`
	ETag              string          `json:"etag"`
	CRC32             string          `json:"crc32"`
	Size              int64           `json:"size"`
	ContentType       string          `json:"content_type"`
	LastModified      time.Time       `json:"last_modified"`
	Metadata          json.RawMessage `json:"metadata"`
	StorageClass      string          `json:"storage_class"`
	VersionID         string          `json:"version_id"`
	Tags              json.RawMessage `json:"tags"`
	Encryption        string          `json:"encryption"`
	KMSKeyID          string          `json:"kms_key_id"`
	SSECKeyMD5        string          `json:"ssec_key_md5"`
	LockMode          string          `json:"lock_mode"`
	LockRetainUntil   *time.Time      `json:"lock_retain_until"`
	LegalHoldStatus   string          `json:"legal_hold_status"`
	ACL               string          `json:"acl"`
	ChecksumAlgorithm string          `json:"checksum_algorithm"`
	ChecksumValue     string          `json:"checksum_value"`
}

type pgS3VersionRow struct {
	Bucket            string          `json:"bucket"`
	Key               string          `json:"key"`
	VersionID         string          `json:"version_id"`
	IsDeleteMarker    bool            `json:"is_delete_marker"`
	IsLatest          bool            `json:"is_latest"`
	ETag              string          `json:"etag"`
	CRC32             string          `json:"crc32"`
	Size              int64           `json:"size"`
	ContentType       string          `json:"content_type"`
	LastModified      time.Time       `json:"last_modified"`
	Metadata          json.RawMessage `json:"metadata"`
	StorageClass      string          `json:"storage_class"`
	Encryption        string          `json:"encryption"`
	KMSKeyID          string          `json:"kms_key_id"`
	SSECKeyMD5        string          `json:"ssec_key_md5"`
	LockMode          string          `json:"lock_mode"`
	LockRetainUntil   *time.Time      `json:"lock_retain_until"`
	LegalHoldStatus   string          `json:"legal_hold_status"`
	ACL               string          `json:"acl"`
	Tags              json.RawMessage `json:"tags"`
	ChecksumAlgorithm string          `json:"checksum_algorithm"`
	ChecksumValue     string          `json:"checksum_value"`
}

type pgS3UploadRow struct {
	UploadID string          `json:"upload_id"`
	Bucket   string          `json:"bucket"`
	Key      string          `json:"key"`
	Meta     json.RawMessage `json:"meta"`
}

type pgS3PartRow struct {
	UploadID   string `json:"upload_id"`
	PartNumber int    `json:"part_number"`
	ETag       string `json:"etag"`
	Size       int64  `json:"size"`
}

type pgS3Snap struct {
	Buckets  []pgS3BucketRow  `json:"buckets"`
	Objects  []pgS3ObjectRow  `json:"objects"`
	Versions []pgS3VersionRow `json:"versions"`
	Uploads  []pgS3UploadRow  `json:"uploads"`
	Parts    []pgS3PartRow    `json:"parts"`
}

func (s *PostgresS3ObjectMetaStore) IsEmpty(ctx context.Context) (bool, error) {
	var count int
	err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM jc_s3_buckets`).Scan(&count)
	if err != nil {
		return false, err
	}
	return count == 0, nil
}

func (s *PostgresS3ObjectMetaStore) Snapshot(ctx context.Context, w io.Writer) error {
	var snap pgS3Snap

	// Buckets
	brows, err := s.pool.Query(ctx, `SELECT name, owner_account_id, region, meta FROM jc_s3_buckets ORDER BY name`)
	if err != nil {
		return fmt.Errorf("snapshot s3 buckets: %w", err)
	}
	defer brows.Close()
	for brows.Next() {
		var r pgS3BucketRow
		var raw []byte
		if err := brows.Scan(&r.Name, &r.OwnerAccountID, &r.Region, &raw); err != nil {
			return fmt.Errorf("snapshot s3 bucket scan: %w", err)
		}
		r.Meta = json.RawMessage(raw)
		snap.Buckets = append(snap.Buckets, r)
	}
	if err := brows.Err(); err != nil {
		return err
	}

	// Objects
	orows, err := s.pool.Query(ctx, `
		SELECT bucket, key, etag, crc32, size, content_type, last_modified, metadata,
		       storage_class, version_id, tags, encryption, kms_key_id, ssec_key_md5,
		       lock_mode, lock_retain_until, legal_hold_status, acl, checksum_algorithm, checksum_value
		FROM jc_s3_objects ORDER BY bucket, key
	`)
	if err != nil {
		return fmt.Errorf("snapshot s3 objects: %w", err)
	}
	defer orows.Close()
	for orows.Next() {
		var r pgS3ObjectRow
		var metaRaw, tagsRaw []byte
		if err := orows.Scan(
			&r.Bucket, &r.Key, &r.ETag, &r.CRC32, &r.Size, &r.ContentType, &r.LastModified,
			&metaRaw, &r.StorageClass, &r.VersionID, &tagsRaw, &r.Encryption, &r.KMSKeyID,
			&r.SSECKeyMD5, &r.LockMode, &r.LockRetainUntil, &r.LegalHoldStatus, &r.ACL,
			&r.ChecksumAlgorithm, &r.ChecksumValue,
		); err != nil {
			return fmt.Errorf("snapshot s3 object scan: %w", err)
		}
		r.Metadata = json.RawMessage(metaRaw)
		r.Tags = json.RawMessage(tagsRaw)
		snap.Objects = append(snap.Objects, r)
	}
	if err := orows.Err(); err != nil {
		return err
	}

	// Versions
	vrows, err := s.pool.Query(ctx, `
		SELECT bucket, key, version_id, is_delete_marker, is_latest, etag, crc32, size,
		       content_type, last_modified, metadata, storage_class, encryption, kms_key_id,
		       ssec_key_md5, lock_mode, lock_retain_until, legal_hold_status, acl, tags,
		       checksum_algorithm, checksum_value
		FROM jc_s3_object_versions ORDER BY bucket, key, last_modified DESC
	`)
	if err != nil {
		return fmt.Errorf("snapshot s3 versions: %w", err)
	}
	defer vrows.Close()
	for vrows.Next() {
		var r pgS3VersionRow
		var metaRaw, tagsRaw []byte
		if err := vrows.Scan(
			&r.Bucket, &r.Key, &r.VersionID, &r.IsDeleteMarker, &r.IsLatest,
			&r.ETag, &r.CRC32, &r.Size, &r.ContentType, &r.LastModified,
			&metaRaw, &r.StorageClass, &r.Encryption, &r.KMSKeyID, &r.SSECKeyMD5,
			&r.LockMode, &r.LockRetainUntil, &r.LegalHoldStatus, &r.ACL, &tagsRaw,
			&r.ChecksumAlgorithm, &r.ChecksumValue,
		); err != nil {
			return fmt.Errorf("snapshot s3 version scan: %w", err)
		}
		r.Metadata = json.RawMessage(metaRaw)
		r.Tags = json.RawMessage(tagsRaw)
		snap.Versions = append(snap.Versions, r)
	}
	if err := vrows.Err(); err != nil {
		return err
	}

	// Multipart uploads
	urows, err := s.pool.Query(ctx, `SELECT upload_id, bucket, key, meta FROM jc_s3_multipart_uploads ORDER BY upload_id`)
	if err != nil {
		return fmt.Errorf("snapshot s3 uploads: %w", err)
	}
	defer urows.Close()
	for urows.Next() {
		var r pgS3UploadRow
		var raw []byte
		if err := urows.Scan(&r.UploadID, &r.Bucket, &r.Key, &raw); err != nil {
			return fmt.Errorf("snapshot s3 upload scan: %w", err)
		}
		r.Meta = json.RawMessage(raw)
		snap.Uploads = append(snap.Uploads, r)
	}
	if err := urows.Err(); err != nil {
		return err
	}

	// Parts
	prows, err := s.pool.Query(ctx, `SELECT upload_id, part_number, etag, size FROM jc_s3_multipart_parts ORDER BY upload_id, part_number`)
	if err != nil {
		return fmt.Errorf("snapshot s3 parts: %w", err)
	}
	defer prows.Close()
	for prows.Next() {
		var r pgS3PartRow
		if err := prows.Scan(&r.UploadID, &r.PartNumber, &r.ETag, &r.Size); err != nil {
			return fmt.Errorf("snapshot s3 part scan: %w", err)
		}
		snap.Parts = append(snap.Parts, r)
	}
	if err := prows.Err(); err != nil {
		return err
	}

	return json.NewEncoder(w).Encode(snap)
}

func (s *PostgresS3ObjectMetaStore) Restore(ctx context.Context, r io.Reader) error {
	var snap pgS3Snap
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("restore s3 begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, stmt := range []string{
		`DELETE FROM jc_s3_multipart_parts`,
		`DELETE FROM jc_s3_multipart_uploads`,
		`DELETE FROM jc_s3_object_versions`,
		`DELETE FROM jc_s3_objects`,
		`DELETE FROM jc_s3_buckets`,
	} {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("restore s3 truncate (%s): %w", stmt, err)
		}
	}

	for _, b := range snap.Buckets {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_s3_buckets (name, owner_account_id, region, meta) VALUES ($1,$2,$3,$4)
		`, b.Name, b.OwnerAccountID, b.Region, json.RawMessage(b.Meta)); err != nil {
			return fmt.Errorf("restore s3 insert bucket: %w", err)
		}
	}

	for _, o := range snap.Objects {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_s3_objects
				(bucket, key, etag, crc32, size, content_type, last_modified, metadata,
				 storage_class, version_id, tags, encryption, kms_key_id, ssec_key_md5,
				 lock_mode, lock_retain_until, legal_hold_status, acl, checksum_algorithm, checksum_value)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
		`, o.Bucket, o.Key, o.ETag, o.CRC32, o.Size, o.ContentType, o.LastModified,
			json.RawMessage(o.Metadata), o.StorageClass, o.VersionID,
			json.RawMessage(o.Tags), o.Encryption, o.KMSKeyID, o.SSECKeyMD5,
			o.LockMode, o.LockRetainUntil, o.LegalHoldStatus, o.ACL,
			o.ChecksumAlgorithm, o.ChecksumValue,
		); err != nil {
			return fmt.Errorf("restore s3 insert object: %w", err)
		}
	}

	for _, v := range snap.Versions {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_s3_object_versions
				(bucket, key, version_id, is_delete_marker, is_latest, etag, crc32, size,
				 content_type, last_modified, metadata, storage_class, encryption, kms_key_id,
				 ssec_key_md5, lock_mode, lock_retain_until, legal_hold_status, acl, tags,
				 checksum_algorithm, checksum_value)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)
		`, v.Bucket, v.Key, v.VersionID, v.IsDeleteMarker, v.IsLatest,
			v.ETag, v.CRC32, v.Size, v.ContentType, v.LastModified,
			json.RawMessage(v.Metadata), v.StorageClass, v.Encryption, v.KMSKeyID, v.SSECKeyMD5,
			v.LockMode, v.LockRetainUntil, v.LegalHoldStatus, v.ACL, json.RawMessage(v.Tags),
			v.ChecksumAlgorithm, v.ChecksumValue,
		); err != nil {
			return fmt.Errorf("restore s3 insert version: %w", err)
		}
	}

	for _, u := range snap.Uploads {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_s3_multipart_uploads (upload_id, bucket, key, meta) VALUES ($1,$2,$3,$4)
		`, u.UploadID, u.Bucket, u.Key, json.RawMessage(u.Meta)); err != nil {
			return fmt.Errorf("restore s3 insert upload: %w", err)
		}
	}

	for _, p := range snap.Parts {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_s3_multipart_parts (upload_id, part_number, etag, size) VALUES ($1,$2,$3,$4)
		`, p.UploadID, p.PartNumber, p.ETag, p.Size); err != nil {
			return fmt.Errorf("restore s3 insert part: %w", err)
		}
	}

	return tx.Commit(ctx)
}
