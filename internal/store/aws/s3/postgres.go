package s3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
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
	raw, _ := json.Marshal(meta)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_s3_buckets (name, meta) VALUES ($1, $2)
	`, bucket, json.RawMessage(raw))
	if err != nil {
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

func (s *PostgresS3ObjectMetaStore) ListBuckets(ctx context.Context) ([]map[string]any, error) {
	rows, err := s.pool.Query(ctx, `SELECT meta FROM jc_s3_buckets ORDER BY created_at`)
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
		INSERT INTO jc_s3_objects (bucket, key, etag, crc32, size, content_type, last_modified, metadata, storage_class, version_id, tags, encryption, kms_key_id, ssec_key_md5, lock_mode, lock_retain_until, legal_hold_status, acl)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
		ON CONFLICT (bucket, key) DO UPDATE
			SET etag=$3, crc32=$4, size=$5, content_type=$6, last_modified=$7, metadata=$8, storage_class=$9, version_id=$10,
			    tags=$11, encryption=$12, kms_key_id=$13, ssec_key_md5=$14, lock_mode=$15, lock_retain_until=$16, legal_hold_status=$17, acl=$18
	`, bucket, key, meta.ETag, meta.CRC32, meta.Size, meta.ContentType, meta.LastModified,
		json.RawMessage(metaRaw), meta.StorageClass, meta.VersionID,
		json.RawMessage(tagsRaw), meta.Encryption, meta.KMSKeyID, meta.SSECKeyMD5,
		meta.LockMode, meta.LockRetainUntil, meta.LegalHoldStatus, meta.ACL)
	return err
}

func (s *PostgresS3ObjectMetaStore) GetObjectMeta(ctx context.Context, bucket, key string) (ObjectMeta, error) {
	var m ObjectMeta
	var metaRaw, tagsRaw []byte
	err := s.pool.QueryRow(ctx, `
		SELECT key, etag, crc32, size, content_type, last_modified, metadata, storage_class, version_id,
		       tags, encryption, kms_key_id, ssec_key_md5, lock_mode, lock_retain_until, legal_hold_status, acl
		FROM jc_s3_objects WHERE bucket=$1 AND key=$2
	`, bucket, key).Scan(
		&m.Key, &m.ETag, &m.CRC32, &m.Size, &m.ContentType, &m.LastModified, &metaRaw, &m.StorageClass, &m.VersionID,
		&tagsRaw, &m.Encryption, &m.KMSKeyID, &m.SSECKeyMD5, &m.LockMode, &m.LockRetainUntil, &m.LegalHoldStatus, &m.ACL)
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
		  (bucket, key, version_id, is_delete_marker, is_latest, etag, crc32, size, content_type, last_modified, metadata, storage_class, encryption, kms_key_id, ssec_key_md5, lock_mode, lock_retain_until, legal_hold_status, acl, tags)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
		ON CONFLICT (bucket, key, version_id) DO UPDATE
		  SET is_latest=$5, etag=$6, crc32=$7, size=$8, content_type=$9, last_modified=$10, metadata=$11
	`, bucket, key, meta.VersionID, meta.IsDeleteMarker, true,
		meta.ETag, meta.CRC32, meta.Size, meta.ContentType, meta.LastModified,
		json.RawMessage(metaRaw), meta.StorageClass, meta.Encryption, meta.KMSKeyID, meta.SSECKeyMD5,
		meta.LockMode, meta.LockRetainUntil, meta.LegalHoldStatus, meta.ACL, json.RawMessage(tagsRaw))
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
		       is_delete_marker, is_latest, lock_mode, lock_retain_until, legal_hold_status, acl, encryption, kms_key_id, ssec_key_md5, tags
		FROM jc_s3_object_versions WHERE bucket=$1 AND key=$2 AND version_id=$3
	`, bucket, key, versionID).Scan(
		&m.Key, &m.ETag, &m.CRC32, &m.Size, &m.ContentType, &m.LastModified, &metaRaw, &m.StorageClass, &m.VersionID,
		&m.IsDeleteMarker, &m.IsLatest, &m.LockMode, &m.LockRetainUntil, &m.LegalHoldStatus, &m.ACL, &m.Encryption, &m.KMSKeyID, &m.SSECKeyMD5, &tagsRaw)
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

func (s *PostgresS3ObjectMetaStore) Reset() {
	ctx := context.Background()
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
