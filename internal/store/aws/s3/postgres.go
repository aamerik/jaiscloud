package s3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
		INSERT INTO jc_s3_objects (bucket, key, etag, size, content_type, last_modified, metadata, storage_class, version_id)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (bucket, key) DO UPDATE
			SET etag=$3, size=$4, content_type=$5, last_modified=$6, metadata=$7, storage_class=$8, version_id=$9
	`, bucket, key, meta.ETag, meta.Size, meta.ContentType, meta.LastModified, json.RawMessage(metaRaw), meta.StorageClass, meta.VersionID)
	return err
}

func (s *PostgresS3ObjectMetaStore) GetObjectMeta(ctx context.Context, bucket, key string) (ObjectMeta, error) {
	var m ObjectMeta
	var metaRaw []byte
	err := s.pool.QueryRow(ctx, `
		SELECT key, etag, size, content_type, last_modified, metadata, storage_class, version_id
		FROM jc_s3_objects WHERE bucket=$1 AND key=$2
	`, bucket, key).Scan(&m.Key, &m.ETag, &m.Size, &m.ContentType, &m.LastModified, &metaRaw, &m.StorageClass, &m.VersionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ObjectMeta{}, fmt.Errorf("NoSuchKey")
	}
	if err != nil {
		return ObjectMeta{}, err
	}
	json.Unmarshal(metaRaw, &m.Metadata)
	return m, nil
}

func (s *PostgresS3ObjectMetaStore) DeleteObjectMeta(ctx context.Context, bucket, key string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM jc_s3_objects WHERE bucket=$1 AND key=$2`, bucket, key)
	return err
}

func (s *PostgresS3ObjectMetaStore) ListObjectMeta(ctx context.Context, bucket, prefix, delimiter, marker string, maxKeys int) ([]ObjectMeta, []string, bool, string, error) {
	if maxKeys <= 0 {
		maxKeys = 1000
	}
	rows, err := s.pool.Query(ctx, `
		SELECT key, etag, size, content_type, last_modified, metadata, storage_class
		FROM jc_s3_objects
		WHERE bucket=$1 AND key LIKE $2 AND key > $3
		ORDER BY key LIMIT $4
	`, bucket, prefix+"%", marker, maxKeys+1)
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
	for _, m := range all {
		// AWS counts both result keys and unique common prefixes toward maxKeys.
		// Stop when the page is full; remaining items mean the result is truncated.
		if len(result)+len(commonPrefixes) >= maxKeys {
			truncated = true
			break
		}
		lastExaminedKey = m.Key
		if delimiter != "" {
			rest := m.Key[len(prefix):]
			idx := strings.Index(rest, delimiter)
			if idx >= 0 {
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
	sortStrings(cpList)

	nextMarker := ""
	if truncated {
		nextMarker = lastExaminedKey
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
	defer rows.Close()
	var parts []PartMeta
	for rows.Next() {
		var p PartMeta
		if err := rows.Scan(&p.PartNumber, &p.ETag, &p.Size); err != nil {
			return nil, err
		}
		parts = append(parts, p)
	}
	s.pool.Exec(ctx, `DELETE FROM jc_s3_multipart_uploads WHERE upload_id=$1`, uploadID)
	return parts, rows.Err()
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

func (s *PostgresS3ObjectMetaStore) Reset() {
	ctx := context.Background()
	s.pool.Exec(ctx, `DELETE FROM jc_s3_objects`)
	s.pool.Exec(ctx, `DELETE FROM jc_s3_buckets`)
	s.pool.Exec(ctx, `DELETE FROM jc_s3_multipart_uploads`)
}
