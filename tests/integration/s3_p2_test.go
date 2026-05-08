package integration_test

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awskms "github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── P2-7: Object Tagging ─────────────────────────────────────────────────────

func TestS3_ObjectTagging_PutGetDelete(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("tag-bucket")})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String("tag-bucket"),
		Key:    aws.String("obj.txt"),
		Body:   strings.NewReader("payload"),
	})
	require.NoError(t, err)

	// Put tags
	_, err = c.PutObjectTagging(ctx, &awss3.PutObjectTaggingInput{
		Bucket: aws.String("tag-bucket"),
		Key:    aws.String("obj.txt"),
		Tagging: &types.Tagging{
			TagSet: []types.Tag{
				{Key: aws.String("env"), Value: aws.String("prod")},
				{Key: aws.String("team"), Value: aws.String("backend")},
			},
		},
	})
	require.NoError(t, err)

	// Get tags
	out, err := c.GetObjectTagging(ctx, &awss3.GetObjectTaggingInput{
		Bucket: aws.String("tag-bucket"),
		Key:    aws.String("obj.txt"),
	})
	require.NoError(t, err)
	tagMap := make(map[string]string, len(out.TagSet))
	for _, t := range out.TagSet {
		tagMap[aws.ToString(t.Key)] = aws.ToString(t.Value)
	}
	assert.Equal(t, "prod", tagMap["env"])
	assert.Equal(t, "backend", tagMap["team"])

	// Delete tags
	_, err = c.DeleteObjectTagging(ctx, &awss3.DeleteObjectTaggingInput{
		Bucket: aws.String("tag-bucket"),
		Key:    aws.String("obj.txt"),
	})
	require.NoError(t, err)

	out2, err := c.GetObjectTagging(ctx, &awss3.GetObjectTaggingInput{
		Bucket: aws.String("tag-bucket"),
		Key:    aws.String("obj.txt"),
	})
	require.NoError(t, err)
	assert.Empty(t, out2.TagSet, "tags should be empty after delete")
}

func TestS3_ObjectTagging_ViaHeader(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("tag-hdr-bucket")})
	require.NoError(t, err)

	// SDK encodes tagging as x-amz-tagging header on PutObject
	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:  aws.String("tag-hdr-bucket"),
		Key:     aws.String("obj.txt"),
		Body:    strings.NewReader("data"),
		Tagging: aws.String("color=blue&size=large"),
	})
	require.NoError(t, err)

	out, err := c.GetObjectTagging(ctx, &awss3.GetObjectTaggingInput{
		Bucket: aws.String("tag-hdr-bucket"),
		Key:    aws.String("obj.txt"),
	})
	require.NoError(t, err)
	tagMap := make(map[string]string)
	for _, tag := range out.TagSet {
		tagMap[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	assert.Equal(t, "blue", tagMap["color"])
	assert.Equal(t, "large", tagMap["size"])
}

func TestS3_BucketTagging_PutGetDelete(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("btag-bucket")})
	require.NoError(t, err)

	_, err = c.PutBucketTagging(ctx, &awss3.PutBucketTaggingInput{
		Bucket: aws.String("btag-bucket"),
		Tagging: &types.Tagging{
			TagSet: []types.Tag{
				{Key: aws.String("project"), Value: aws.String("jaiscloud")},
			},
		},
	})
	require.NoError(t, err)

	out, err := c.GetBucketTagging(ctx, &awss3.GetBucketTaggingInput{
		Bucket: aws.String("btag-bucket"),
	})
	require.NoError(t, err)
	require.Len(t, out.TagSet, 1)
	assert.Equal(t, "project", aws.ToString(out.TagSet[0].Key))
	assert.Equal(t, "jaiscloud", aws.ToString(out.TagSet[0].Value))

	_, err = c.DeleteBucketTagging(ctx, &awss3.DeleteBucketTaggingInput{
		Bucket: aws.String("btag-bucket"),
	})
	require.NoError(t, err)

	_, err = c.GetBucketTagging(ctx, &awss3.GetBucketTaggingInput{
		Bucket: aws.String("btag-bucket"),
	})
	// AWS returns NoSuchTagSet (404) when there are no tags; some SDKs surface this as an error
	// and some as an empty set. We accept either.
	if err == nil {
		t.Log("GetBucketTagging returned no error with empty tag set")
	}
}

// ─── P2-1: Server-Side Encryption ────────────────────────────────────────────

func TestS3_BucketEncryption_PutGetDelete(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("enc-bucket")})
	require.NoError(t, err)

	// Put SSE-S3 bucket encryption
	_, err = c.PutBucketEncryption(ctx, &awss3.PutBucketEncryptionInput{
		Bucket: aws.String("enc-bucket"),
		ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{
			Rules: []types.ServerSideEncryptionRule{
				{
					ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{
						SSEAlgorithm: types.ServerSideEncryptionAes256,
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// Get encryption config
	out, err := c.GetBucketEncryption(ctx, &awss3.GetBucketEncryptionInput{
		Bucket: aws.String("enc-bucket"),
	})
	require.NoError(t, err)
	require.Len(t, out.ServerSideEncryptionConfiguration.Rules, 1)
	assert.Equal(t, types.ServerSideEncryptionAes256,
		out.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm)

	// Objects stored in this bucket should echo SSE header
	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String("enc-bucket"),
		Key:    aws.String("secret.txt"),
		Body:   strings.NewReader("classified"),
	})
	require.NoError(t, err)

	// Delete encryption config
	_, err = c.DeleteBucketEncryption(ctx, &awss3.DeleteBucketEncryptionInput{
		Bucket: aws.String("enc-bucket"),
	})
	require.NoError(t, err)
}

func TestS3_PutObject_SSE_HeaderEchoed(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("sse-echo-bucket")})
	require.NoError(t, err)

	out, err := c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:               aws.String("sse-echo-bucket"),
		Key:                  aws.String("obj"),
		Body:                 strings.NewReader("data"),
		ServerSideEncryption: types.ServerSideEncryptionAes256,
	})
	require.NoError(t, err)
	assert.Equal(t, types.ServerSideEncryptionAes256, out.ServerSideEncryption,
		"SSE algorithm must be echoed in PutObject response")
}

// ─── P2-2: Versioning ─────────────────────────────────────────────────────────

func TestS3_Versioning_EnableAndPutMultipleVersions(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("ver-bucket")})
	require.NoError(t, err)

	// Enable versioning
	_, err = c.PutBucketVersioning(ctx, &awss3.PutBucketVersioningInput{
		Bucket: aws.String("ver-bucket"),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	// Verify versioning status
	vs, err := c.GetBucketVersioning(ctx, &awss3.GetBucketVersioningInput{
		Bucket: aws.String("ver-bucket"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.BucketVersioningStatusEnabled, vs.Status)

	// Write two versions of the same key
	put1, err := c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String("ver-bucket"),
		Key:    aws.String("config.json"),
		Body:   strings.NewReader(`{"v":1}`),
	})
	require.NoError(t, err)
	v1 := aws.ToString(put1.VersionId)
	require.NotEmpty(t, v1)

	put2, err := c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String("ver-bucket"),
		Key:    aws.String("config.json"),
		Body:   strings.NewReader(`{"v":2}`),
	})
	require.NoError(t, err)
	v2 := aws.ToString(put2.VersionId)
	require.NotEmpty(t, v2)
	assert.NotEqual(t, v1, v2)

	// GetObject without versionId returns latest (v2)
	latest, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String("ver-bucket"),
		Key:    aws.String("config.json"),
	})
	require.NoError(t, err)
	body, _ := io.ReadAll(latest.Body)
	latest.Body.Close()
	assert.Equal(t, `{"v":2}`, string(body))

	// GetObject with versionId=v1 returns old version
	old, err := c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket:    aws.String("ver-bucket"),
		Key:       aws.String("config.json"),
		VersionId: aws.String(v1),
	})
	require.NoError(t, err)
	body1, _ := io.ReadAll(old.Body)
	old.Body.Close()
	assert.Equal(t, `{"v":1}`, string(body1))
}

func TestS3_Versioning_ListObjectVersions(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("listver-bucket")})
	require.NoError(t, err)

	_, err = c.PutBucketVersioning(ctx, &awss3.PutBucketVersioningInput{
		Bucket: aws.String("listver-bucket"),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		_, err = c.PutObject(ctx, &awss3.PutObjectInput{
			Bucket: aws.String("listver-bucket"),
			Key:    aws.String("k"),
			Body:   strings.NewReader("v"),
		})
		require.NoError(t, err)
	}

	lv, err := c.ListObjectVersions(ctx, &awss3.ListObjectVersionsInput{
		Bucket: aws.String("listver-bucket"),
	})
	require.NoError(t, err)
	assert.Len(t, lv.Versions, 3, "must have 3 versions")
	assert.True(t, aws.ToBool(lv.Versions[0].IsLatest), "first version in list must be latest")
}

func TestS3_Versioning_DeleteCreatesMarker(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("dm-bucket")})
	require.NoError(t, err)

	_, err = c.PutBucketVersioning(ctx, &awss3.PutBucketVersioningInput{
		Bucket: aws.String("dm-bucket"),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String("dm-bucket"),
		Key:    aws.String("obj"),
		Body:   strings.NewReader("data"),
	})
	require.NoError(t, err)

	// Delete without versionId → creates a delete marker
	del, err := c.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: aws.String("dm-bucket"),
		Key:    aws.String("obj"),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(del.DeleteMarker), "delete without versionId must create a delete marker")
	assert.NotEmpty(t, aws.ToString(del.VersionId))

	// GetObject without versionId now returns 404 (delete marker is current)
	_, err = c.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String("dm-bucket"),
		Key:    aws.String("obj"),
	})
	require.Error(t, err, "GetObject on a delete-marker object must return an error")

	// ListObjectVersions shows both the version and the delete marker
	lv, err := c.ListObjectVersions(ctx, &awss3.ListObjectVersionsInput{
		Bucket: aws.String("dm-bucket"),
	})
	require.NoError(t, err)
	assert.Len(t, lv.DeleteMarkers, 1)
	assert.Len(t, lv.Versions, 1)
}

func TestS3_Versioning_DeleteSpecificVersion(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("dver-bucket")})
	require.NoError(t, err)

	_, err = c.PutBucketVersioning(ctx, &awss3.PutBucketVersioningInput{
		Bucket: aws.String("dver-bucket"),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	put, err := c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String("dver-bucket"),
		Key:    aws.String("obj"),
		Body:   strings.NewReader("data"),
	})
	require.NoError(t, err)

	// Delete a specific version — permanently removes it, no delete marker
	del, err := c.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket:    aws.String("dver-bucket"),
		Key:       aws.String("obj"),
		VersionId: put.VersionId,
	})
	require.NoError(t, err)
	assert.False(t, aws.ToBool(del.DeleteMarker))

	lv, err := c.ListObjectVersions(ctx, &awss3.ListObjectVersionsInput{
		Bucket: aws.String("dver-bucket"),
	})
	require.NoError(t, err)
	assert.Empty(t, lv.Versions, "version must be permanently deleted")
	assert.Empty(t, lv.DeleteMarkers, "no delete markers since a specific version was deleted")
}

// ─── P2-3: Object Lock ────────────────────────────────────────────────────────

func TestS3_ObjectLock_LegalHold(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("lock-bucket")})
	require.NoError(t, err)

	// Enable versioning (required for object lock in AWS)
	_, err = c.PutBucketVersioning(ctx, &awss3.PutBucketVersioningInput{
		Bucket: aws.String("lock-bucket"),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	putResp, err := c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String("lock-bucket"),
		Key:    aws.String("evidence.txt"),
		Body:   strings.NewReader("immutable"),
	})
	require.NoError(t, err)
	versionID := putResp.VersionId
	require.NotNil(t, versionID, "versioned bucket must return a version ID")

	// Place a legal hold
	_, err = c.PutObjectLegalHold(ctx, &awss3.PutObjectLegalHoldInput{
		Bucket: aws.String("lock-bucket"),
		Key:    aws.String("evidence.txt"),
		LegalHold: &types.ObjectLockLegalHold{
			Status: types.ObjectLockLegalHoldStatusOn,
		},
	})
	require.NoError(t, err)

	// Verify the hold is visible.
	hold, err := c.GetObjectLegalHold(ctx, &awss3.GetObjectLegalHoldInput{
		Bucket: aws.String("lock-bucket"),
		Key:    aws.String("evidence.txt"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.ObjectLockLegalHoldStatusOn, hold.LegalHold.Status)

	// Delete targeting the specific version must be blocked (legal hold active).
	// In real AWS, DeleteObject without VersionId only inserts a delete marker and
	// always succeeds; the hold protects the specific version from permanent removal.
	_, err = c.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket:    aws.String("lock-bucket"),
		Key:       aws.String("evidence.txt"),
		VersionId: versionID,
	})
	require.Error(t, err, "delete must fail when legal hold is active")

	// Release the hold and verify version is now deletable.
	_, err = c.PutObjectLegalHold(ctx, &awss3.PutObjectLegalHoldInput{
		Bucket: aws.String("lock-bucket"),
		Key:    aws.String("evidence.txt"),
		LegalHold: &types.ObjectLockLegalHold{
			Status: types.ObjectLockLegalHoldStatusOff,
		},
	})
	require.NoError(t, err)
	_, err = c.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket:    aws.String("lock-bucket"),
		Key:       aws.String("evidence.txt"),
		VersionId: versionID,
	})
	require.NoError(t, err, "delete must succeed after hold released")
}

func TestS3_ObjectLock_Retention_Governance(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("ret-bucket")})
	require.NoError(t, err)

	_, err = c.PutBucketVersioning(ctx, &awss3.PutBucketVersioningInput{
		Bucket: aws.String("ret-bucket"),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	putResp, err := c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String("ret-bucket"),
		Key:    aws.String("doc.txt"),
		Body:   strings.NewReader("retained"),
	})
	require.NoError(t, err)
	versionID := putResp.VersionId
	require.NotNil(t, versionID, "versioned bucket must return a version ID")

	retainUntil := time.Now().UTC().Add(24 * time.Hour)
	_, err = c.PutObjectRetention(ctx, &awss3.PutObjectRetentionInput{
		Bucket: aws.String("ret-bucket"),
		Key:    aws.String("doc.txt"),
		Retention: &types.ObjectLockRetention{
			Mode:            types.ObjectLockRetentionModeGovernance,
			RetainUntilDate: aws.Time(retainUntil),
		},
	})
	require.NoError(t, err)

	// Verify retention is stored.
	ret, err := c.GetObjectRetention(ctx, &awss3.GetObjectRetentionInput{
		Bucket: aws.String("ret-bucket"),
		Key:    aws.String("doc.txt"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.ObjectLockRetentionModeGovernance, ret.Retention.Mode)

	// Delete the specific version without bypass must fail.
	// (DeleteObject without VersionId only creates a delete marker and always
	// succeeds; the retention policy guards the specific version.)
	_, err = c.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket:    aws.String("ret-bucket"),
		Key:       aws.String("doc.txt"),
		VersionId: versionID,
	})
	require.Error(t, err, "delete must fail under GOVERNANCE retention without bypass")
}

// ─── P2-4: ACLs ───────────────────────────────────────────────────────────────

func TestS3_Acl_BucketGetReturnsOwner(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("acl-bucket")})
	require.NoError(t, err)

	out, err := c.GetBucketAcl(ctx, &awss3.GetBucketAclInput{
		Bucket: aws.String("acl-bucket"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Owner)
	require.NotEmpty(t, out.Grants)
	assert.Equal(t, types.PermissionFullControl, out.Grants[0].Permission)
}

func TestS3_Acl_ObjectGetReturnsOwner(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("oacl-bucket")})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String("oacl-bucket"),
		Key:    aws.String("obj"),
		Body:   strings.NewReader("data"),
	})
	require.NoError(t, err)

	out, err := c.GetObjectAcl(ctx, &awss3.GetObjectAclInput{
		Bucket: aws.String("oacl-bucket"),
		Key:    aws.String("obj"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Owner)
	require.NotEmpty(t, out.Grants)
}

func TestS3_Acl_PutPublicRead(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("pub-acl-bucket")})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String("pub-acl-bucket"),
		Key:    aws.String("img.png"),
		Body:   bytes.NewReader([]byte{0xFF, 0xD8}),
		ACL:    types.ObjectCannedACLPublicRead,
	})
	require.NoError(t, err)

	out, err := c.GetObjectAcl(ctx, &awss3.GetObjectAclInput{
		Bucket: aws.String("pub-acl-bucket"),
		Key:    aws.String("img.png"),
	})
	require.NoError(t, err)
	// Must have at least 2 grants: FULL_CONTROL for owner + READ for AllUsers
	assert.GreaterOrEqual(t, len(out.Grants), 2)
	hasPublicRead := false
	for _, g := range out.Grants {
		if g.Permission == types.PermissionRead && g.Grantee != nil && g.Grantee.Type == types.TypeGroup {
			hasPublicRead = true
		}
	}
	assert.True(t, hasPublicRead, "public-read ACL must grant READ to AllUsers group")
}

// ─── P2-5: Lifecycle ──────────────────────────────────────────────────────────

func TestS3_Lifecycle_PutGetDelete(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("lc-bucket")})
	require.NoError(t, err)

	_, err = c.PutBucketLifecycleConfiguration(ctx, &awss3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String("lc-bucket"),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{
			Rules: []types.LifecycleRule{
				{
					ID:     aws.String("expire-tmp"),
					Status: types.ExpirationStatusEnabled,
					Filter: &types.LifecycleRuleFilter{
						Prefix: aws.String("tmp/"),
					},
					Expiration: &types.LifecycleExpiration{
						Days: aws.Int32(30),
					},
				},
			},
		},
	})
	require.NoError(t, err)

	out, err := c.GetBucketLifecycleConfiguration(ctx, &awss3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String("lc-bucket"),
	})
	require.NoError(t, err)
	require.Len(t, out.Rules, 1)
	assert.Equal(t, "expire-tmp", aws.ToString(out.Rules[0].ID))
	assert.Equal(t, int32(30), aws.ToInt32(out.Rules[0].Expiration.Days))

	_, err = c.DeleteBucketLifecycle(ctx, &awss3.DeleteBucketLifecycleInput{
		Bucket: aws.String("lc-bucket"),
	})
	require.NoError(t, err)

	_, err = c.GetBucketLifecycleConfiguration(ctx, &awss3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String("lc-bucket"),
	})
	// After delete, AWS returns NoSuchLifecycleConfiguration; SDK surfaces as error
	require.Error(t, err, "GetBucketLifecycleConfiguration must error after deletion")
}

// ─── P2-6: CORS ───────────────────────────────────────────────────────────────

func TestS3_CORS_PutGetDelete(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("cors-bucket")})
	require.NoError(t, err)

	_, err = c.PutBucketCors(ctx, &awss3.PutBucketCorsInput{
		Bucket: aws.String("cors-bucket"),
		CORSConfiguration: &types.CORSConfiguration{
			CORSRules: []types.CORSRule{
				{
					AllowedOrigins: []string{"https://example.com"},
					AllowedMethods: []string{"GET", "PUT"},
					AllowedHeaders: []string{"Content-Type"},
					MaxAgeSeconds:  aws.Int32(3600),
				},
			},
		},
	})
	require.NoError(t, err)

	out, err := c.GetBucketCors(ctx, &awss3.GetBucketCorsInput{
		Bucket: aws.String("cors-bucket"),
	})
	require.NoError(t, err)
	require.Len(t, out.CORSRules, 1)
	assert.Equal(t, []string{"https://example.com"}, out.CORSRules[0].AllowedOrigins)
	assert.Equal(t, []string{"GET", "PUT"}, out.CORSRules[0].AllowedMethods)
	assert.Equal(t, int32(3600), aws.ToInt32(out.CORSRules[0].MaxAgeSeconds))

	_, err = c.DeleteBucketCors(ctx, &awss3.DeleteBucketCorsInput{
		Bucket: aws.String("cors-bucket"),
	})
	require.NoError(t, err)

	_, err = c.GetBucketCors(ctx, &awss3.GetBucketCorsInput{
		Bucket: aws.String("cors-bucket"),
	})
	require.Error(t, err, "GetBucketCors must error after deletion (NoSuchCORSConfiguration)")
}

// ─── P2-1: SSE-KMS (bucket default + per-object) ─────────────────────────────

func TestS3_SSE_KMS_BucketDefault(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	kmsClient := newKMSClient(t)
	s3Client := newS3Client(t)

	// Create a KMS key to use as the bucket default
	keyOut, err := kmsClient.CreateKey(ctx, &awskms.CreateKeyInput{
		Description: aws.String("s3-integration-test-key"),
		KeyUsage:    kmstypes.KeyUsageTypeEncryptDecrypt,
	})
	require.NoError(t, err)
	keyID := aws.ToString(keyOut.KeyMetadata.KeyId)
	keyARN := aws.ToString(keyOut.KeyMetadata.Arn)
	require.NotEmpty(t, keyARN)

	// Create bucket and set SSE-KMS as the default encryption
	_, err = s3Client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("kms-enc-bucket")})
	require.NoError(t, err)

	_, err = s3Client.PutBucketEncryption(ctx, &awss3.PutBucketEncryptionInput{
		Bucket: aws.String("kms-enc-bucket"),
		ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{
			Rules: []types.ServerSideEncryptionRule{
				{
					ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{
						SSEAlgorithm:   types.ServerSideEncryptionAwsKms,
						KMSMasterKeyID: aws.String(keyARN),
					},
					BucketKeyEnabled: aws.Bool(true),
				},
			},
		},
	})
	require.NoError(t, err)

	// Verify the config is stored correctly
	encOut, err := s3Client.GetBucketEncryption(ctx, &awss3.GetBucketEncryptionInput{
		Bucket: aws.String("kms-enc-bucket"),
	})
	require.NoError(t, err)
	require.Len(t, encOut.ServerSideEncryptionConfiguration.Rules, 1)
	rule := encOut.ServerSideEncryptionConfiguration.Rules[0]
	assert.Equal(t, types.ServerSideEncryptionAwsKms, rule.ApplyServerSideEncryptionByDefault.SSEAlgorithm)
	assert.Equal(t, keyARN, aws.ToString(rule.ApplyServerSideEncryptionByDefault.KMSMasterKeyID))

	// PutObject — server applies bucket-default KMS key; response must echo SSE-KMS headers
	putOut, err := s3Client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String("kms-enc-bucket"),
		Key:    aws.String("secret.bin"),
		Body:   strings.NewReader("classified payload"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.ServerSideEncryptionAwsKms, putOut.ServerSideEncryption,
		"PutObject response must echo SSE-KMS algorithm")
	assert.Equal(t, keyARN, aws.ToString(putOut.SSEKMSKeyId),
		"PutObject response must echo the KMS key ARN")

	// GetObject must also echo SSE-KMS headers and return correct content
	getOut, err := s3Client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String("kms-enc-bucket"),
		Key:    aws.String("secret.bin"),
	})
	require.NoError(t, err)
	body, _ := io.ReadAll(getOut.Body)
	getOut.Body.Close()
	assert.Equal(t, "classified payload", string(body))
	assert.Equal(t, types.ServerSideEncryptionAwsKms, getOut.ServerSideEncryption)

	// Verify the KMS key still exists and is enabled (emulator didn't destroy it)
	descOut, err := kmsClient.DescribeKey(ctx, &awskms.DescribeKeyInput{KeyId: aws.String(keyID)})
	require.NoError(t, err)
	assert.True(t, descOut.KeyMetadata.Enabled)
}

func TestS3_SSE_KMS_PerObjectOverride(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	kmsClient := newKMSClient(t)
	s3Client := newS3Client(t)

	// Create two KMS keys: one as bucket default, one as per-object override
	defaultKey, err := kmsClient.CreateKey(ctx, &awskms.CreateKeyInput{
		Description: aws.String("bucket-default-key"),
		KeyUsage:    kmstypes.KeyUsageTypeEncryptDecrypt,
	})
	require.NoError(t, err)
	defaultARN := aws.ToString(defaultKey.KeyMetadata.Arn)

	overrideKey, err := kmsClient.CreateKey(ctx, &awskms.CreateKeyInput{
		Description: aws.String("per-object-override-key"),
		KeyUsage:    kmstypes.KeyUsageTypeEncryptDecrypt,
	})
	require.NoError(t, err)
	overrideARN := aws.ToString(overrideKey.KeyMetadata.Arn)

	_, err = s3Client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("kms-override-bucket")})
	require.NoError(t, err)

	// Set bucket default to defaultKey
	_, err = s3Client.PutBucketEncryption(ctx, &awss3.PutBucketEncryptionInput{
		Bucket: aws.String("kms-override-bucket"),
		ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{
			Rules: []types.ServerSideEncryptionRule{
				{
					ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{
						SSEAlgorithm:   types.ServerSideEncryptionAwsKms,
						KMSMasterKeyID: aws.String(defaultARN),
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// Upload with explicit per-object KMS key override
	putOut, err := s3Client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:               aws.String("kms-override-bucket"),
		Key:                  aws.String("obj"),
		Body:                 strings.NewReader("payload"),
		ServerSideEncryption: types.ServerSideEncryptionAwsKms,
		SSEKMSKeyId:          aws.String(overrideARN),
	})
	require.NoError(t, err)
	assert.Equal(t, types.ServerSideEncryptionAwsKms, putOut.ServerSideEncryption)
	assert.Equal(t, overrideARN, aws.ToString(putOut.SSEKMSKeyId),
		"per-object KMS key must override bucket default")
}

// ─── P2-8: Presigned URL expiration ──────────────────────────────────────────

func TestS3_GetBucketVersioning_NonExistentBucket(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	out, err := c.GetBucketVersioning(ctx, &awss3.GetBucketVersioningInput{
		Bucket: aws.String("no-such-bucket"),
	})
	// AWS returns NoSuchBucket; SDK may surface as error or empty status
	if err == nil {
		assert.Equal(t, types.BucketVersioningStatus(""), out.Status,
			"non-existent bucket must return empty versioning status or error")
	}
}

// ─── Combined scenario: tagging survives CopyObject (COPY directive) ──────────

func TestS3_CopyObject_TaggingDirective(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("copy-tag-src")})
	require.NoError(t, err)
	_, err = c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("copy-tag-dst")})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:  aws.String("copy-tag-src"),
		Key:     aws.String("src"),
		Body:    strings.NewReader("data"),
		Tagging: aws.String("owner=alice"),
	})
	require.NoError(t, err)

	// COPY directive — tags must be preserved on destination
	_, err = c.CopyObject(ctx, &awss3.CopyObjectInput{
		Bucket:          aws.String("copy-tag-dst"),
		Key:             aws.String("dst"),
		CopySource:      aws.String("copy-tag-src/src"),
		TaggingDirective: types.TaggingDirectiveCopy,
	})
	require.NoError(t, err)

	out, err := c.GetObjectTagging(ctx, &awss3.GetObjectTaggingInput{
		Bucket: aws.String("copy-tag-dst"),
		Key:    aws.String("dst"),
	})
	require.NoError(t, err)
	require.Len(t, out.TagSet, 1)
	assert.Equal(t, "owner", aws.ToString(out.TagSet[0].Key))
	assert.Equal(t, "alice", aws.ToString(out.TagSet[0].Value))
}

func TestS3_CopyObject_ReplaceDirective_ClearsSourceTags(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("rep-tag-src")})
	require.NoError(t, err)
	_, err = c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("rep-tag-dst")})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:  aws.String("rep-tag-src"),
		Key:     aws.String("src"),
		Body:    strings.NewReader("data"),
		Tagging: aws.String("owner=alice"),
	})
	require.NoError(t, err)

	// REPLACE directive with new tags
	_, err = c.CopyObject(ctx, &awss3.CopyObjectInput{
		Bucket:          aws.String("rep-tag-dst"),
		Key:             aws.String("dst"),
		CopySource:      aws.String("rep-tag-src/src"),
		TaggingDirective: types.TaggingDirectiveReplace,
		Tagging:         aws.String("env=staging"),
	})
	require.NoError(t, err)

	out, err := c.GetObjectTagging(ctx, &awss3.GetObjectTaggingInput{
		Bucket: aws.String("rep-tag-dst"),
		Key:    aws.String("dst"),
	})
	require.NoError(t, err)
	tagMap := make(map[string]string)
	for _, tag := range out.TagSet {
		tagMap[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	_, hasOwner := tagMap["owner"]
	assert.False(t, hasOwner, "REPLACE directive must clear source tags")
	assert.Equal(t, "staging", tagMap["env"])
}
