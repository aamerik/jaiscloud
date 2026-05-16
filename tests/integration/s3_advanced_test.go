package integration_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	awss3control "github.com/aws/aws-sdk-go-v2/service/s3control"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── Website Config ───────────────────────────────────────────────────────────

func TestS3_WebsiteRoundtrip(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "website-test-bucket"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	websiteXML := `<?xml version="1.0" encoding="UTF-8"?>
<WebsiteConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <IndexDocument><Suffix>index.html</Suffix></IndexDocument>
  <ErrorDocument><Key>error.html</Key></ErrorDocument>
</WebsiteConfiguration>`

	_, err = c.PutBucketWebsite(ctx, &awss3.PutBucketWebsiteInput{
		Bucket: aws.String(bucket),
		WebsiteConfiguration: &s3types.WebsiteConfiguration{
			IndexDocument: &s3types.IndexDocument{Suffix: aws.String("index.html")},
			ErrorDocument: &s3types.ErrorDocument{Key: aws.String("error.html")},
		},
	})
	require.NoError(t, err)
	_ = websiteXML

	out, err := c.GetBucketWebsite(ctx, &awss3.GetBucketWebsiteInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)
	require.NotNil(t, out.IndexDocument)
	assert.Equal(t, "index.html", aws.ToString(out.IndexDocument.Suffix))
	require.NotNil(t, out.ErrorDocument)
	assert.Equal(t, "error.html", aws.ToString(out.ErrorDocument.Key))
}

func TestS3_WebsiteDelete(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "website-delete-bucket"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = c.PutBucketWebsite(ctx, &awss3.PutBucketWebsiteInput{
		Bucket: aws.String(bucket),
		WebsiteConfiguration: &s3types.WebsiteConfiguration{
			IndexDocument: &s3types.IndexDocument{Suffix: aws.String("index.html")},
		},
	})
	require.NoError(t, err)

	_, err = c.DeleteBucketWebsite(ctx, &awss3.DeleteBucketWebsiteInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)

	_, err = c.GetBucketWebsite(ctx, &awss3.GetBucketWebsiteInput{
		Bucket: aws.String(bucket),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NoSuchWebsiteConfiguration")
}

// ─── Replication Config ───────────────────────────────────────────────────────

func TestS3_ReplicationRoundtrip(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	srcBucket := "repl-source-bucket"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(srcBucket)})
	require.NoError(t, err)

	// Enable versioning (required for replication)
	_, err = c.PutBucketVersioning(ctx, &awss3.PutBucketVersioningInput{
		Bucket: aws.String(srcBucket),
		VersioningConfiguration: &s3types.VersioningConfiguration{
			Status: s3types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	_, err = c.PutBucketReplication(ctx, &awss3.PutBucketReplicationInput{
		Bucket: aws.String(srcBucket),
		ReplicationConfiguration: &s3types.ReplicationConfiguration{
			Role: aws.String("arn:aws:iam::000000000000:role/replication-role"),
			Rules: []s3types.ReplicationRule{
				{
					ID:     aws.String("rule-1"),
					Status: s3types.ReplicationRuleStatusEnabled,
					Filter: &s3types.ReplicationRuleFilter{
						Prefix: aws.String(""),
					},
					Destination: &s3types.Destination{
						Bucket: aws.String("arn:aws:s3:::dest-bucket"),
					},
				},
			},
		},
	})
	require.NoError(t, err)

	out, err := c.GetBucketReplication(ctx, &awss3.GetBucketReplicationInput{
		Bucket: aws.String(srcBucket),
	})
	require.NoError(t, err)
	require.NotNil(t, out.ReplicationConfiguration)
	assert.Equal(t, "arn:aws:iam::000000000000:role/replication-role", aws.ToString(out.ReplicationConfiguration.Role))
	require.Len(t, out.ReplicationConfiguration.Rules, 1)
	assert.Equal(t, "rule-1", aws.ToString(out.ReplicationConfiguration.Rules[0].ID))
}

func TestS3_ReplicationDelete(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "repl-delete-bucket"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = c.PutBucketVersioning(ctx, &awss3.PutBucketVersioningInput{
		Bucket: aws.String(bucket),
		VersioningConfiguration: &s3types.VersioningConfiguration{
			Status: s3types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	_, err = c.PutBucketReplication(ctx, &awss3.PutBucketReplicationInput{
		Bucket: aws.String(bucket),
		ReplicationConfiguration: &s3types.ReplicationConfiguration{
			Role: aws.String("arn:aws:iam::000000000000:role/replication-role"),
			Rules: []s3types.ReplicationRule{
				{
					ID:     aws.String("rule-1"),
					Status: s3types.ReplicationRuleStatusEnabled,
					Destination: &s3types.Destination{
						Bucket: aws.String("arn:aws:s3:::dest-bucket"),
					},
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = c.DeleteBucketReplication(ctx, &awss3.DeleteBucketReplicationInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)

	_, err = c.GetBucketReplication(ctx, &awss3.GetBucketReplicationInput{
		Bucket: aws.String(bucket),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ReplicationConfigurationNotFoundError")
}

// ─── Access Points ────────────────────────────────────────────────────────────

func TestS3_AccessPointCRUD(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3ControlClient(t)
	s3c := newS3Client(t)

	bucket := "ap-test-bucket"
	_, err := s3c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	apName := "my-access-point"
	accountID := "000000000000"

	createOut, err := c.CreateAccessPoint(ctx, &awss3control.CreateAccessPointInput{
		AccountId: aws.String(accountID),
		Name:      aws.String(apName),
		Bucket:    aws.String(bucket),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(createOut.AccessPointArn), apName)

	getOut, err := c.GetAccessPoint(ctx, &awss3control.GetAccessPointInput{
		AccountId: aws.String(accountID),
		Name:      aws.String(apName),
	})
	require.NoError(t, err)
	assert.Equal(t, apName, aws.ToString(getOut.Name))
	assert.Equal(t, bucket, aws.ToString(getOut.Bucket))

	listOut, err := c.ListAccessPoints(ctx, &awss3control.ListAccessPointsInput{
		AccountId: aws.String(accountID),
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(listOut.AccessPointList), 1)

	found := false
	for _, ap := range listOut.AccessPointList {
		if aws.ToString(ap.Name) == apName {
			found = true
			break
		}
	}
	assert.True(t, found, "access point should be in list")

	_, err = c.DeleteAccessPoint(ctx, &awss3control.DeleteAccessPointInput{
		AccountId: aws.String(accountID),
		Name:      aws.String(apName),
	})
	require.NoError(t, err)

	_, err = c.GetAccessPoint(ctx, &awss3control.GetAccessPointInput{
		AccountId: aws.String(accountID),
		Name:      aws.String(apName),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NoSuchAccessPoint")
}

// ─── SelectObjectContent ──────────────────────────────────────────────────────

func TestS3_SelectObjectContent_CSV(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "select-csv-bucket"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	csvContent := "name,age,city\nalice,30,NYC\nbob,25,LA\ncharlie,35,Chicago\n"
	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("data.csv"),
		Body:   strings.NewReader(csvContent),
	})
	require.NoError(t, err)

	out, err := c.SelectObjectContent(ctx, &awss3.SelectObjectContentInput{
		Bucket:         aws.String(bucket),
		Key:            aws.String("data.csv"),
		Expression:     aws.String("SELECT * FROM S3Object"),
		ExpressionType: s3types.ExpressionTypeSql,
		InputSerialization: &s3types.InputSerialization{
			CSV: &s3types.CSVInput{
				FileHeaderInfo: s3types.FileHeaderInfoUse,
			},
		},
		OutputSerialization: &s3types.OutputSerialization{
			CSV: &s3types.CSVOutput{},
		},
	})
	require.NoError(t, err)

	// Consume the event stream
	var recordData []byte
	stream := out.GetStream()
	defer stream.Close()

	for event := range stream.Events() {
		if rec, ok := event.(*s3types.SelectObjectContentEventStreamMemberRecords); ok {
			recordData = append(recordData, rec.Value.Payload...)
		}
	}
	require.NoError(t, stream.Err())

	// The output should contain the CSV rows (without header since FileHeaderInfo=USE)
	result := string(recordData)
	assert.Contains(t, result, "alice")
	assert.Contains(t, result, "bob")
	assert.Contains(t, result, "charlie")
	// Header should not appear in output rows
	assert.NotContains(t, result, "name,age,city")
}

func TestS3_SelectObjectContent_JSONLines(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newS3Client(t)

	bucket := "select-json-bucket"
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	jsonLines := `{"name":"alice","score":100}
{"name":"bob","score":200}
{"name":"charlie","score":150}
`
	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("data.jsonl"),
		Body:   bytes.NewReader([]byte(jsonLines)),
	})
	require.NoError(t, err)

	out, err := c.SelectObjectContent(ctx, &awss3.SelectObjectContentInput{
		Bucket:         aws.String(bucket),
		Key:            aws.String("data.jsonl"),
		Expression:     aws.String("SELECT * FROM S3Object"),
		ExpressionType: s3types.ExpressionTypeSql,
		InputSerialization: &s3types.InputSerialization{
			JSON: &s3types.JSONInput{
				Type: s3types.JSONTypeLines,
			},
		},
		OutputSerialization: &s3types.OutputSerialization{
			JSON: &s3types.JSONOutput{
				RecordDelimiter: aws.String("\n"),
			},
		},
	})
	require.NoError(t, err)

	var recordData []byte
	stream := out.GetStream()
	defer stream.Close()

	for event := range stream.Events() {
		if rec, ok := event.(*s3types.SelectObjectContentEventStreamMemberRecords); ok {
			recordData = append(recordData, rec.Value.Payload...)
		}
	}
	require.NoError(t, stream.Err())

	result := string(recordData)
	assert.Contains(t, result, "alice")
	assert.Contains(t, result, "bob")
	assert.Contains(t, result, "charlie")
}
