// Package ecr provides the in-memory store for ECR repositories and images.
package ecr

import "time"

type Repository struct {
	RegistryID           string
	Name                 string
	ARN                  string
	URI                  string
	CreatedAt            time.Time
	ImageTagMutability   string // "MUTABLE" or "IMMUTABLE"
	ImageScanningConfig  ImageScanningConfig
	EncryptionConfig     EncryptionConfig
	Images               map[string]*Image // digest → image
	LifecyclePolicy      string
	RepositoryPolicy     string
	Tags                 map[string]string
}

type ImageScanningConfig struct {
	ScanOnPush bool
}

type EncryptionConfig struct {
	EncryptionType string // "AES256" or "KMS"
	KMSKey         string
}

type Image struct {
	Digest             string
	Manifest           string
	ManifestMediaType  string
	Tags               []string
	PushedAt           time.Time
	Size               int64
	ScanFindings       *ImageScanFindings
	ArtifactMediaType  string
}

type ImageIdentifier struct {
	ImageDigest string
	ImageTag    string
}

type ImageScanFindings struct {
	ImageScanFindingsSummary ImageScanFindingsSummary
	Findings                 []ImageScanFinding
	ScanCompletedAt          time.Time
	VulnerabilitySourceUpdatedAt time.Time
}

type ImageScanFindingsSummary struct {
	ImageScanCompletedAt         time.Time
	VulnerabilitySourceUpdatedAt time.Time
	FindingSeverityCounts        map[string]int
}

type ImageScanFinding struct {
	Name        string
	Description string
	URI         string
	Severity    string
	Attributes  map[string]string
}

type FailedImage struct {
	ImageID       ImageIdentifier
	FailureCode   string
	FailureReason string
}

type PullThroughCacheRule struct {
	EcrRepositoryPrefix  string
	UpstreamRegistryURL  string
	CreatedAt            time.Time
}
