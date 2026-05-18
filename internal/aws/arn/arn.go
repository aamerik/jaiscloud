// Package arn provides ARN parsing and account/region resolution for
// cross-resource dispatch. All providers that dispatch to resources in other
// accounts must route through this package rather than ad-hoc string splitting.
package arn

import (
	"errors"
	"strings"
)

// ErrInvalidARN is returned when the input does not begin with "arn:".
var ErrInvalidARN = errors.New("invalid ARN")

// Parsed holds the components of an AWS ARN.
type Parsed struct {
	Partition    string // "aws", "aws-cn", "aws-us-gov"
	Service      string // "sqs", "dynamodb", "iam", ...
	Region       string // "" for global services (IAM, Route53, CloudFront)
	AccountID    string // "" for some services (e.g. S3 bucket ARNs)
	Resource     string // everything after the 5th colon
	ResourceType string // prefix before "/" or ":" inside Resource; best-effort
}

// Parse splits an ARN string into its components.
// It never panics; malformed ARNs return ErrInvalidARN.
// The LLD specifies six colon-delimited fields; SplitN(6) puts the entire
// resource remainder (including embedded colons) into parts[5].
func Parse(s string) (Parsed, error) {
	if !strings.HasPrefix(s, "arn:") {
		return Parsed{}, ErrInvalidARN
	}
	parts := strings.SplitN(s, ":", 6)
	if len(parts) < 6 {
		return Parsed{}, ErrInvalidARN
	}
	p := Parsed{
		Partition: parts[1],
		Service:   parts[2],
		Region:    parts[3],
		AccountID: parts[4],
		Resource:  parts[5],
	}
	// Best-effort resource-type split: "type/name" or "type:name"
	if i := strings.IndexAny(p.Resource, "/:"); i >= 0 {
		p.ResourceType = p.Resource[:i]
	}
	return p, nil
}

// MustParse is like Parse but panics on error. Only use in tests or constants.
func MustParse(s string) Parsed {
	p, err := Parse(s)
	if err != nil {
		panic("arn.MustParse: " + err.Error() + ": " + s)
	}
	return p
}

// ResolveAccountRegion returns the (account, region) pair to use for
// cross-resource dispatch from an ARN, falling back to the caller's own
// (account, region) when the ARN omits them (e.g. IAM, S3 bucket ARNs).
func ResolveAccountRegion(arnStr, callerAccount, callerRegion string) (account, region string) {
	p, err := Parse(arnStr)
	if err != nil {
		return callerAccount, callerRegion
	}
	account = p.AccountID
	if account == "" {
		account = callerAccount
	}
	region = p.Region
	if region == "" {
		region = callerRegion
	}
	return
}

// ServiceFromARN extracts the service field from an ARN, or returns "" on error.
func ServiceFromARN(arnStr string) string {
	p, err := Parse(arnStr)
	if err != nil {
		return ""
	}
	return p.Service
}
