package identity

import (
	"os"
	"regexp"
	"strings"
)

// validAWSRegionRE matches the structural pattern for AWS region names.
var validAWSRegionRE = regexp.MustCompile(`^[a-z]{2}-[a-z]+-\d$`)

// awsRegions is the recognised set of standard AWS regions.
// Requests arriving in an unrecognised region are coerced to DefaultRegion
// unless JAISCLOUD_ALLOW_NONSTANDARD_REGIONS=true (§5.4.1).
var awsRegions = map[string]struct{}{
	"us-east-1": {}, "us-east-2": {}, "us-west-1": {}, "us-west-2": {},
	"eu-west-1": {}, "eu-west-2": {}, "eu-west-3": {},
	"eu-central-1": {}, "eu-central-2": {},
	"eu-north-1": {}, "eu-south-1": {}, "eu-south-2": {},
	"ap-south-1": {}, "ap-south-2": {},
	"ap-southeast-1": {}, "ap-southeast-2": {}, "ap-southeast-3": {}, "ap-southeast-4": {},
	"ap-northeast-1": {}, "ap-northeast-2": {}, "ap-northeast-3": {},
	"ap-east-1":    {},
	"ca-central-1": {}, "ca-west-1": {},
	"sa-east-1":  {},
	"me-south-1": {}, "me-central-1": {},
	"af-south-1":    {},
	"il-central-1":  {},
	"us-gov-east-1": {}, "us-gov-west-1": {},
}

// NormaliseRegion returns the canonical region to use for a request.
//
//   - If parsed is a known AWS region, return it unchanged.
//   - If JAISCLOUD_ALLOW_NONSTANDARD_REGIONS=true and parsed matches the
//     structural regex, return it unchanged.
//   - If parsed is empty, return fallback (cfg.Region) or DefaultRegion.
//   - Otherwise coerce to DefaultRegion ("us-east-1").
//
// This mirrors LocalStack's RegionRewriterStrategy.apply behaviour (§5.4.1).
func NormaliseRegion(parsed, fallback string) string {
	if parsed == "" {
		if fallback != "" {
			return fallback
		}
		return DefaultRegion
	}
	if _, ok := awsRegions[parsed]; ok {
		return parsed
	}
	if allowNonstandardRegions() && validAWSRegionRE.MatchString(parsed) {
		return parsed
	}
	return DefaultRegion
}

func allowNonstandardRegions() bool {
	return strings.ToLower(os.Getenv("JAISCLOUD_ALLOW_NONSTANDARD_REGIONS")) == "true"
}
