// Package wire holds the reserved keys shared between GCP codecs and providers.
//
// GCP media APIs need to pass raw bytes between the decode path (adapter) and
// the provider, and resumable uploads need to surface a Location header from the
// provider back to the codec. These reserved keys are that contract.
package wire

const (
	// MediaKey carries raw object bytes: []byte in NormalizedRequest.Params
	// (codec → provider) or in ProviderResponse.Data (provider → codec).
	MediaKey = "media"
	// ContentTypeKey carries the object's Content-Type as a string.
	ContentTypeKey = "contentType"
	// LocationKey carries a resumable-upload Location header value as a string
	// in ProviderResponse.Data (provider → codec). Namespaced with a prefix that
	// cannot collide with a GCS resource field (which uses "location" for buckets).
	LocationKey = "jaiscloud:resumeLocation"
	// RangeKey carries the "Range: bytes=0-N" header value for 308 Resume
	// Incomplete responses (provider → codec).
	RangeKey = "jaiscloud:range"
)
