// Package logs implements the CloudWatch Logs provider for JaisCloud.
// It handles log group/stream lifecycle, event ingestion and retrieval.
package logs

import (
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// Provider handles CloudWatch Logs API operations.
type Provider struct {
	store *memStore
}

// New constructs a Provider with a fresh in-memory store.
func New() *Provider {
	return &Provider{store: newMemStore()}
}

// Routes returns all CloudWatch Logs handler registrations.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"CloudWatchLogs.CreateLogGroup":              p.CreateLogGroup,
		"CloudWatchLogs.DeleteLogGroup":              p.DeleteLogGroup,
		"CloudWatchLogs.DescribeLogGroups":           p.DescribeLogGroups,
		"CloudWatchLogs.CreateLogStream":             p.CreateLogStream,
		"CloudWatchLogs.DeleteLogStream":             p.DeleteLogStream,
		"CloudWatchLogs.DescribeLogStreams":          p.DescribeLogStreams,
		"CloudWatchLogs.PutLogEvents":                p.PutLogEvents,
		"CloudWatchLogs.GetLogEvents":                p.GetLogEvents,
		"CloudWatchLogs.FilterLogEvents":             p.FilterLogEvents,
		"CloudWatchLogs.TagLogGroup":                 p.TagLogGroup,
		"CloudWatchLogs.UntagLogGroup":               p.UntagLogGroup,
		"CloudWatchLogs.ListTagsLogGroup":            p.ListTagsLogGroup,
		"CloudWatchLogs.PutRetentionPolicy":          p.PutRetentionPolicy,
		"CloudWatchLogs.DeleteRetentionPolicy":       p.DeleteRetentionPolicy,
		// ARN-based tagging (4.11)
		"CloudWatchLogs.TagResource":                 p.TagResource,
		"CloudWatchLogs.UntagResource":               p.UntagResource,
		"CloudWatchLogs.ListTagsForResource":         p.ListTagsForResource,
		// Subscription filters (4.12)
		"CloudWatchLogs.PutSubscriptionFilter":       p.PutSubscriptionFilter,
		"CloudWatchLogs.DescribeSubscriptionFilters": p.DescribeSubscriptionFilters,
		"CloudWatchLogs.DeleteSubscriptionFilter":    p.DeleteSubscriptionFilter,
	}
}

// Reset wipes all state. Implements admin.Resetter.
func (p *Provider) Reset() { p.store.Reset() }

// ─── helpers ──────────────────────────────────────────────────────────────────

// paramStr extracts a string value from params; returns "" when absent or wrong type.
func paramStr(params map[string]any, key string) string {
	v, _ := params[key].(string)
	return v
}

// paramInt extracts an integer from params (JSON numbers arrive as float64).
// Returns def when absent or not a number.
func paramInt(params map[string]any, key string, def int) int {
	switch v := params[key].(type) {
	case float64:
		return int(v)
	case int:
		return v
	case int64:
		return int(v)
	}
	return def
}

// logsErr creates a ProviderError with the given code, message and HTTP status.
func logsErr(code, msg string, status int) error {
	return model.NewProviderError(code, msg, status)
}

// verifyGroupExists returns the group or a ResourceNotFoundException.
// Caller must hold at least a read lock.
func (p *Provider) verifyGroupExists(name string) (*LogGroup, error) {
	g, ok := p.store.groups[name]
	if !ok {
		return nil, logsErr("ResourceNotFoundException", "The specified log group does not exist: "+name, 400)
	}
	return g, nil
}

