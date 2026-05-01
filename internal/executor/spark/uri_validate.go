package spark

import (
	"fmt"
	"sort"
	"strings"

	"jaiscloud/internal/model"
)

// schemesToSkip are recognized non-storage schemes that are never checked
// against cloud allowlists. Everything else with "://" is a storage candidate.
var schemesToSkip = map[string]bool{
	"jdbc":    true, // JDBC sinks / Hive metastore
	"http":    true, // internal endpoints
	"https":   true,
	"kafka":   true,
	"redis":   true,
	"mongodb": true,
	"k8s":     true, // Spark --master k8s://...
}

// uriToken holds an arg token and the URI scheme found within it (may be "").
type uriToken struct {
	Arg    string
	Scheme string
}

// scanArgsForURIs walks spark-submit args and returns (arg, scheme) pairs for
// every token that contains "://". Comma-separated tokens (--jars / --files CSV
// form) are split and reported individually. Tokens without "://" produce an
// entry with Scheme=="".
func scanArgsForURIs(args []string) []uriToken {
	var out []uriToken
	for _, a := range args {
		// Split CSV tokens (--jars a.jar,b.jar or --conf spark.jars=a.jar,b.jar)
		parts := strings.Split(a, ",")
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if idx := strings.Index(p, "://"); idx >= 0 {
				scheme := p[:idx]
				// Strip leading "=" from --conf=value or --jars=value form.
				if eq := strings.LastIndex(scheme, "="); eq >= 0 {
					scheme = scheme[eq+1:]
				}
				out = append(out, uriToken{Arg: p, Scheme: strings.ToLower(scheme)})
			}
		}
	}
	return out
}

// validateAgainstAllowlist checks every storage URI in args against the
// per-cloud allowlist. Returns a user-readable error on first mismatch.
// Empty scheme (bare path, ivy coord) and schemes in schemesToSkip are allowed.
func validateAgainstAllowlist(cloud model.Cloud, allowed map[string]bool, args []string) error {
	for _, u := range scanArgsForURIs(args) {
		if u.Scheme == "" || schemesToSkip[u.Scheme] {
			continue
		}
		if !allowed[u.Scheme] {
			keys := sortedAllowedKeys(allowed)
			return fmt.Errorf(
				"storage URI scheme %q not supported on cloud %q (got %q); "+
					"allowed schemes for this cloud: [%s]",
				u.Scheme, string(cloud), u.Arg, strings.Join(keys, ", "))
		}
	}
	return nil
}

func sortedAllowedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		if k != "" { // skip the empty-scheme sentinel
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}
