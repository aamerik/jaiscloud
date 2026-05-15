package parameter

import (
	"strconv"
	"strings"
)

// Normalize ensures a parameter name starts with "/".
func Normalize(name string) string {
	if name == "" || name[0] == '/' {
		return name
	}
	return "/" + name
}

// ParseSelector splits "name:version_or_label" into base name, version (or 0), and label.
// It finds the last ":" that is followed by either digits or label characters.
func ParseSelector(name string) (base string, version int64, label string) {
	idx := strings.LastIndex(name, ":")
	if idx < 0 {
		return name, 0, ""
	}
	suffix := name[idx+1:]
	base = name[:idx]
	if v, err := strconv.ParseInt(suffix, 10, 64); err == nil {
		return base, v, ""
	}
	return base, 0, suffix
}
