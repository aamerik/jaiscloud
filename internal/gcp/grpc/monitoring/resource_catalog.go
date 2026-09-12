package monitoring

import (
	monitoredrespb "google.golang.org/genproto/googleapis/api/monitoredres"

	"jaiscloud/internal/gcp/rescatalog"
	"jaiscloud/internal/gcp/resource"
)

// monitoredResourceDescriptorName is the Cloud Monitoring resource name for a
// canonical monitored resource type.
func monitoredResourceDescriptorName(project, typ string) string {
	return resource.ResourceID(project)("monitored-resource-descriptor", typ)
}

// monitoredResourceDescriptors returns the canonical catalog of well-known
// monitored resource descriptors (rescatalog), scoped to project.
func monitoredResourceDescriptors(project string) []*monitoredrespb.MonitoredResourceDescriptor {
	entries := rescatalog.Catalog()
	out := make([]*monitoredrespb.MonitoredResourceDescriptor, 0, len(entries))
	for _, e := range entries {
		out = append(out, rescatalog.Proto(e, monitoredResourceDescriptorName(project, e.Type())))
	}
	return out
}

// lookupMonitoredResourceDescriptor returns the canonical descriptor for typ,
// or ok=false when the type is not in the catalog.
func lookupMonitoredResourceDescriptor(project, typ string) (*monitoredrespb.MonitoredResourceDescriptor, bool) {
	e, ok := rescatalog.Lookup(typ)
	if !ok {
		return nil, false
	}
	return rescatalog.Proto(e, monitoredResourceDescriptorName(project, typ)), true
}
