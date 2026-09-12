package logging

import (
	"context"

	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"

	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/gcp/rescatalog"

	monitoredres "google.golang.org/genproto/googleapis/api/monitoredres"
)

// ListMonitoredResourceDescriptors returns the canonical catalog of well-known
// monitored resource descriptors shared with the Cloud Monitoring service
// (rescatalog). The descriptors carry type/display_name/description/labels;
// unlike Monitoring, the Logging surface does not set the descriptor resource
// name (real Cloud Logging leaves `name` unset).
//
// page_size/page_token are honored via the shared cursor pager. The
// ListMonitoredResourceDescriptorsRequest in this proto revision carries no
// `parent` or `filter` fields, so the catalog is global and unfiltered.
func (s *Service) ListMonitoredResourceDescriptors(_ context.Context, req *loggingpb.ListMonitoredResourceDescriptorsRequest) (*loggingpb.ListMonitoredResourceDescriptorsResponse, error) {
	entries := rescatalog.Catalog()
	all := make([]*monitoredres.MonitoredResourceDescriptor, 0, len(entries))
	for _, e := range entries {
		all = append(all, rescatalog.Proto(e, ""))
	}
	page, next := paging.Page(all, func(d *monitoredres.MonitoredResourceDescriptor) string { return d.GetType() },
		map[string]any{"pageSize": int(req.GetPageSize()), "pageToken": req.GetPageToken()})
	return &loggingpb.ListMonitoredResourceDescriptorsResponse{
		ResourceDescriptors: page,
		NextPageToken:       next,
	}, nil
}
