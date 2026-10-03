package managedkafkaui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the Managed Kafka UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/clusters", h.ListClusters)
	r.Get("/clusters/{location}/{cluster}", h.GetCluster)
	r.Get("/clusters/{location}/{cluster}/topics", h.ListClusterTopics)
	r.Get("/clusters/{location}/{cluster}/topics/{topic}", h.GetTopic)
	r.Get("/clusters/{location}/{cluster}/acls", h.ListAcls)
	r.Get("/clusters/{location}/{cluster}/consumer-groups", h.ListConsumerGroups)

	r.Get("/topics", h.ListTopics)

	return r
}
