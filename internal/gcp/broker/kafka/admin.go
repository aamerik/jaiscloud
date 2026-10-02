package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// topicAdmin is the subset of Kafka-wire admin operations the broker performs
// on behalf of the Managed Kafka control plane. Isolating it behind an
// interface keeps the (large) Kafka client dependency contained and lets tests
// assert provisioning without a running broker. A future swap of franz-go for
// another client is a change here, not in the core.
type topicAdmin interface {
	// EnsureTopic creates the topic if absent. It is idempotent: an existing
	// topic with the same name is not an error.
	EnsureTopic(ctx context.Context, topic string, partitions int32, replicationFactor int16) error
	// AddPartitions raises the topic's partition count to totalPartitions.
	AddPartitions(ctx context.Context, topic string, totalPartitions int32) error
	// DeleteTopic removes the topic. It is idempotent: an absent topic is not
	// an error.
	DeleteTopic(ctx context.Context, topic string) error
	// Close releases the underlying client.
	Close() error
}

// topicAdminFactory builds a topicAdmin for one broker endpoint.
type topicAdminFactory func(endpoint string) (topicAdmin, error)

// adminAPI is the slice of *kadm.Client the franzAdmin uses. It is an interface
// so unit tests can exercise the per-topic response-error mapping (which the
// top-level error hides) without a running broker.
type adminAPI interface {
	CreateTopic(ctx context.Context, partitions int32, replicationFactor int16, configs map[string]*string, topic string) (kadm.CreateTopicResponse, error)
	UpdatePartitions(ctx context.Context, set int, topics ...string) (kadm.CreatePartitionsResponses, error)
	DeleteTopic(ctx context.Context, topic string) (kadm.DeleteTopicResponse, error)
	Close()
}

// adminPool caches one admin client per broker endpoint so repeated topic
// mutations reuse a single Kafka connection instead of dialing per call. It is
// safe for concurrent use.
type adminPool struct {
	newAdmin topicAdminFactory
	mu       sync.Mutex
	admins   map[string]topicAdmin
}

// newAdminPool returns a pool. A nil factory selects the franz-go client (tests
// inject a fake).
func newAdminPool(factory topicAdminFactory) *adminPool {
	if factory == nil {
		factory = newFranzAdmin
	}
	return &adminPool{newAdmin: factory, admins: make(map[string]topicAdmin)}
}

// get returns the cached admin for endpoint, creating one on first use.
func (p *adminPool) get(endpoint string) (topicAdmin, error) {
	p.mu.Lock()
	if a, ok := p.admins[endpoint]; ok {
		p.mu.Unlock()
		return a, nil
	}
	p.mu.Unlock()

	// Build outside the lock so a factory that dials cannot serialize every
	// caller; re-check for a racing winner before inserting.
	a, err := p.newAdmin(endpoint)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if existing, ok := p.admins[endpoint]; ok {
		_ = a.Close()
		return existing, nil
	}
	p.admins[endpoint] = a
	return a, nil
}

// close drops and closes the admin for endpoint, if any.
func (p *adminPool) close(endpoint string) {
	p.mu.Lock()
	a := p.admins[endpoint]
	delete(p.admins, endpoint)
	p.mu.Unlock()
	if a != nil {
		_ = a.Close()
	}
}

// closeAll drops and closes every pooled admin.
func (p *adminPool) closeAll() {
	p.mu.Lock()
	admins := p.admins
	p.admins = make(map[string]topicAdmin)
	p.mu.Unlock()
	for _, a := range admins {
		_ = a.Close()
	}
}

// ensureTopic provisions endpoint's topic through the pool. An empty endpoint
// (no live broker) is a metadata-only no-op.
func ensureTopic(ctx context.Context, pool *adminPool, endpoint, topic string, partitions int) error {
	if endpoint == "" {
		return nil
	}
	if partitions < 1 {
		partitions = 1
	}
	a, err := pool.get(endpoint)
	if err != nil {
		return err
	}
	return a.EnsureTopic(ctx, topic, int32(partitions), 1)
}

// addPartitions raises endpoint's topic partition count through the pool. An
// empty endpoint (no live broker) is a metadata-only no-op.
func addPartitions(ctx context.Context, pool *adminPool, endpoint, topic string, totalPartitions int) error {
	if endpoint == "" {
		return nil
	}
	a, err := pool.get(endpoint)
	if err != nil {
		return err
	}
	return a.AddPartitions(ctx, topic, int32(totalPartitions))
}

// deleteBrokerTopic removes endpoint's topic through the pool. An empty endpoint
// (no live broker) is a metadata-only no-op.
func deleteBrokerTopic(ctx context.Context, pool *adminPool, endpoint, topic string) error {
	if endpoint == "" {
		return nil
	}
	a, err := pool.get(endpoint)
	if err != nil {
		return err
	}
	return a.DeleteTopic(ctx, topic)
}

// franzAdmin is the franz-go/kadm-backed topicAdmin.
type franzAdmin struct {
	client adminAPI
}

// newFranzAdmin dials endpoint's Kafka listener and returns an admin client.
func newFranzAdmin(endpoint string) (topicAdmin, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(endpoint),
		kgo.ClientID("jaiscloud-managedkafka"),
	)
	if err != nil {
		return nil, fmt.Errorf("managedkafka broker: kafka client: %w", err)
	}
	return &franzAdmin{client: kadm.NewClient(cl)}, nil
}

func (a *franzAdmin) EnsureTopic(ctx context.Context, topic string, partitions int32, replicationFactor int16) error {
	if partitions < 1 {
		partitions = 1
	}
	if replicationFactor < 1 {
		replicationFactor = 1
	}
	_, err := a.client.CreateTopic(ctx, partitions, replicationFactor, nil, topic)
	// Creating a topic that already exists is idempotent: the caller asked for
	// the topic to exist and it does.
	if errors.Is(err, kerr.TopicAlreadyExists) {
		return nil
	}
	return err
}

func (a *franzAdmin) AddPartitions(ctx context.Context, topic string, totalPartitions int32) error {
	rs, err := a.client.UpdatePartitions(ctx, int(totalPartitions), topic)
	// err is only a request-level (network) failure; a broker rejection such as
	// InvalidPartitions or UnknownTopicOrPartition is carried per topic in the
	// response, so it must be surfaced explicitly.
	if err != nil {
		return err
	}
	return rs.Error()
}

func (a *franzAdmin) DeleteTopic(ctx context.Context, topic string) error {
	_, err := a.client.DeleteTopic(ctx, topic)
	// Deleting an absent topic is idempotent: the postcondition (no topic)
	// already holds.
	if errors.Is(err, kerr.UnknownTopicOrPartition) {
		return nil
	}
	return err
}

func (a *franzAdmin) Close() error {
	a.client.Close()
	return nil
}
