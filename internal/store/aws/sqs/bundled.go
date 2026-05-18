package sqs

import (
	"context"
	"time"

	"jaiscloud/internal/store/bundle"
)

// BundledSQSStore wraps LocalBundle[MemoryMessageStore] to provide per-(account,region)
// isolation. It implements SQSMessageStore and is used in lite mode.
type BundledSQSStore struct {
	b *bundle.LocalBundle[MemoryMessageStore]
}

func NewBundledSQSStore() *BundledSQSStore {
	return &BundledSQSStore{
		b: bundle.NewLocal(func(_, _ string) *MemoryMessageStore {
			return NewMemoryMessageStore()
		}),
	}
}

func (s *BundledSQSStore) inner(account, region string) (*MemoryMessageStore, error) {
	return s.b.Get(account, region)
}

func (s *BundledSQSStore) Send(ctx context.Context, account, region string, msg SQSMessage) (dedupMessageID, sequenceNumber string, err error) {
	st, err := s.inner(account, region)
	if err != nil {
		return "", "", err
	}
	return st.Send(ctx, account, region, msg)
}

func (s *BundledSQSStore) Receive(ctx context.Context, account, region, queueURL string, maxMessages int, now time.Time) ([]SQSMessage, error) {
	st, err := s.inner(account, region)
	if err != nil {
		return nil, err
	}
	return st.Receive(ctx, account, region, queueURL, maxMessages, now)
}

func (s *BundledSQSStore) Delete(ctx context.Context, account, region, queueURL, receiptHandle string) error {
	st, err := s.inner(account, region)
	if err != nil {
		return err
	}
	return st.Delete(ctx, account, region, queueURL, receiptHandle)
}

func (s *BundledSQSStore) ChangeVisibility(ctx context.Context, account, region, queueURL, receiptHandle string, timeoutSec int, now time.Time) error {
	st, err := s.inner(account, region)
	if err != nil {
		return err
	}
	return st.ChangeVisibility(ctx, account, region, queueURL, receiptHandle, timeoutSec, now)
}

func (s *BundledSQSStore) Purge(ctx context.Context, account, region, queueURL string) error {
	st, err := s.inner(account, region)
	if err != nil {
		return err
	}
	return st.Purge(ctx, account, region, queueURL)
}

func (s *BundledSQSStore) GetApproximateCounts(ctx context.Context, account, region, queueURL string, now time.Time) (visible, notVisible, delayed int, err error) {
	st, e := s.inner(account, region)
	if e != nil {
		return 0, 0, 0, e
	}
	return st.GetApproximateCounts(ctx, account, region, queueURL, now)
}

func (s *BundledSQSStore) SetQueueRetention(ctx context.Context, account, region, queueURL string, retentionSecs int) error {
	st, err := s.inner(account, region)
	if err != nil {
		return err
	}
	return st.SetQueueRetention(ctx, account, region, queueURL, retentionSecs)
}

func (s *BundledSQSStore) Reset()                         { s.b.Reset() }
func (s *BundledSQSStore) ResetScope(account, region string) { s.b.ResetScope(account, region) }
func (s *BundledSQSStore) ResetAccount(account string)       { s.b.ResetAccount(account) }
