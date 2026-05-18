package kinesis

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"
)

// ─── internal stream state ────────────────────────────────────────────────────

type shardState struct {
	Shard   Shard
	Records []Record
	NextSeq uint64
}

type streamState struct {
	Stream    Stream
	Shards    []*shardState
	Consumers map[string]*Consumer // key = consumer ARN
}

// ─── MemoryKinesisStore ───────────────────────────────────────────────────────

// MemoryKinesisStore is a thread-safe in-memory Kinesis store.
type MemoryKinesisStore struct {
	mu        sync.RWMutex
	streams   map[string]*streamState   // key = stream ARN
	nameScope map[string]string         // key = "account:region:name" → ARN
	iterators map[string]*IteratorEntry // key = opaque UUID token
}

// NewMemoryKinesisStore returns an initialised store.
func NewMemoryKinesisStore() *MemoryKinesisStore {
	return &MemoryKinesisStore{
		streams:   make(map[string]*streamState),
		nameScope: make(map[string]string),
		iterators: make(map[string]*IteratorEntry),
	}
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func uuid() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// seqNum formats uint64 as 56-digit zero-padded decimal.
func seqNum(n uint64) string {
	return fmt.Sprintf("%056d", n)
}

// maxHashKey = 2^128 - 1
var maxHashKey = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 128), big.NewInt(1))

// calculateHashRanges divides 2^128 evenly across n shards.
func calculateHashRanges(n int) []HashKeyRange {
	step := new(big.Int).Div(maxHashKey, big.NewInt(int64(n)))
	ranges := make([]HashKeyRange, n)
	for i := 0; i < n; i++ {
		start := new(big.Int).Mul(step, big.NewInt(int64(i)))
		end := new(big.Int).Sub(new(big.Int).Mul(step, big.NewInt(int64(i+1))), big.NewInt(1))
		if i == n-1 {
			end = new(big.Int).Set(maxHashKey)
		}
		ranges[i] = HashKeyRange{
			StartingHashKey: start.String(),
			EndingHashKey:   end.String(),
		}
	}
	return ranges
}

// routeToShard selects a shard by MD5(partitionKey) or explicitHashKey.
func routeToShard(shards []*shardState, partitionKey, explicitHashKey string) *shardState {
	var hashKey *big.Int
	if explicitHashKey != "" {
		hashKey, _ = new(big.Int).SetString(explicitHashKey, 10)
		if hashKey == nil {
			return nil
		}
	} else {
		h := md5.Sum([]byte(partitionKey))
		hashKey = new(big.Int).SetBytes(h[:])
	}
	for _, s := range shards {
		if !s.Shard.IsOpen {
			continue
		}
		start, _ := new(big.Int).SetString(s.Shard.HashKeyRange.StartingHashKey, 10)
		end, _ := new(big.Int).SetString(s.Shard.HashKeyRange.EndingHashKey, 10)
		if hashKey.Cmp(start) >= 0 && hashKey.Cmp(end) <= 0 {
			return s
		}
	}
	return nil
}

// scopeKey returns the nameScope key for (account, region, name).
func scopeKey(account, region, name string) string {
	return account + ":" + region + ":" + name
}

// arnScope parses account and region out of a Kinesis ARN.
// ARN format: arn:aws:kinesis:region:account:stream/name
func arnScope(arn string) (account, region string) {
	parts := strings.SplitN(arn, ":", 7)
	if len(parts) >= 6 {
		return parts[4], parts[3]
	}
	return "", ""
}

// ─── KinesisStore implementation ──────────────────────────────────────────────

// CreateStream creates a stream with the given shard count.
func (s *MemoryKinesisStore) CreateStream(stream Stream, shardCount int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	account, region := arnScope(stream.ARN)
	sk := scopeKey(account, region, stream.Name)
	if _, ok := s.nameScope[sk]; ok {
		return &KinesisError{Code: "ResourceInUseException", Message: "Stream " + stream.Name + " already exists", Status: 400}
	}
	ranges := calculateHashRanges(shardCount)
	shards := make([]*shardState, shardCount)
	for i, r := range ranges {
		id := fmt.Sprintf("shardId-%012d", i)
		shards[i] = &shardState{
			Shard: Shard{
				ShardID:      id,
				HashKeyRange: r,
				SequenceNumberRange: SequenceNumberRange{
					StartingSequenceNumber: seqNum(1),
				},
				IsOpen: true,
			},
			NextSeq: 1,
		}
	}
	if stream.RetentionPeriodHours == 0 {
		stream.RetentionPeriodHours = 24
	}
	if stream.EncryptionType == "" {
		stream.EncryptionType = "NONE"
	}
	if stream.Tags == nil {
		stream.Tags = make(map[string]string)
	}
	stream.Status = StreamStatusActive
	st := &streamState{Stream: stream, Shards: shards, Consumers: make(map[string]*Consumer)}
	s.streams[stream.ARN] = st
	s.nameScope[sk] = stream.ARN
	return nil
}

// GetStreamByARN resolves a stream by its ARN.
func (s *MemoryKinesisStore) GetStreamByARN(arn string) (*Stream, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st, ok := s.streams[arn]
	if !ok {
		return nil, &KinesisError{Code: "ResourceNotFoundException", Message: "Stream not found for ARN " + arn, Status: 400}
	}
	cp := st.Stream
	return &cp, nil
}

// GetStreamInScope returns stream metadata scoped by account+region+name.
func (s *MemoryKinesisStore) GetStreamInScope(account, region, name string) (*Stream, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	arn, ok := s.nameScope[scopeKey(account, region, name)]
	if !ok {
		return nil, &KinesisError{Code: "ResourceNotFoundException", Message: "Stream " + name + " not found", Status: 400}
	}
	st := s.streams[arn]
	cp := st.Stream
	return &cp, nil
}

// DeleteStreamByARN deletes a stream by ARN.
func (s *MemoryKinesisStore) DeleteStreamByARN(arn string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.streams[arn]
	if !ok {
		return &KinesisError{Code: "ResourceNotFoundException", Message: "Stream not found for ARN " + arn, Status: 400}
	}
	account, region := arnScope(arn)
	delete(s.nameScope, scopeKey(account, region, st.Stream.Name))
	delete(s.streams, arn)
	return nil
}

// DeleteStreamInScope deletes a stream by account+region+name.
func (s *MemoryKinesisStore) DeleteStreamInScope(account, region, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sk := scopeKey(account, region, name)
	arn, ok := s.nameScope[sk]
	if !ok {
		return &KinesisError{Code: "ResourceNotFoundException", Message: "Stream " + name + " not found", Status: 400}
	}
	delete(s.nameScope, sk)
	delete(s.streams, arn)
	return nil
}

// ListStreamsInScope returns all streams for the given account+region.
func (s *MemoryKinesisStore) ListStreamsInScope(account, region string) []Stream {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := account + ":" + region + ":"
	var out []Stream
	for sk, arn := range s.nameScope {
		if strings.HasPrefix(sk, prefix) {
			out = append(out, s.streams[arn].Stream)
		}
	}
	return out
}

// ListShardsInScope returns shards for a stream by account+region+name.
func (s *MemoryKinesisStore) ListShardsInScope(account, region, name string) ([]Shard, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	arn, ok := s.nameScope[scopeKey(account, region, name)]
	if !ok {
		return nil, &KinesisError{Code: "ResourceNotFoundException", Message: "Stream " + name + " not found", Status: 400}
	}
	st := s.streams[arn]
	out := make([]Shard, len(st.Shards))
	for i, sh := range st.Shards {
		out[i] = sh.Shard
	}
	return out, nil
}

// PutRecordInScope appends a record to the correct shard, scoped by account+region+name.
func (s *MemoryKinesisStore) PutRecordInScope(account, region, name string, data []byte, partitionKey, explicitHashKey string) (shardID, sequenceNumber string, err error) {
	if len(data) > 1024*1024 {
		return "", "", &KinesisError{Code: "InvalidArgumentException", Message: "Data must be less than or equal to 1 MB in size", Status: 400}
	}
	if len(partitionKey) > 256 {
		return "", "", &KinesisError{Code: "InvalidArgumentException", Message: "PartitionKey must not exceed 256 characters", Status: 400}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	arn, ok := s.nameScope[scopeKey(account, region, name)]
	if !ok {
		return "", "", &KinesisError{Code: "ResourceNotFoundException", Message: "Stream " + name + " not found", Status: 400}
	}
	st := s.streams[arn]
	shard := routeToShard(st.Shards, partitionKey, explicitHashKey)
	if shard == nil {
		return "", "", &KinesisError{Code: "InvalidArgumentException", Message: "Could not route to shard", Status: 400}
	}
	shard.NextSeq++
	seq := seqNum(shard.NextSeq)
	rec := Record{
		SequenceNumber:         seq,
		ApproximateArrivalTime: time.Now().UTC(),
		Data:                   data,
		PartitionKey:           partitionKey,
		EncryptionType:         st.Stream.EncryptionType,
	}
	shard.Records = append(shard.Records, rec)
	return shard.Shard.ShardID, seq, nil
}

// CreateIteratorInScope creates an opaque iterator token for a stream identified by account+region+name.
func (s *MemoryKinesisStore) CreateIteratorInScope(account, region, streamName, shardID string, iterType ShardIteratorType, seqNumParam string, timestamp *time.Time) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	arn, ok := s.nameScope[scopeKey(account, region, streamName)]
	if !ok {
		return "", &KinesisError{Code: "ResourceNotFoundException", Message: "Stream " + streamName + " not found", Status: 400}
	}
	st := s.streams[arn]
	var ss *shardState
	for _, sh := range st.Shards {
		if sh.Shard.ShardID == shardID {
			ss = sh
			break
		}
	}
	if ss == nil {
		return "", &KinesisError{Code: "ResourceNotFoundException", Message: "Shard " + shardID + " not found", Status: 400}
	}

	var pos int
	switch iterType {
	case IterTrimHorizon:
		pos = 0
	case IterLatest:
		pos = len(ss.Records)
	case IterAtSequenceNumber:
		if seqNumParam == "" {
			return "", &KinesisError{Code: "InvalidArgumentException", Message: "SequenceNumber required", Status: 400}
		}
		pos = findSeqPosition(ss.Records, seqNumParam, false)
	case IterAfterSequenceNumber:
		if seqNumParam == "" {
			return "", &KinesisError{Code: "InvalidArgumentException", Message: "SequenceNumber required", Status: 400}
		}
		pos = findSeqPosition(ss.Records, seqNumParam, true)
	case IterAtTimestamp:
		if timestamp == nil {
			pos = 0
		} else {
			pos = findTimestampPosition(ss.Records, *timestamp)
		}
	default:
		return "", &KinesisError{Code: "InvalidArgumentException", Message: "Unknown iterator type: " + string(iterType), Status: 400}
	}

	id := uuid()
	s.iterators[id] = &IteratorEntry{
		StreamARN: arn,
		ShardID:   shardID,
		Position:  pos,
		CreatedAt: time.Now(),
	}
	return id, nil
}

// SplitShardInScope closes a parent shard and creates two child shards, scoped by account+region+name.
func (s *MemoryKinesisStore) SplitShardInScope(account, region, name, shardID, newStartingHashKey string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	arn, ok := s.nameScope[scopeKey(account, region, name)]
	if !ok {
		return &KinesisError{Code: "ResourceNotFoundException", Message: "Stream " + name + " not found", Status: 400}
	}
	st := s.streams[arn]
	var parent *shardState
	for _, sh := range st.Shards {
		if sh.Shard.ShardID == shardID {
			parent = sh
			break
		}
	}
	if parent == nil {
		return &KinesisError{Code: "ResourceNotFoundException", Message: "Shard " + shardID + " not found", Status: 400}
	}
	if !parent.Shard.IsOpen {
		return &KinesisError{Code: "ResourceInUseException", Message: "Shard " + shardID + " is already closed", Status: 400}
	}

	splitKey, ok2 := new(big.Int).SetString(newStartingHashKey, 10)
	if !ok2 {
		return &KinesisError{Code: "InvalidArgumentException", Message: "Invalid NewStartingHashKey", Status: 400}
	}
	startKey, _ := new(big.Int).SetString(parent.Shard.HashKeyRange.StartingHashKey, 10)
	endKey, _ := new(big.Int).SetString(parent.Shard.HashKeyRange.EndingHashKey, 10)
	if splitKey.Cmp(startKey) <= 0 || splitKey.Cmp(endKey) >= 0 {
		return &KinesisError{Code: "InvalidArgumentException", Message: "NewStartingHashKey must be strictly within parent shard range", Status: 400}
	}

	parent.Shard.IsOpen = false
	endSeq := seqNum(parent.NextSeq)
	parent.Shard.SequenceNumberRange.EndingSequenceNumber = endSeq

	nextIdx := len(st.Shards)
	child1ID := fmt.Sprintf("shardId-%012d", nextIdx)
	child2ID := fmt.Sprintf("shardId-%012d", nextIdx+1)
	splitMinus1 := new(big.Int).Sub(splitKey, big.NewInt(1))
	c1 := &shardState{
		Shard: Shard{
			ShardID:       child1ID,
			ParentShardID: shardID,
			HashKeyRange: HashKeyRange{
				StartingHashKey: parent.Shard.HashKeyRange.StartingHashKey,
				EndingHashKey:   splitMinus1.String(),
			},
			SequenceNumberRange: SequenceNumberRange{StartingSequenceNumber: seqNum(1)},
			IsOpen:              true,
		},
		NextSeq: 1,
	}
	c2 := &shardState{
		Shard: Shard{
			ShardID:       child2ID,
			ParentShardID: shardID,
			HashKeyRange: HashKeyRange{
				StartingHashKey: splitKey.String(),
				EndingHashKey:   parent.Shard.HashKeyRange.EndingHashKey,
			},
			SequenceNumberRange: SequenceNumberRange{StartingSequenceNumber: seqNum(1)},
			IsOpen:              true,
		},
		NextSeq: 1,
	}
	st.Shards = append(st.Shards, c1, c2)
	return nil
}

// MergeShardsInScope closes two adjacent shards and creates one combined child, scoped by account+region+name.
func (s *MemoryKinesisStore) MergeShardsInScope(account, region, name, shardToMerge, adjacentShard string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	arn, ok := s.nameScope[scopeKey(account, region, name)]
	if !ok {
		return &KinesisError{Code: "ResourceNotFoundException", Message: "Stream " + name + " not found", Status: 400}
	}
	st := s.streams[arn]
	var p1, p2 *shardState
	for _, sh := range st.Shards {
		if sh.Shard.ShardID == shardToMerge {
			p1 = sh
		}
		if sh.Shard.ShardID == adjacentShard {
			p2 = sh
		}
	}
	if p1 == nil || p2 == nil {
		return &KinesisError{Code: "ResourceNotFoundException", Message: "One or both shards not found", Status: 400}
	}
	if !p1.Shard.IsOpen || !p2.Shard.IsOpen {
		return &KinesisError{Code: "ResourceInUseException", Message: "Both shards must be open to merge", Status: 400}
	}
	p1End, _ := new(big.Int).SetString(p1.Shard.HashKeyRange.EndingHashKey, 10)
	p2Start, _ := new(big.Int).SetString(p2.Shard.HashKeyRange.StartingHashKey, 10)
	p2End, _ := new(big.Int).SetString(p2.Shard.HashKeyRange.EndingHashKey, 10)
	p1Start, _ := new(big.Int).SetString(p1.Shard.HashKeyRange.StartingHashKey, 10)
	p1EndPlus1 := new(big.Int).Add(p1End, big.NewInt(1))
	p2EndPlus1 := new(big.Int).Add(p2End, big.NewInt(1))

	var lower, upper *shardState
	if p1EndPlus1.Cmp(p2Start) == 0 {
		lower, upper = p1, p2
	} else if p2EndPlus1.Cmp(p1Start) == 0 {
		lower, upper = p2, p1
	} else {
		return &KinesisError{Code: "InvalidArgumentException", Message: "Shards are not adjacent", Status: 400}
	}

	endSeq := seqNum(max64(lower.NextSeq, upper.NextSeq))
	lower.Shard.IsOpen = false
	lower.Shard.SequenceNumberRange.EndingSequenceNumber = endSeq
	upper.Shard.IsOpen = false
	upper.Shard.SequenceNumberRange.EndingSequenceNumber = endSeq

	nextIdx := len(st.Shards)
	merged := &shardState{
		Shard: Shard{
			ShardID:               fmt.Sprintf("shardId-%012d", nextIdx),
			ParentShardID:         lower.Shard.ShardID,
			AdjacentParentShardID: upper.Shard.ShardID,
			HashKeyRange: HashKeyRange{
				StartingHashKey: lower.Shard.HashKeyRange.StartingHashKey,
				EndingHashKey:   upper.Shard.HashKeyRange.EndingHashKey,
			},
			SequenceNumberRange: SequenceNumberRange{StartingSequenceNumber: seqNum(1)},
			IsOpen:              true,
		},
		NextSeq: 1,
	}
	st.Shards = append(st.Shards, merged)
	return nil
}

// UpdateStreamModeInScope changes the stream mode, scoped by account+region+name.
func (s *MemoryKinesisStore) UpdateStreamModeInScope(account, region, name string, mode StreamMode) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	arn, ok := s.nameScope[scopeKey(account, region, name)]
	if !ok {
		return &KinesisError{Code: "ResourceNotFoundException", Message: "Stream " + name + " not found", Status: 400}
	}
	s.streams[arn].Stream.Mode = mode
	return nil
}

// SetRetentionPeriodInScope updates retention, scoped by account+region+name.
func (s *MemoryKinesisStore) SetRetentionPeriodInScope(account, region, name string, hours int) error {
	if hours < 24 || hours > 8760 {
		return &KinesisError{Code: "InvalidArgumentException", Message: "RetentionPeriodHours must be between 24 and 8760", Status: 400}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	arn, ok := s.nameScope[scopeKey(account, region, name)]
	if !ok {
		return &KinesisError{Code: "ResourceNotFoundException", Message: "Stream " + name + " not found", Status: 400}
	}
	s.streams[arn].Stream.RetentionPeriodHours = hours
	return nil
}

// AddTagsInScope merges tags onto the stream, scoped by account+region+name.
func (s *MemoryKinesisStore) AddTagsInScope(account, region, name string, tags map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	arn, ok := s.nameScope[scopeKey(account, region, name)]
	if !ok {
		return &KinesisError{Code: "ResourceNotFoundException", Message: "Stream " + name + " not found", Status: 400}
	}
	st := s.streams[arn]
	if st.Stream.Tags == nil {
		st.Stream.Tags = make(map[string]string)
	}
	for k, v := range tags {
		st.Stream.Tags[k] = v
	}
	if len(st.Stream.Tags) > 50 {
		return &KinesisError{Code: "InvalidArgumentException", Message: "A stream can have at most 50 tags", Status: 400}
	}
	return nil
}

// RemoveTagsInScope removes tag keys, scoped by account+region+name.
func (s *MemoryKinesisStore) RemoveTagsInScope(account, region, name string, keys []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	arn, ok := s.nameScope[scopeKey(account, region, name)]
	if !ok {
		return &KinesisError{Code: "ResourceNotFoundException", Message: "Stream " + name + " not found", Status: 400}
	}
	st := s.streams[arn]
	for _, k := range keys {
		delete(st.Stream.Tags, k)
	}
	return nil
}

// GetTagsInScope returns tags, scoped by account+region+name.
func (s *MemoryKinesisStore) GetTagsInScope(account, region, name string) (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	arn, ok := s.nameScope[scopeKey(account, region, name)]
	if !ok {
		return nil, &KinesisError{Code: "ResourceNotFoundException", Message: "Stream " + name + " not found", Status: 400}
	}
	st := s.streams[arn]
	out := make(map[string]string, len(st.Stream.Tags))
	for k, v := range st.Stream.Tags {
		out[k] = v
	}
	return out, nil
}

// findSeqPosition finds the index of the record with the given sequence number.
// If after=true, returns the index after the matching record.
func findSeqPosition(records []Record, seq string, after bool) int {
	for i, r := range records {
		if r.SequenceNumber == seq {
			if after {
				return i + 1
			}
			return i
		}
	}
	return len(records)
}

func findTimestampPosition(records []Record, ts time.Time) int {
	for i, r := range records {
		if !r.ApproximateArrivalTime.Before(ts) {
			return i
		}
	}
	return len(records)
}

// GetRecords reads records from the shard identified by the iterator token.
func (s *MemoryKinesisStore) GetRecords(iteratorID string, limit int) ([]Record, string, int64, error) {
	if limit <= 0 {
		limit = 10000
	}
	if limit > 10000 {
		return nil, "", 0, &KinesisError{Code: "InvalidArgumentException", Message: "Limit must be between 1 and 10000", Status: 400}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	iter, ok := s.iterators[iteratorID]
	if !ok {
		return nil, "", 0, &KinesisError{Code: "InvalidArgumentException", Message: "Invalid shard iterator", Status: 400}
	}
	if time.Since(iter.CreatedAt) > 5*time.Minute {
		delete(s.iterators, iteratorID)
		return nil, "", 0, &KinesisError{Code: "ExpiredIteratorException", Message: "Shard iterator has expired", Status: 400}
	}

	st, ok := s.streams[iter.StreamARN]
	if !ok {
		delete(s.iterators, iteratorID)
		return nil, "", 0, &KinesisError{Code: "ResourceNotFoundException", Message: "Stream not found", Status: 400}
	}
	var ss *shardState
	for _, sh := range st.Shards {
		if sh.Shard.ShardID == iter.ShardID {
			ss = sh
			break
		}
	}
	if ss == nil {
		delete(s.iterators, iteratorID)
		return nil, "", 0, &KinesisError{Code: "ResourceNotFoundException", Message: "Shard not found", Status: 400}
	}

	retention := time.Duration(st.Stream.RetentionPeriodHours) * time.Hour
	cutoff := time.Now().UTC().Add(-retention)

	pos := iter.Position
	if pos < 0 {
		pos = 0
	}

	var records []Record
	totalSize := 0
	newPos := pos
	for i := pos; i < len(ss.Records) && len(records) < limit; i++ {
		r := ss.Records[i]
		if r.ApproximateArrivalTime.Before(cutoff) {
			newPos = i + 1
			continue
		}
		if totalSize+len(r.Data) > 10*1024*1024 {
			break
		}
		records = append(records, r)
		totalSize += len(r.Data)
		newPos = i + 1
	}

	// compute millisBehindLatest
	var millisBehind int64
	if len(ss.Records) > 0 {
		latest := ss.Records[len(ss.Records)-1].ApproximateArrivalTime
		if len(records) > 0 {
			last := records[len(records)-1].ApproximateArrivalTime
			d := latest.Sub(last)
			if d > 0 {
				millisBehind = d.Milliseconds()
			}
		} else {
			millisBehind = time.Since(latest).Milliseconds()
		}
	}

	// build next iterator (or nil if shard closed and consumed)
	delete(s.iterators, iteratorID)
	var nextID string
	if ss.Shard.IsOpen || newPos < len(ss.Records) {
		nextID = uuid()
		s.iterators[nextID] = &IteratorEntry{
			StreamARN: iter.StreamARN,
			ShardID:   iter.ShardID,
			Position:  newPos,
			CreatedAt: time.Now(),
		}
	}

	return records, nextID, millisBehind, nil
}

// ─── Consumers (ARN-based — already unique) ───────────────────────────────────

// RegisterConsumer creates an Enhanced Fan-Out consumer.
func (s *MemoryKinesisStore) RegisterConsumer(streamARN, consumerName, consumerARN string) (*Consumer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.streams[streamARN]
	if !ok {
		return nil, &KinesisError{Code: "ResourceNotFoundException", Message: "Stream not found for ARN " + streamARN, Status: 400}
	}
	for _, c := range st.Consumers {
		if c.Name == consumerName {
			return nil, &KinesisError{Code: "ResourceInUseException", Message: "Consumer " + consumerName + " already exists", Status: 400}
		}
	}
	if len(st.Consumers) >= 20 {
		return nil, &KinesisError{Code: "LimitExceededException", Message: "Maximum 20 consumers per stream", Status: 400}
	}
	c := &Consumer{
		Name:      consumerName,
		ARN:       consumerARN,
		StreamARN: streamARN,
		Status:    "ACTIVE",
		CreatedAt: time.Now().UTC(),
	}
	st.Consumers[consumerARN] = c
	return c, nil
}

// DeregisterConsumer removes a consumer by ARN.
func (s *MemoryKinesisStore) DeregisterConsumer(consumerARN string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, st := range s.streams {
		if _, ok := st.Consumers[consumerARN]; ok {
			delete(st.Consumers, consumerARN)
			return nil
		}
	}
	return &KinesisError{Code: "ResourceNotFoundException", Message: "Consumer not found: " + consumerARN, Status: 400}
}

// ListConsumers returns consumers for a stream ARN.
func (s *MemoryKinesisStore) ListConsumers(streamARN string) ([]*Consumer, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st, ok := s.streams[streamARN]
	if !ok {
		return nil, &KinesisError{Code: "ResourceNotFoundException", Message: "Stream not found for ARN " + streamARN, Status: 400}
	}
	out := make([]*Consumer, 0, len(st.Consumers))
	for _, c := range st.Consumers {
		cp := *c
		out = append(out, &cp)
	}
	return out, nil
}

// GetConsumer returns a consumer by ARN.
func (s *MemoryKinesisStore) GetConsumer(consumerARN string) (*Consumer, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, st := range s.streams {
		if c, ok := st.Consumers[consumerARN]; ok {
			cp := *c
			return &cp, nil
		}
	}
	return nil, &KinesisError{Code: "ResourceNotFoundException", Message: "Consumer not found: " + consumerARN, Status: 400}
}

// ─── Resource Policy (ARN-based) ──────────────────────────────────────────────

// SetResourcePolicy stores a resource policy by stream ARN.
func (s *MemoryKinesisStore) SetResourcePolicy(streamARN, policy string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.streams[streamARN]
	if !ok {
		return &KinesisError{Code: "ResourceNotFoundException", Message: "Stream not found for ARN " + streamARN, Status: 400}
	}
	st.Stream.ResourcePolicy = policy
	return nil
}

// GetResourcePolicy returns the resource policy for a stream ARN.
func (s *MemoryKinesisStore) GetResourcePolicy(streamARN string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st, ok := s.streams[streamARN]
	if !ok {
		return "", &KinesisError{Code: "ResourceNotFoundException", Message: "Stream not found for ARN " + streamARN, Status: 400}
	}
	return st.Stream.ResourcePolicy, nil
}

// DeleteResourcePolicy removes the resource policy for a stream ARN.
func (s *MemoryKinesisStore) DeleteResourcePolicy(streamARN string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.streams[streamARN]
	if !ok {
		return &KinesisError{Code: "ResourceNotFoundException", Message: "Stream not found for ARN " + streamARN, Status: 400}
	}
	st.Stream.ResourcePolicy = ""
	return nil
}

// ─── UpdateStreamMode (ARN-based) ────────────────────────────────────────────

// UpdateStreamModeByARN changes the stream mode by ARN.
func (s *MemoryKinesisStore) UpdateStreamModeByARN(arn string, mode StreamMode) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.streams[arn]
	if !ok {
		return &KinesisError{Code: "ResourceNotFoundException", Message: "Stream not found for ARN " + arn, Status: 400}
	}
	st.Stream.Mode = mode
	return nil
}

func max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// ─── admin ────────────────────────────────────────────────────────────────────

// Reset wipes all state.
func (s *MemoryKinesisStore) Reset() {
	s.mu.Lock()
	s.streams = make(map[string]*streamState)
	s.nameScope = make(map[string]string)
	s.iterators = make(map[string]*IteratorEntry)
	s.mu.Unlock()
}

// ─── snapshot / restore ───────────────────────────────────────────────────────

type snapshotData struct {
	Streams   map[string]*streamState   `json:"streams"`
	NameScope map[string]string         `json:"name_scope"`
	Iterators map[string]*IteratorEntry `json:"iterators"`
}

func (s *MemoryKinesisStore) Snapshot() (json.RawMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(snapshotData{
		Streams:   s.streams,
		NameScope: s.nameScope,
		Iterators: s.iterators,
	})
}

func (s *MemoryKinesisStore) Restore(data json.RawMessage) error {
	var snap snapshotData
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	s.mu.Lock()
	s.streams = snap.Streams
	if s.streams == nil {
		s.streams = make(map[string]*streamState)
	}
	s.nameScope = snap.NameScope
	if s.nameScope == nil {
		s.nameScope = make(map[string]string)
	}
	s.iterators = snap.Iterators
	if s.iterators == nil {
		s.iterators = make(map[string]*IteratorEntry)
	}
	s.mu.Unlock()
	return nil
}

// ─── error type ───────────────────────────────────────────────────────────────

// KinesisError is a structured error matching AWS Kinesis error responses.
type KinesisError struct {
	Code    string
	Message string
	Status  int
}

func (e *KinesisError) Error() string { return e.Code + ": " + e.Message }
