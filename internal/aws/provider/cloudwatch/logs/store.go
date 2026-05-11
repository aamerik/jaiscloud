package logs

import "sync"

// LogGroup holds metadata for a CloudWatch Logs log group.
type LogGroup struct {
	LogGroupName    string
	CreationTime    int64
	RetentionInDays *int
	Arn             string
	StoredBytes     int64
}

// LogStream holds metadata for a CloudWatch Logs log stream.
type LogStream struct {
	LogStreamName       string
	CreationTime        int64
	FirstEventTimestamp int64
	LastEventTimestamp  int64
	LastIngestionTime   int64
	UploadSequenceToken string
	StoredBytes         int64
	Arn                 string
}

// LogEvent is a single log entry stored in a stream.
type LogEvent struct {
	Timestamp     int64
	Message       string
	IngestionTime int64
}

const maxEventsPerStream = 10_000

// eventRing is a bounded ring buffer of LogEvents.
type eventRing struct {
	buf  []LogEvent
	head int // index of the oldest event
	size int // number of events currently stored
	cap  int
}

func newEventRing() *eventRing {
	return &eventRing{
		buf: make([]LogEvent, maxEventsPerStream),
		cap: maxEventsPerStream,
	}
}

// Append adds events to the ring, overwriting the oldest if full.
func (r *eventRing) Append(events []LogEvent) {
	for _, e := range events {
		if r.size < r.cap {
			// Not yet full — write at head+size
			idx := (r.head + r.size) % r.cap
			r.buf[idx] = e
			r.size++
		} else {
			// Full — overwrite oldest (head) and advance head
			r.buf[r.head] = e
			r.head = (r.head + 1) % r.cap
		}
	}
}

// Slice returns all stored events in insertion order (oldest first).
func (r *eventRing) Slice() []LogEvent {
	if r.size == 0 {
		return nil
	}
	out := make([]LogEvent, r.size)
	for i := 0; i < r.size; i++ {
		out[i] = r.buf[(r.head+i)%r.cap]
	}
	return out
}

// SubscriptionFilter holds a CloudWatch Logs subscription filter.
type SubscriptionFilter struct {
	LogGroupName   string `json:"logGroupName"`
	FilterName     string `json:"filterName"`
	FilterPattern  string `json:"filterPattern"`
	DestinationArn string `json:"destinationArn"`
	Distribution   string `json:"distribution"`
	CreationTime   int64  `json:"creationTime"`
}

// memStore is the in-memory backing store for CloudWatch Logs.
type memStore struct {
	mu sync.RWMutex

	// groups maps groupName → *LogGroup
	groups map[string]*LogGroup
	// streams maps groupName → streamName → *LogStream
	streams map[string]map[string]*LogStream
	// events maps groupName → streamName → *eventRing
	events map[string]map[string]*eventRing
	// tags maps groupArn → map[string]string
	tags map[string]map[string]string
	// seqToken maps groupName → streamName → counter
	seqToken map[string]map[string]int64
	// subscriptionFilters maps groupName → filterName → *SubscriptionFilter
	subscriptionFilters map[string]map[string]*SubscriptionFilter
}

func newMemStore() *memStore {
	s := &memStore{}
	s.reset()
	return s
}

func (s *memStore) reset() {
	s.groups = make(map[string]*LogGroup)
	s.streams = make(map[string]map[string]*LogStream)
	s.events = make(map[string]map[string]*eventRing)
	s.tags = make(map[string]map[string]string)
	s.seqToken = make(map[string]map[string]int64)
	s.subscriptionFilters = make(map[string]map[string]*SubscriptionFilter)
}

// Reset wipes all state (called on POST /_jaiscloud/reset).
func (s *memStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reset()
}
