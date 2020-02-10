package es

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"
)

// NewInMemoryDriver creates a new InMemoryDriver
func NewInMemoryDriver() *InMemoryDriver {
	return &InMemoryDriver{
		sequence: 0,
		stream:   map[string]map[int64]*record{},
		clock:    time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC),
	}
}

// InMemoryDriver implementation for unit testing
type InMemoryDriver struct {
	sequence int64
	stream   map[string]map[int64]*record
	clock    time.Time
}

// Load all events by aggregate ID
func (s *InMemoryDriver) Load(aggregateID string) ([]*Event, error) {
	records := s.stream[aggregateID]

	var events []*Event
	for _, record := range records {
		event, err := record.toEvent()
		if err != nil {
			return nil, err
		}

		events = append(events, event)
	}

	sort.Slice(events, func(i, j int) bool {
		return events[i].AggregateVersion < events[j].AggregateVersion
	})

	return events, nil
}

// Save all events in memory
func (s *InMemoryDriver) Save(events []*Event) error {
	newStream := map[string]map[int64]*record{}
	newSequence := s.sequence
	newClock := s.clock
	deepCopy(s.stream, newStream)

	for _, event := range events {
		newSequence++
		event.ID = strconv.FormatInt(newSequence, 10)
		event.Created = newClock
		newClock = newClock.Add(1 * time.Second)

		r, err := toRecord(event)
		if err != nil {
			return err
		}

		if _, ok := newStream[r.AggregateID]; !ok {
			newStream[r.AggregateID] = map[int64]*record{}
		}

		if _, ok := newStream[r.AggregateID][r.AggregateVersion]; ok {
			return fmt.Errorf("optmistic locking violation")
		}

		newStream[r.AggregateID][r.AggregateVersion] = r
	}

	deepCopy(newStream, s.stream)
	s.sequence = newSequence
	s.clock = newClock
	return nil
}

// ReadEventsOfTypes .
func (s *InMemoryDriver) ReadEventsOfTypes(position int64, count uint) ([]*Event, error) {
	stream := s.Stream()
	limit := math.Min(float64(len(stream)), float64(position+int64(count)))
	return stream[position:int64(limit)], nil
}

func deepCopy(source, destination map[string]map[int64]*record) {
	for aggregateID, records := range source {
		for version, r := range records {
			if _, ok := destination[aggregateID]; !ok {
				destination[aggregateID] = map[int64]*record{}
			}
			destination[aggregateID][version] = r
		}
	}
}

// Stream all events
func (s *InMemoryDriver) Stream() []*Event {
	var events []*Event
	for _, records := range s.stream {
		for _, record := range records {
			event, err := record.toEvent()
			if err != nil {
				log.
					Fatal().
					Err(err).
					Msg("Failed reading in-memory stream")
			}

			events = append(events, event)
		}
	}

	sort.Slice(events, func(i, j int) bool {
		return events[i].ID < events[j].ID
	})

	return events
}

type record struct {
	ID               string
	Type             string
	AggregateID      string
	AggregateType    string
	AggregateVersion int64
	Payload          string
	Created          string
}

func (r *record) toEvent() (*Event, error) {
	payload, err := resolveType(r.Type)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal([]byte(r.Payload), payload)
	if err != nil {
		return nil, err
	}

	var created time.Time
	err = json.Unmarshal([]byte(r.Created), &created)
	if err != nil {
		return nil, err
	}

	return &Event{
		ID:               r.ID,
		Type:             r.Type,
		AggregateID:      r.AggregateID,
		AggregateType:    r.AggregateType,
		AggregateVersion: r.AggregateVersion,
		Payload:          payload,
		Created:          created,
	}, nil
}

func toRecord(e *Event) (*record, error) {
	payload, err := json.Marshal(e.Payload)
	if err != nil {
		return nil, err
	}

	created, err := json.Marshal(e.Created)
	if err != nil {
		return nil, err
	}

	return &record{
		ID:               e.ID,
		Type:             e.Type,
		AggregateID:      e.AggregateID,
		AggregateType:    e.AggregateType,
		AggregateVersion: e.AggregateVersion,
		Payload:          string(payload),
		Created:          string(created),
	}, nil
}
