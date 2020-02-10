package es_test

import (
	"testing"
	"time"

	"github.com/indebted-modules/es"
	"github.com/stretchr/testify/suite"
)

type InMemoryDriverSuite struct {
	suite.Suite
}

func TestInMemoryDriverSuite(t *testing.T) {
	suite.Run(t, new(InMemoryDriverSuite))
}

func (s *InMemoryDriverSuite) TestReadEventsForward() {
	driver := es.NewInMemoryDriver()
	err := driver.Save([]*es.Event{
		es.NewEvent("uuid-1", &SomethingHappened{Data: "1"}),
		es.NewEvent("uuid-2", &SomethingHappened{Data: "2"}),
		es.NewEvent("uuid-3", &SomethingElseHappened{Data: "3"}),
	})
	s.NoError(err)

	events, err := driver.ReadEventsForward(0, 1)
	s.NoError(err)
	s.Equal([]*es.Event{
		{
			ID:               "1",
			Type:             "SomethingHappened",
			AggregateID:      "uuid-1",
			AggregateType:    "SampleAggregate",
			AggregateVersion: 0,
			Payload:          &SomethingHappened{Data: "1"},
			Created:          time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC),
		},
	}, events)

	events, err = driver.ReadEventsForward(1, 1)
	s.NoError(err)
	s.Equal([]*es.Event{
		{
			ID:               "2",
			Type:             "SomethingHappened",
			AggregateID:      "uuid-2",
			AggregateType:    "SampleAggregate",
			AggregateVersion: 0,
			Payload:          &SomethingHappened{Data: "2"},
			Created:          time.Date(2000, time.January, 1, 0, 0, 1, 0, time.UTC),
		},
	}, events)

	events, err = driver.ReadEventsForward(2, 1)
	s.NoError(err)
	s.Equal([]*es.Event{
		{
			ID:               "3",
			Type:             "SomethingElseHappened",
			AggregateID:      "uuid-3",
			AggregateType:    "AnotherSampleAggregate",
			AggregateVersion: 0,
			Payload:          &SomethingElseHappened{Data: "3"},
			Created:          time.Date(2000, time.January, 1, 0, 0, 2, 0, time.UTC),
		},
	}, events)

	events, err = driver.ReadEventsForward(3, 1)
	s.NoError(err)
	s.Empty(events)

	events, err = driver.ReadEventsForward(0, 2)
	s.NoError(err)
	s.Equal([]*es.Event{
		{
			ID:               "1",
			Type:             "SomethingHappened",
			AggregateID:      "uuid-1",
			AggregateType:    "SampleAggregate",
			AggregateVersion: 0,
			Payload:          &SomethingHappened{Data: "1"},
			Created:          time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			ID:               "2",
			Type:             "SomethingHappened",
			AggregateID:      "uuid-2",
			AggregateType:    "SampleAggregate",
			AggregateVersion: 0,
			Payload:          &SomethingHappened{Data: "2"},
			Created:          time.Date(2000, time.January, 1, 0, 0, 1, 0, time.UTC),
		},
	}, events)

	events, err = driver.ReadEventsForward(1, 10)
	s.NoError(err)
	s.Equal([]*es.Event{
		{
			ID:               "2",
			Type:             "SomethingHappened",
			AggregateID:      "uuid-2",
			AggregateType:    "SampleAggregate",
			AggregateVersion: 0,
			Payload:          &SomethingHappened{Data: "2"},
			Created:          time.Date(2000, time.January, 1, 0, 0, 1, 0, time.UTC),
		},
		{
			ID:               "3",
			Type:             "SomethingElseHappened",
			AggregateID:      "uuid-3",
			AggregateType:    "AnotherSampleAggregate",
			AggregateVersion: 0,
			Payload:          &SomethingElseHappened{Data: "3"},
			Created:          time.Date(2000, time.January, 1, 0, 0, 2, 0, time.UTC),
		},
	}, events)
}
