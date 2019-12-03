package es_test

import (
	"testing"
	"time"

	"github.com/indebted-modules/es"
	"github.com/indebted-modules/uuid"
	"github.com/stretchr/testify/suite"
)

type EventSuite struct {
	suite.Suite
}

type SampleAggregate struct {
	es.Versionable
}

type AnotherSampleAggregate struct {
	es.Versionable
}

func TestEventSuite(t *testing.T) {
	suite.Run(t, new(EventSuite))
}

func (SampleAggregate) Reduce(typ string, payload interface{}) {}

func (AnotherSampleAggregate) Reduce(typ string, payload interface{}) {}

func (s *EventSuite) TestNewEvent() {
	somethingHappened := &SomethingHappened{}
	firstEvent := es.NewEvent("aggregate-id", somethingHappened)
	s.True(uuid.ValidateID(firstEvent.ID))
	s.Equal("SomethingHappened", firstEvent.Type)
	s.Equal("aggregate-id", firstEvent.AggregateID)
	s.Equal("SampleAggregate", firstEvent.AggregateType)
	s.Equal(int64(0), firstEvent.AggregateVersion)
	s.Same(somethingHappened, firstEvent.Payload)
	s.WithinDuration(time.Now(), firstEvent.Created, time.Millisecond*200)

	somethingElseHappened := &SomethingElseHappened{}
	secondEvent := es.NewEvent("another-aggregate-id", somethingElseHappened)
	s.True(uuid.ValidateID(secondEvent.ID))
	s.Equal("SomethingElseHappened", secondEvent.Type)
	s.Equal("another-aggregate-id", secondEvent.AggregateID)
	s.Equal("AnotherSampleAggregate", secondEvent.AggregateType)
	s.Equal(int64(0), secondEvent.AggregateVersion)
	s.Same(somethingElseHappened, secondEvent.Payload)
	s.WithinDuration(time.Now(), secondEvent.Created, time.Millisecond*200)

	s.NotEqual(firstEvent.ID, secondEvent.ID)
	s.NotEqual(firstEvent.Created, secondEvent.Created)
}
