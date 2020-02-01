package es_test

import (
	"testing"

	"github.com/indebted-modules/es"
	"github.com/stretchr/testify/suite"
)

type StoreSuite struct {
	suite.Suite
}

func TestStoreSuite(t *testing.T) {
	suite.Run(t, new(StoreSuite))
}

func (s *StoreSuite) TestStoreLoad() {
	driver := es.NewInMemoryDriver()
	err := driver.Save([]*es.Event{
		s.evtVersion(es.NewEvent("1", &SomethingHappened{Data: "event-1"}), 1),
		s.evtVersion(es.NewEvent("1", &SomethingHappened{Data: "event-2"}), 2),
		s.evtVersion(es.NewEvent("2", &SomethingElseHappened{Data: "event-3"}), 1),
		s.evtVersion(es.NewEvent("2", &SomethingElseHappened{Data: "event-4"}), 2),
		s.evtVersion(es.NewEvent("2", &SomethingElseHappened{Data: "event-5"}), 3),
	})
	s.NoError(err)

	store := es.NewStore(driver)

	sampleAggregate := &SampleAggregate{}
	err = store.Load("1", sampleAggregate)
	s.NoError(err)
	s.Equal([]string{"event-1", "event-2"}, sampleAggregate.ReducedData)

	anotherAggregate := &AnotherSampleAggregate{}
	err = store.Load("2", anotherAggregate)
	s.NoError(err)
	s.Equal([]string{"event-3", "event-4", "event-5"}, anotherAggregate.ReducedData)
}

func (s *StoreSuite) TestLoadWithEmptyAggregateID() {
	store := es.NewStore(&BrokenDriver{ErrorMessage: "driver should not have been called"})

	sampleAggregate := &SampleAggregate{}
	err := store.Load("", sampleAggregate)

	s.NoError(err)
}

func (s *StoreSuite) evtVersion(event *es.Event, version int64) *es.Event {
	event.AggregateVersion = version
	return event
}
