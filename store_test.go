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

func (s *StoreSuite) TestLoad() {
	driver := es.NewInMemoryDriver()
	err := driver.Save([]*es.Event{
		es.NewEvent("1", &SomethingHappened{Data: "event-1"}),
		es.NewEvent("1", &SomethingHappened{Data: "event-2"}),
		es.NewEvent("2", &SomethingElseHappened{Data: "event-3"}),
		es.NewEvent("2", &SomethingElseHappened{Data: "event-4"}),
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
	s.Equal([]string{"event-3", "event-4"}, anotherAggregate.ReducedData)
}

func (s *StoreSuite) TestLoadWithEmptyAggregateID() {
	store := es.NewStore(&BrokenDriver{ErrorMessage: "driver should not have been called"})

	sampleAggregate := &SampleAggregate{}
	err := store.Load("", sampleAggregate)

	s.NoError(err)
}
