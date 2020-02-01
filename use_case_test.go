package es_test

import (
	"sync"
	"testing"

	"github.com/indebted-modules/es"
	"github.com/stretchr/testify/suite"
)

type UseCaseSuite struct {
	suite.Suite
}

func TestUseCaseSuite(t *testing.T) {
	suite.Run(t, new(UseCaseSuite))
}

func (s *UseCaseSuite) TestAggregateLifecycle() {
	store := es.NewStore(es.NewInMemoryDriver())

	sampleAggregate := &SampleAggregate{}
	events := sampleAggregate.DoSomething("1", []string{"event-1", "event-2"})
	err := store.Save(events)
	s.NoError(err)
	s.Equal(
		[]string{"event-1", "event-2"},
		sampleAggregate.ReducedData,
		"Apply events during memory lifecycle",
	)

	newInMemoryCycle := &SampleAggregate{}
	err = store.Load("1", newInMemoryCycle)
	s.NoError(err)
	s.Equal(
		[]string{"event-1", "event-2"},
		newInMemoryCycle.ReducedData,
		"Re-apply events when loading aggregate to memory again",
	)
}

func (s *UseCaseSuite) TestConcurrentWriting() {
	store := es.NewStore(es.NewInMemoryDriver())

	sampleAggregate := &SampleAggregate{}
	events := sampleAggregate.DoSomething("1", []string{"event-1", "event-2"})
	err := store.Save(events)
	s.NoError(err)

	newInMemory1 := &SampleAggregate{}
	err = store.Load("1", newInMemory1)
	s.NoError(err)

	newInMemory2 := &SampleAggregate{}
	err = store.Load("1", newInMemory2)
	s.NoError(err)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		_ = store.Save(newInMemory1.DoSomething("1", []string{"event-3"}))
		wg.Done()
	}()
	go func() {
		_ = store.Save(newInMemory2.DoSomething("1", []string{"event-3"}))
		wg.Done()
	}()
	wg.Wait()

	newInMemoryCycle := &SampleAggregate{}
	err = store.Load("1", newInMemoryCycle)
	s.NoError(err)
	s.Equal(
		[]string{"event-1", "event-2", "event-3"},
		newInMemoryCycle.ReducedData,
		"Don't apply events to stale aggregates",
	)
}
