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

func (s *StoreSuite) TestStoreSave() {
	driver := es.NewInMemoryDriver()
	store := es.NewStore(driver)

	sampleAggregate := &SampleAggregate{}
	err := store.Save(sampleAggregate.DoSomething("1", []string{"event-1", "event-2"}))
	s.NoError(err)

	stream := driver.Stream()
	s.Equal(2, len(stream))
	s.Equal(stream[0].Payload, &SomethingHappened{Data: "event-1"})
	s.Equal(stream[1].Payload, &SomethingHappened{Data: "event-2"})
}

func (s *StoreSuite) TestLoadProjection() {
	driver := es.NewInMemoryDriver()
	err := driver.Save([]*es.Event{
		es.NewEvent("1", &TaskCreated{Description: "Do the dishes"}),
		es.NewEvent("2", &TaskCreated{Description: "Cook rice"}),
	})
	store := es.NewStore(driver)

	tasks := &TasksProjection{Tasks: map[string]string{}}
	err = store.LoadProjection(tasks)
	s.NoError(err)
	s.Equal(tasks.Tasks, map[string]string{"1": "Do the dishes", "2": "Cook rice"})
}

func (s *StoreSuite) TestLoadWithEmptyAggregateID() {
	store := es.NewStore(&BrokenDriver{ErrorMessage: "driver should not have been called"})

	sampleAggregate := &SampleAggregate{}
	err := store.Load("", sampleAggregate)

	s.NoError(err)
}

func (s *StoreSuite) evtVersion(event *es.Event, version int64) *es.Event{
		event.AggregateVersion = version
		return event
	}

type TaskCreated struct {
	Description string
}

func (t *TaskCreated) PayloadType() string {
	return "TaskCreated"
}

func (t *TaskCreated) AggregateType() string {
	return "Task"
}

type TasksProjection struct {
	Tasks map[string]string
}

func (t *TasksProjection) Reduce(event *es.Event) {
	switch event.Type {
	case "TaskCreated":
		taskCreated := event.Payload.(TaskCreated)
		t.Tasks[event.ID] = taskCreated.Description
	}
}
