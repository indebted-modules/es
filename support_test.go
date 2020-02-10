package es_test

import (
	"fmt"

	"github.com/indebted-modules/es"
)

// SampleAggregate sample aggregate for testing
type SampleAggregate struct {
	es.Versionable
	ReducedData []string
}

func (s *SampleAggregate) Reduce(typ string, payload interface{}) {
	switch typ {
	case "SomethingHappened":
		event := payload.(*SomethingHappened)
		s.ReducedData = append(s.ReducedData, event.Data)
	}
}

// DoSomething commands do validation and return events
func (s *SampleAggregate) DoSomething(id string, data []string) []*es.AppliedEvent {
	events := []*es.Event{}

	for i := range data {
		events = append(events, es.NewEvent(id, &SomethingHappened{Data: data[i]}))
	}

	return s.Apply(s, events)
}

// AnotherSampleAggregate another sample aggregate for testing
type AnotherSampleAggregate struct {
	es.Versionable
	ReducedData []string
}

func (a *AnotherSampleAggregate) Reduce(typ string, payload interface{}) {
	switch typ {
	case "SomethingElseHappened":
		event := payload.(*SomethingElseHappened)
		a.ReducedData = append(a.ReducedData, event.Data)
	}
}

// DoSomethingElse commands do validation and return events
func (a *AnotherSampleAggregate) DoSomethingElse(id string, data []string) []*es.AppliedEvent {
	events := []*es.Event{}

	for i := range data {
		events = append(events, es.NewEvent(id, &SomethingElseHappened{Data: data[i]}))
	}

	return a.Apply(a, events)
}

// BrokenDriver always returns error for testing error cases
type BrokenDriver struct {
	ErrorMessage string
}

func (d *BrokenDriver) Load(_ string) ([]*es.Event, error) {
	return nil, fmt.Errorf(d.ErrorMessage)
}

func (d *BrokenDriver) Save(_ []*es.Event) error {
	return fmt.Errorf(d.ErrorMessage)
}

func (d *BrokenDriver) ReadEventsForward(position int64, count uint) ([]*es.Event, error) {
	return nil, fmt.Errorf(d.ErrorMessage)
}

// SomethingHappened event sample for testing
type SomethingHappened struct {
	Data string
}

func (SomethingHappened) PayloadType() string {
	return "SomethingHappened"
}

func (SomethingHappened) AggregateType() string {
	return "SampleAggregate"
}

// SomethingElseHappened another event sample for testing
type SomethingElseHappened struct {
	Data string
}

func (SomethingElseHappened) PayloadType() string {
	return "SomethingElseHappened"
}

func (SomethingElseHappened) AggregateType() string {
	return "AnotherSampleAggregate"
}

func init() {
	es.Register(SomethingHappened{})
	es.Register(SomethingElseHappened{})
}
