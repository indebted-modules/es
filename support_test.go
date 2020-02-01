package es_test

import (
	"fmt"
	"github.com/indebted-modules/es"
)

// SampleAggregate .
type SampleAggregate struct {
	es.Versionable
}

func (SampleAggregate) Reduce(typ string, payload interface{}) {

}

// FakeDriver .
type FakeDriver struct {
	loadCalled bool
	saveCalled bool
}

func (d *FakeDriver) Load(aggregateID string) ([]*es.Event, error) {
	d.loadCalled = true
	return nil, nil
}

func (d *FakeDriver) Save(events []*es.Event) error {
	d.saveCalled = true
	return nil
}

// BrokenDriver .
type BrokenDriver struct {
	ErrorMessage string
}

func (d *BrokenDriver) Load(_ string) ([]*es.Event, error) {
	return nil, fmt.Errorf(d.ErrorMessage)
}

func (d *BrokenDriver) Save(_ []*es.Event) error {
	return fmt.Errorf(d.ErrorMessage)
}

// SomethingHappened .
type SomethingHappened struct {
}

func (SomethingHappened) PayloadType() string {
	return "SomethingHappened"
}

func (SomethingHappened) AggregateType() string {
	return "SampleAggregate"
}

// SomethingElseHappened
type SomethingElseHappened struct {
}

func (SomethingElseHappened) PayloadType() string {
	return "SomethingElseHappened"
}

func (SomethingElseHappened) AggregateType() string {
	return "AnotherSampleAggregate"
}

// EventType .
type EventType struct {
	ID string
}

func (EventType) PayloadType() string {
	return "EventType"
}

func (EventType) AggregateType() string {
	return "AggregateType"
}
