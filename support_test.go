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
	Data string
}

func (SomethingHappened) PayloadType() string {
	return "SomethingHappened"
}

func (SomethingHappened) AggregateType() string {
	return "SampleAggregate"
}

// SomethingElseHappened
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
