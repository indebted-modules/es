package es_test

import (
	"fmt"

	"github.com/indebted-modules/es"
)

// SampleAggregate .
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

// AnotherSampleAggregate .
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
