package es_test

import (
	"testing"

	"github.com/indebted-modules/es"
	"github.com/stretchr/testify/suite"
)

type RegistrySuite struct {
	suite.Suite
}

func TestRegistrySuite(t *testing.T) {
	suite.Run(t, new(RegistrySuite))
}

type SomethingHappened struct{}

func (SomethingHappened) PayloadType() string {
	return "SomethingHappened"
}
func (SomethingHappened) AggregateType() string {
	return "SampleAggregate"
}

type SomethingElseHappened struct{}

func (SomethingElseHappened) PayloadType() string { return "SomethingElseHappened" }
func (SomethingElseHappened) AggregateType() string {
	return "AnotherSampleAggregate"
}

func (s *RegistrySuite) TestResolveTypeFailsIfTypeNotRegistered() {
	r := es.NewRegistry()
	event, err := r.ResolveType("UnregisteredType")
	s.Equal("No type registered for 'UnregisteredType'", err.Error())
	s.Nil(event)
}

func (s *RegistrySuite) TestResolveTypeResolvesRegisteredValues() {
	r := es.NewRegistry()
	err := r.Register(SomethingHappened{})
	s.Nil(err)
	err = r.Register(SomethingElseHappened{})
	s.Nil(err)

	event, err := r.ResolveType("SomethingHappened")
	s.Nil(err)
	s.IsType(&SomethingHappened{}, event)

	event, err = r.ResolveType("SomethingElseHappened")
	s.Nil(err)
	s.IsType(&SomethingElseHappened{}, event)
}

func (s *RegistrySuite) TestRegisterFailsIfTypeAlreadyRegistered() {
	r := es.NewRegistry()
	err := r.Register(SomethingHappened{})
	s.Nil(err)

	err = r.Register(SomethingHappened{})
	s.Equal("Event payload already registered with name 'SomethingHappened'", err.Error())
}

func (s *RegistrySuite) TestRegisterFailsIfTypeIsPointer() {
	r := es.NewRegistry()
	err := r.Register(&SomethingHappened{})
	s.Equal("Pointers not allowed", err.Error())
}
