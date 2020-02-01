package es_test

import (
	"testing"

	"github.com/indebted-modules/es"
	"github.com/stretchr/testify/suite"
)

type VerboseDriverSuite struct {
	suite.Suite
}

func TestVerboseDriverSuite(t *testing.T) {
	suite.Run(t, new(VerboseDriverSuite))
}

func (s *VerboseDriverSuite) TestDelegateLoadToInternalDriver() {
	fakeDriver := &FakeDriver{}
	verboseDriver := es.NewVerboseDriver(fakeDriver)

	events, err := verboseDriver.Load("123")
	s.Nil(events)
	s.Nil(err)
	s.True(fakeDriver.loadCalled)
}

func (s *VerboseDriverSuite) TestDelegateSaveToInternalDriver() {
	fakeDriver := &FakeDriver{}
	verboseDriver := es.NewVerboseDriver(fakeDriver)

	err := verboseDriver.Save([]*es.Event{})
	s.Nil(err)
	s.True(fakeDriver.saveCalled)
}
