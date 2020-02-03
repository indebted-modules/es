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

type FakeDriver struct {
	loadCalled bool
	saveCalled bool
}

func (d *FakeDriver) LoadSlice() ([]*es.Event, error) {
	panic("implement me")
}

func (d *FakeDriver) Load(aggregateID string) ([]*es.Event, error) {
	d.loadCalled = true
	return nil, nil
}

func (d *FakeDriver) Save(events []*es.Event) error {
	d.saveCalled = true
	return nil
}

func (s *VerboseDriverSuite) TestDelegateLoadToInternalDriver() {
	driver := es.NewInMemoryDriver()
	err := driver.Save([]*es.Event{es.NewEvent("123", &SomethingHappened{})})
	s.NoError(err)

	verboseDriver := es.NewVerboseDriver(driver)
	events, err := verboseDriver.Load("123")
	s.NoError(err)
	s.Equal(&SomethingHappened{}, events[0].Payload)
}

func (s *VerboseDriverSuite) TestDelegateSaveToInternalDriver() {
	driver := es.NewInMemoryDriver()
	verboseDriver := es.NewVerboseDriver(driver)

	err := verboseDriver.Save([]*es.Event{es.NewEvent("123", &SomethingHappened{})})
	s.NoError(err)
	s.Equal(&SomethingHappened{}, driver.Stream()[0].Payload)
}
