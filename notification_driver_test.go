package es_test

import (
	"testing"

	"github.com/indebted-modules/es"
	"github.com/stretchr/testify/suite"
)

type NotificationDriverSuite struct {
	suite.Suite
}

func TestNotificationDriverSuite(t *testing.T) {
	suite.Run(t, new(NotificationDriverSuite))
}

type FakeNotifier struct {
	calls []interface{}
}

func (f *FakeNotifier) Publish(data interface{}) error {
	f.calls = append(f.calls, data)
	return nil
}

func (s *NotificationDriverSuite) TestDelegateLoadToInternalDriver() {
	fakeDriver := &FakeDriver{}
	fakeNotifier := &FakeNotifier{}
	driver := es.NewNotificationDriver(fakeNotifier, fakeDriver)

	events, err := driver.Load("123")
	s.Empty(events)
	s.NoError(err)
	s.True(fakeDriver.loadCalled)
	s.Empty(fakeNotifier.calls)
}

func (s *NotificationDriverSuite) TestSaveDoesNotPublishWhenBrokenDriver() {
	brokenDriver := &BrokenDriver{ErrorMessage: "borken!"}
	fakeNotifier := &FakeNotifier{}
	driver := es.NewNotificationDriver(fakeNotifier, brokenDriver)

	err := driver.Save([]*es.Event{})
	s.Error(err, "borken!")
	s.Empty(fakeNotifier.calls)
}

func (s *NotificationDriverSuite) TestSaveDoesNotPublishWhenNoEvents() {
	fakeDriver := &FakeDriver{}
	fakeNotifier := &FakeNotifier{}
	driver := es.NewNotificationDriver(fakeNotifier, fakeDriver)

	err := driver.Save([]*es.Event{})
	s.NoError(err)
	s.True(fakeDriver.saveCalled)
	s.Empty(fakeNotifier.calls)
}

func (s *NotificationDriverSuite) TestSavePublishesOneEvent() {
	fakeDriver := &FakeDriver{}
	fakeNotifier := &FakeNotifier{}
	driver := es.NewNotificationDriver(fakeNotifier, fakeDriver)

	err := driver.Save([]*es.Event{
		{Type: "SomethingHappened"},
	})
	s.NoError(err)
	s.True(fakeDriver.saveCalled)
	s.Equal([]interface{}{
		es.Packet{
			Types: []string{
				"SomethingHappened",
			},
		},
	}, fakeNotifier.calls)
}

func (s *NotificationDriverSuite) TestSavePublishesMultipleEvents() {
	fakeDriver := &FakeDriver{}
	fakeNotifier := &FakeNotifier{}
	driver := es.NewNotificationDriver(fakeNotifier, fakeDriver)

	err := driver.Save([]*es.Event{
		{Type: "SomethingHappened"},
		{Type: "SomethingHappened"},
		{Type: "SomethingElseHappened"},
		{Type: "SomethingElseHappened"},
	})
	s.NoError(err)
	s.True(fakeDriver.saveCalled)
	s.Equal([]interface{}{
		es.Packet{
			Types: []string{
				"SomethingHappened",
				"SomethingElseHappened",
			},
		},
	}, fakeNotifier.calls)
}
