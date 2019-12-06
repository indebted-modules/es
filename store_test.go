package es_test

import (
	"fmt"
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

type BrokenDriver struct {
	ErrorMessage string
}

func (d *BrokenDriver) Load(aggregateID string) ([]*es.Event, error) {
	return nil, fmt.Errorf(d.ErrorMessage)
}

func (d *BrokenDriver) Save(events []*es.Event) error {
	return fmt.Errorf(d.ErrorMessage)
}
func (s *StoreSuite) TestLoadWithEmptyAggregateID() {
	store := es.NewStore(&BrokenDriver{ErrorMessage: "driver should not have been called"})

	sampleAggregate := &SampleAggregate{}
	err := store.Load("", sampleAggregate)

	s.NoError(err)
}
