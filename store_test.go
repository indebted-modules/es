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

func (s *StoreSuite) TestLoadWithEmptyAggregateID() {
	store := es.NewStore(&BrokenDriver{ErrorMessage: "driver should not have been called"})

	sampleAggregate := &SampleAggregate{}
	err := store.Load("", sampleAggregate)

	s.NoError(err)
}
