package es

import "github.com/rs/zerolog/log"

// NewVerboseDriver creates a new VerboseDriver
func NewVerboseDriver(driver Driver) *VerboseDriver {
	return &VerboseDriver{Driver: driver}
}

// VerboseDriver implementation for deployed environments
type VerboseDriver struct {
	Driver Driver
}

// Load delegates to internal driver
func (s *VerboseDriver) Load(aggregateID string) ([]*Event, error) {
	return s.Driver.Load(aggregateID)
}

// Save delegates to internal driver and log all produced events
func (s *VerboseDriver) Save(events []*Event) error {
	err := s.Driver.Save(events)
	if err != nil {
		return err
	}

	for _, event := range events {
		log.
			Info().
			Str("EventID", event.ID).
			Str("EventType", event.Type).
			Str("AggregateID", event.AggregateID).
			Str("AggregateType", event.AggregateType).
			Int64("AggregateVersion", event.AggregateVersion).
			Time("Created", event.Created).
			Msg("Produced event")
	}

	return nil
}
