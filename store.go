package es

// Store implementation
type Store struct {
	driver Driver
}

// NewStore creates a new store
func NewStore(driver Driver) *Store {
	return &Store{
		driver: driver,
	}
}

// Load loads aggregate by ID
func (s *Store) Load(aggregateID string, aggregate Aggregate) error {
	events, err := s.driver.Load(aggregateID)
	if err != nil {
		return err
	}
	for _, event := range events {
		aggregate.Reduce(event.Type, event.Payload)
		aggregate.setVersion(event.AggregateVersion)
	}
	return nil
}

// Save saves aggregate events
func (s *Store) Save(appliedEvents []*AppliedEvent) error {
	events := []*Event{}
	for _, appliedEvent := range appliedEvents {
		events = append(events, appliedEvent.Event)
	}
	return s.driver.Save(events)
}
