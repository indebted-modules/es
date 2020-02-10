package es

// Driver interface
type Driver interface {
	Load(aggregateID string) ([]*Event, error)
	Save(events []*Event) error
	ReadEventsForward(position int64) ([]*Event, error)
}
