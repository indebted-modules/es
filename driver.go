package es

// Driver interface
type Driver interface {
	Load(aggregateID string) ([]*Event, error)
	Save(events []*Event) error
	ReadEventsOfTypes(position int64, count uint, types []string) ([]*Event, error)
}
