package es

// Versionable abstraction
type Versionable struct {
	aggregateVersion int64
}

// AppliedEvent wraps around event for version safety
type AppliedEvent struct {
	Event *Event
}

// Apply applies version increment and reduce function to aggregate events
func (v *Versionable) Apply(aggregate Aggregate, events []*Event) []*AppliedEvent {
	appliedEvents := []*AppliedEvent{}
	for _, event := range events {
		aggregate.Reduce(event.Type, event.Payload)
		v.aggregateVersion++
		event.AggregateVersion = v.aggregateVersion
		appliedEvents = append(appliedEvents, &AppliedEvent{Event: event})
	}
	return appliedEvents
}

func (v *Versionable) setVersion(version int64) {
	v.aggregateVersion = version
}
