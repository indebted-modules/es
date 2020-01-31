package es

type notificationDriver struct {
	notifier Notifier
	driver   Driver
}

func (s *notificationDriver) Load(aggregateID string) ([]*Event, error) {
	return s.driver.Load(aggregateID)
}

func (s *notificationDriver) Save(events []*Event) error {
	err := s.driver.Save(events)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}

	types := s.extractEventTypes(events)
	err = s.notifier.Publish(Packet{Types: types})
	if err != nil {
		return err // TODO: return error? log? relaxed approach to notifying?
	}

	return nil
}

func (s *notificationDriver) extractEventTypes(events []*Event) []string {
	set := make(map[string]bool)
	types := make([]string, 0, len(set))
	for _, event := range events {
		if set[event.Type] == false {
			set[event.Type] = true
			types = append(types, event.Type)
		}
	}
	return types
}

type Packet struct {
	Types []string
}

func NewNotificationDriver(notifier Notifier, driver Driver) *notificationDriver {
	return &notificationDriver{
		notifier: notifier,
		driver:   driver,
	}
}
