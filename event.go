package es

import (
	"time"

	"github.com/indebted-modules/uuid"
)

// EventPayload interface
type EventPayload interface {
	PayloadType() string
	AggregateType() string
}

// Event model
type Event struct {
	ID               string
	Type             string
	AggregateID      string
	AggregateType    string
	AggregateVersion int64
	Payload          interface{}
	Created          time.Time
}

// NewEvent creates a new event
func NewEvent(aggregateID string, payload EventPayload) *Event {
	// TODO: validate if payload is a pointer
	event := &Event{
		ID:            uuid.NewID(),
		Type:          payload.PayloadType(),
		AggregateID:   aggregateID,
		AggregateType: payload.AggregateType(),
		Payload:       payload,
		Created:       time.Now(),
	}
	return event
}
