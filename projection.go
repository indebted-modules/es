package es

type Projection interface {
	Reduce(event *Event)
}
