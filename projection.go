package es

// Projection abstraction
type Projection interface {
	Reduce(event Event)
}
