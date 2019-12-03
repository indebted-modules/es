package es

// Aggregate interface
type Aggregate interface {
	Reduce(typ string, payload interface{})
	setVersion(version int64)
}
