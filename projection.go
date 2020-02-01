package es

// Projection abstraction
type Projection struct {
	version int64
}

func (v *Projection) setVersion(version int64) {
	v.version = version
}
