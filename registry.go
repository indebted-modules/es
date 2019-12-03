package es

import (
	"fmt"
	"reflect"

	"github.com/rs/zerolog/log"
)

var registry = NewRegistry()

type typeBuilder func() interface{}

// Registry is a type registry meant to be used as a way to get interfaces from type names
type Registry struct {
	entries map[string]typeBuilder
}

// NewRegistry creates an empty type registry
func NewRegistry() Registry {
	return Registry{
		entries: map[string]typeBuilder{},
	}
}

// Register adds the given interface to the registry of known types
func (r *Registry) Register(i EventPayload) error {
	t := reflect.TypeOf(i)
	if t.Kind() == reflect.Ptr {
		return fmt.Errorf("Pointers not allowed")
	}

	name := i.PayloadType()
	if _, ok := r.entries[name]; ok {
		return fmt.Errorf("Event payload already registered with name '%s'", name)
	}

	r.entries[name] = func() interface{} {
		return reflect.New(t).Interface()
	}

	return nil
}

// ResolveType looks for a registered type and returns a new pointer to it
func (r *Registry) ResolveType(name string) (interface{}, error) {
	resolve, ok := r.entries[name]
	if !ok {
		return nil, fmt.Errorf("No type registered for '%s'", name)
	}

	return resolve(), nil
}

// Register event type with payload value
func Register(i EventPayload) {
	err := registry.Register(i)
	if err != nil {
		log.
			Fatal().
			Err(err).
			Msg("Failed registering event type")
	}
}

func resolveType(name string) (interface{}, error) {
	return registry.ResolveType(name)
}
