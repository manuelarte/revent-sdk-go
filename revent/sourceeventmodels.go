package revent

import (
	"context"
	"time"
)

type (
	// Event is the payload of a SourceEvent. It must be a type that
	// encoding/json can marshal and unmarshal.
	Event interface {
		ID() string
	}

	SourceEvent[E Event] struct {
		MonotonicClock uint64    `json:"monotonic_clock"`
		CreatedAt      time.Time `json:"created_at"`
		Payload        E         `json:"payload"`
	}

	// SourceEventHandler defines a function that can handle an event.
	SourceEventHandler[E Event, S SourceEvent[E]] func(context.Context, S)
)
