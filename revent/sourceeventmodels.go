package revent

import (
	"context"
	"encoding/json"
	"time"
)

type (
	Event interface {
		json.Marshaler
		json.Unmarshaler
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
