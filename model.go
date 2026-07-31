package revent_sdk_go

import (
	"errors"
	"sync"

	"github.com/manuelarte/revent-sdk-go/revent"
)

var ErrClientIDRequired = errors.New("ClientID is required")

type (
	// ClientID defines the client id to register to R-event.
	ClientID string

	State struct {
		cfg             Config
		muQueryHandlers sync.RWMutex
		queryHandlers   map[revent.QueryID]any
	}
)

func (c ClientID) Validate() error {
	if c == "" {
		return ErrClientIDRequired
	}

	return nil
}

func NewState(cfg Config) *State {
	return &State{
		cfg:           cfg,
		queryHandlers: make(map[revent.QueryID]any),
	}
}
