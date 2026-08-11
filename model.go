package revent_sdk_go

import (
	"errors"
)

var (
	ErrClientIDRequired       = errors.New("ClientID is required")
	_                   error = new(CantConnectToServerError)
)

type (
	// ClientID defines the client id to register to R-event.
	ClientID string
)

func (c ClientID) Validate() error {
	if c == "" {
		return ErrClientIDRequired
	}

	return nil
}

func (c ClientID) String() string {
	return string(c)
}
