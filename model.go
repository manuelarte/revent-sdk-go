package revent_sdk_go

import (
	"errors"
	"fmt"
)

var (
	ErrClientIDRequired         = errors.New("ClientID is required")
	ErrStreamNotConnected       = errors.New("stream not connected")
	_                     error = new(CantConnectToServerError)
)

type (
	// ClientID defines the client id to register to R-event.
	ClientID string

	CantConnectToServerError struct {
		Addr        string
		NumAttempts int
	}

	ILogger interface {
		Info(msg string, args ...any)
		Error(msg string, args ...any)
		Warn(msg string, args ...any)
		Debug(msg string, args ...any)
	}
)

func (c CantConnectToServerError) Error() string {
	return fmt.Sprintf("failed to connect to server at %s after %d attempts", c.Addr, c.NumAttempts)
}

func (c ClientID) Validate() error {
	if c == "" {
		return ErrClientIDRequired
	}

	return nil
}

func (c ClientID) String() string {
	return string(c)
}
