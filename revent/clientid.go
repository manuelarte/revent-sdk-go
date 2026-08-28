package revent

import "errors"

var ErrClientIDRequired = errors.New("ClientID is required")

// ClientID defines the client id to register to R-event.
type ClientID string

func (c ClientID) Validate() error {
	if c == "" {
		return ErrClientIDRequired
	}

	return nil
}

func (c ClientID) String() string {
	return string(c)
}
