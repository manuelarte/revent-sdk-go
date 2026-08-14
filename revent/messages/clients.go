package messages

import (
	"fmt"

	"github.com/manuelarte/revent-sdk-go/revent"
)

var (
	_ ClientMsg = new(ClientRegistrationMsg)
	_ ServerMsg = new(ClientRegistrationResponseMsg)
)

var _ error = new(ClientRegistrationErrorMsg)

type (
	ClientRegistrationMsg struct {
		ClientID      revent.ClientID
		QueryHandlers []revent.QueryID
	}

	ClientRegistrationResponseMsg struct {
		Msg *ClientRegisteredMsg
		Err *ClientRegistrationErrorMsg
	}

	ClientRegisteredMsg struct {
		ClientID revent.ClientID
	}

	//nolint:errname // keep consistency with Msg at the end
	ClientRegistrationErrorMsg struct {
		ClientID revent.ClientID
		Reason   string
	}
)

func (c ClientRegistrationResponseMsg) ClientID() revent.ClientID {
	if c.Msg != nil {
		return c.Msg.ClientID
	}

	if c.Err != nil {
		return c.Err.ClientID
	}

	return ""
}

func (e ClientRegistrationErrorMsg) Error() string {
	if e.Reason == "" {
		return fmt.Sprintf("client %q registration rejected", e.ClientID)
	}

	return fmt.Sprintf("client %q registration rejected: %s", e.ClientID, e.Reason)
}

func (c ClientRegistrationMsg) clientMessage()         {}
func (c ClientRegistrationResponseMsg) serverMessage() {}
