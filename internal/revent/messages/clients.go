package messages

import (
	"fmt"

	"github.com/manuelarte/revent-sdk-go/revent"
)

var (
	_ ClientMsg = new(ClientRegistrationMsg)
	_ ServerMsg = new(ClientRegisteredMsg)
	_ ServerMsg = new(ClientRegistrationErrorMsg)
)

var _ error = new(ClientRegistrationErrorMsg)

type (
	ClientRegistrationMsg struct {
		ClientID      revent.ClientID
		QueryHandlers []revent.QueryID
	}

	ClientRegisteredMsg struct {
		ClientID revent.ClientID
	}

	// ClientRegistrationResponseMsg r-event message indicating that
	// the client could not be registered.
	//nolint:errname // keep consistency with Msg at the end
	ClientRegistrationErrorMsg struct {
		ClientID revent.ClientID
		Reason   string
	}
)

func (e ClientRegistrationErrorMsg) Error() string {
	if e.Reason == "" {
		return fmt.Sprintf("client %q registration rejected", e.ClientID)
	}

	return fmt.Sprintf("client %q registration rejected: %s", e.ClientID, e.Reason)
}

func (c ClientRegistrationMsg) clientMessage()      {}
func (c ClientRegisteredMsg) serverMessage()        {}
func (c ClientRegistrationErrorMsg) serverMessage() {}
