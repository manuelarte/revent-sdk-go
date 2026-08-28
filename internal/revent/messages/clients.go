package messages

import (
	"fmt"

	"github.com/manuelarte/revent-sdk-go/revent"
)

var (
	_ ClientMsg = new(ClientRegistrationMsg)
	_ ServerMsg = new(ClientRegisteredMsg)
	_ ServerMsg = new(ClientRegistrationFailedMsg)
)

var _ error = new(ClientRegistrationFailedMsg)

type (
	// ClientRegistrationMsg client event indicating that the client wants to register.
	ClientRegistrationMsg struct {
		ClientID      revent.ClientID
		QueryHandlers []revent.QueryID
	}

	// ClientRegisteredMsg r-event message indicating that the client has been successfully registered.
	ClientRegisteredMsg struct {
		ClientID revent.ClientID
	}

	// ClientRegistrationFailedMsg r-event message indicating that
	// the client could not be registered.
	//nolint:errname // keep consistency with Msg at the end
	ClientRegistrationFailedMsg struct {
		ClientID revent.ClientID
		Reason   string
	}
)

func (e ClientRegistrationFailedMsg) Error() string {
	if e.Reason == "" {
		return fmt.Sprintf("client %q registration rejected", e.ClientID)
	}

	return fmt.Sprintf("client %q registration rejected: %s", e.ClientID, e.Reason)
}

func (c ClientRegistrationMsg) clientMessage()       {}
func (c ClientRegisteredMsg) serverMessage()         {}
func (c ClientRegistrationFailedMsg) serverMessage() {}
