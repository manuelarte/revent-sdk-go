package revent

import "fmt"

var (
	_ ClientMessage = new(ClientRegistrationMessage)
	_ ServerMessage = new(ClientRegisteredMessage)
)

var _ error = new(ClientRegistrationError)

type (
	Message any

	ClientMessage interface {
		Message
		clientMessage()
	}

	ClientRegistrationMessage struct {
		ClientID      ClientID
		QueryHandlers []QueryID
	}

	ServerMessage interface {
		Message
		serverMessage()
	}

	ClientRegisteredMessage struct {
		ClientID ClientID
	}

	ClientRegistrationError struct {
		ClientID ClientID
		Reason   string
	}
)

func (e ClientRegistrationError) Error() string {
	if e.Reason == "" {
		return fmt.Sprintf("client %q registration rejected", e.ClientID)
	}

	return fmt.Sprintf("client %q registration rejected: %s", e.ClientID, e.Reason)
}

func (c ClientRegistrationMessage) clientMessage() {}
func (c ClientRegisteredMessage) serverMessage()   {}
func (e ClientRegistrationError) serverMessage()   {}
