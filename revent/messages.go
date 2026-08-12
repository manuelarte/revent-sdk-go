package revent

var (
	_ ClientMessage = new(ClientRegistrationMessage)
	_ ServerMessage = new(ClientRegisteredMessage)
)

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

func (c ClientRegistrationMessage) clientMessage() {}
func (c ClientRegisteredMessage) serverMessage()   {}
func (c ClientRegistrationError) serverMessage()   {}
