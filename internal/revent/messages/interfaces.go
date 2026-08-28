package messages

import "github.com/manuelarte/revent-sdk-go/revent"

type (
	Msg any

	IdempotentMsg interface {
		GetRequestID() revent.RequestID
	}

	ClientIdentifiable interface {
		ClientID() revent.ClientID
	}

	ClientMsg interface {
		Msg
		clientMessage()
	}

	ServerMsg interface {
		Msg
		serverMessage()
	}
)
