package txrx

import (
	reventv1 "github.com/manuelarte/revent-sdk-go/internal/api/gRPC/revent/v1"
	"github.com/manuelarte/revent-sdk-go/revent"
)

type UnknownMessageError struct {
	msg revent.Message
}

func (e *UnknownMessageError) Error() string {
	return "do not know how to transform msg"
}

func TransformClientMessageToGRPC(msg revent.ClientMessage) (*reventv1.ClientToServerMessage, error) {
	switch msg := msg.(type) {
	case *revent.ClientRegistrationMessage:
		return &reventv1.ClientToServerMessage{
			Payload: &reventv1.ClientToServerMessage_RegisterClient{
				RegisterClient: &reventv1.RegisterClient{
					ClientId:      msg.ClientID.String(),
					QueryHandlers: getQueryHandlerIDs(msg.QueryHandlers),
				},
			},
		}, nil
	}

	return nil, &UnknownMessageError{
		msg: msg,
	}
}

func getQueryHandlerIDs(queries []revent.QueryID) []string {
	queryHandlers := make([]string, 0, len(queries))
	for _, queryID := range queries {
		queryHandlers = append(queryHandlers, string(queryID))
	}

	return queryHandlers
}
