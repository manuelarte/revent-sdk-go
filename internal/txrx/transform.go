package txrx

import (
	"fmt"

	reventv1 "github.com/manuelarte/revent-sdk-go/internal/api/gRPC/revent/v1"
	"github.com/manuelarte/revent-sdk-go/revent"
)

type UnknownMsgError struct {
	Msg revent.Message
}

func (e *UnknownMsgError) Error() string {
	return fmt.Sprintf("do not know how to transform msg: %T", e.Msg)
}

func transformClientMessageToGRPC(msg revent.ClientMessage) (*reventv1.ClientToServerMessage, error) {
	//nolint:gocritic // more events coming
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

	return nil, &UnknownMsgError{
		Msg: msg,
	}
}

func transformServerMessageToGRPC(msg *reventv1.ServerToClientMessage) (revent.ServerMessage, error) {
	switch casted := msg.Payload.(type) {
	case *reventv1.ServerToClientMessage_ClientRegistered:
		return transformToClientRegistered(casted), nil
	//nolint:nilnil // think about this later.
	case *reventv1.ServerToClientMessage_Heartbeat:
		return nil, nil
	}

	return nil, &UnknownMsgError{
		Msg: msg,
	}
}

func transformToClientRegistered(msg *reventv1.ServerToClientMessage_ClientRegistered) *revent.ClientRegisteredMessage {
	return &revent.ClientRegisteredMessage{
		ClientID: revent.ClientID(msg.ClientRegistered.ClientId),
	}
}

func getQueryHandlerIDs(queries []revent.QueryID) []string {
	queryHandlers := make([]string, 0, len(queries))
	for _, queryID := range queries {
		queryHandlers = append(queryHandlers, string(queryID))
	}

	return queryHandlers
}
