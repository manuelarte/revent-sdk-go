package txrx

import (
	"fmt"

	"github.com/google/uuid"

	reventv1 "github.com/manuelarte/revent-sdk-go/internal/api/gRPC/revent/v1"
	"github.com/manuelarte/revent-sdk-go/revent"
)

type UnknownMsgError struct {
	Msg revent.Msg
}

func (e *UnknownMsgError) Error() string {
	return fmt.Sprintf("do not know how to transform msg: %T", e.Msg)
}

func transformClientMessageToGRPC(msg revent.ClientMsg) (*reventv1.ClientToServerMessage, error) {
	switch msg := msg.(type) {
	case *revent.ClientRegistrationMsg:
		return &reventv1.ClientToServerMessage{
			Payload: &reventv1.ClientToServerMessage_RegisterClient{
				RegisterClient: &reventv1.RegisterClient{
					ClientId:      msg.ClientID.String(),
					QueryHandlers: getQueryHandlerIDs(msg.QueryHandlers),
				},
			},
		}, nil
	case *revent.QueryRequestMsg:
		return &reventv1.ClientToServerMessage{
			Payload: &reventv1.ClientToServerMessage_QueryRequest{
				QueryRequest: &reventv1.QueryRequest{
					RequestId:  msg.RequestID.String(),
					QueryId:    msg.QueryID.String(),
					Parameters: nil,
				},
			},
		}, nil
	}

	return nil, &UnknownMsgError{
		Msg: msg,
	}
}

func transformServerMessageToGRPC(msg *reventv1.ServerToClientMessage) (revent.ServerMsg, error) {
	switch casted := msg.GetPayload().(type) {
	case *reventv1.ServerToClientMessage_ClientRegistered:
		return transformToClientRegistered(casted), nil
	case *reventv1.ServerToClientMessage_QueryResponded:
		return transformToQueryResponse(casted), nil
	//nolint:nilnil // think about this later.
	case *reventv1.ServerToClientMessage_Heartbeat:
		return nil, nil
	}

	return nil, &UnknownMsgError{
		Msg: msg,
	}
}

func transformToClientRegistered(msg *reventv1.ServerToClientMessage_ClientRegistered) *revent.ClientRegisteredMsg {
	return &revent.ClientRegisteredMsg{
		ClientID: revent.ClientID(msg.ClientRegistered.GetClientId()),
	}
}

func transformToQueryResponse(msg *reventv1.ServerToClientMessage_QueryResponded) *revent.QueryResponseMsg {
	// TODO: this needs a lot of work
	return &revent.QueryResponseMsg{
		RequestID: revent.RequestID(uuid.MustParse(msg.QueryResponded.GetRequestId())),
		QueryID:   "made up",
		Response:  nil,
	}
}

func getQueryHandlerIDs(queries []revent.QueryID) []string {
	queryHandlers := make([]string, 0, len(queries))
	for _, queryID := range queries {
		queryHandlers = append(queryHandlers, string(queryID))
	}

	return queryHandlers
}
