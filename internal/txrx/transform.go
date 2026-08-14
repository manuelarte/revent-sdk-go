package txrx

import (
	"fmt"

	"github.com/google/uuid"

	reventv1 "github.com/manuelarte/revent-sdk-go/internal/api/gRPC/revent/v1"
	"github.com/manuelarte/revent-sdk-go/revent"
	"github.com/manuelarte/revent-sdk-go/revent/messages"
)

type UnknownMsgError struct {
	Msg messages.Msg
}

func (e *UnknownMsgError) Error() string {
	return fmt.Sprintf("do not know how to transform msg: %T", e.Msg)
}

func transformClientMessageToGRPC(msg messages.ClientMsg) (*reventv1.ClientToServerMessage, error) {
	switch msg := msg.(type) {
	case *messages.ClientRegistrationMsg:
		return &reventv1.ClientToServerMessage{
			Payload: &reventv1.ClientToServerMessage_RegisterClient{
				RegisterClient: &reventv1.RegisterClient{
					ClientId:      msg.ClientID.String(),
					QueryHandlers: getQueryHandlerIDs(msg.QueryHandlers),
				},
			},
		}, nil
	case *messages.QueryRequestMsg:
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

func transformServerMessageToGRPC(msg *reventv1.ServerToClientMessage) (messages.ServerMsg, error) {
	switch casted := msg.GetPayload().(type) {
	case *reventv1.ServerToClientMessage_ClientRegistered:
		return &messages.ClientRegisteredMsg{
			ClientID: revent.ClientID(casted.ClientRegistered.GetClientId()),
		}, nil
	case *reventv1.ServerToClientMessage_QueryResponded:
		return &messages.QueryResponseRawMsg{
			RequestID: revent.RequestID(uuid.MustParse(casted.QueryResponded.GetRequestId())),
			Response:  casted.QueryResponded.GetResult(),
		}, nil
	case *reventv1.ServerToClientMessage_QueryRequestedError:
		return &messages.QueryRequestedErrorRawMsg{
			RequestID: revent.RequestID(uuid.MustParse(casted.QueryRequestedError.GetRequestId())),
			Reason:    messages.QueryRequestedErrorReason(casted.QueryRequestedError.GetReason()),
		}, nil
	//nolint:nilnil // think about this later.
	case *reventv1.ServerToClientMessage_Heartbeat:
		return nil, nil
	}

	return nil, &UnknownMsgError{
		Msg: msg.GetPayload(),
	}
}

func getQueryHandlerIDs(queries []revent.QueryID) []string {
	queryHandlers := make([]string, 0, len(queries))
	for _, queryID := range queries {
		queryHandlers = append(queryHandlers, string(queryID))
	}

	return queryHandlers
}
