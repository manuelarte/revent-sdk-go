package txrx

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	reventv1 "github.com/manuelarte/revent-sdk-go/internal/api/gRPC/revent/v1"
	"github.com/manuelarte/revent-sdk-go/internal/revent/messages"
	"github.com/manuelarte/revent-sdk-go/revent"
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
		var paramsMap map[string]string

		if msg.Parameters != nil {
			bytes, err := json.Marshal(msg.Parameters)
			if err == nil {
				var rawMap map[string]any
				if errUnmarshal := json.Unmarshal(bytes, &rawMap); errUnmarshal == nil {
					paramsMap = make(map[string]string, len(rawMap))
					for k, v := range rawMap {
						paramsMap[k] = fmt.Sprint(v)
					}
				}
			}
		}

		return &reventv1.ClientToServerMessage{
			Payload: &reventv1.ClientToServerMessage_QueryRequest{
				QueryRequest: &reventv1.QueryRequest{
					RequestId:  msg.RequestID.String(),
					QueryId:    msg.QueryID.String(),
					Parameters: paramsMap,
				},
			},
		}, nil
	case *messages.QueryResponseRawMsg:
		return &reventv1.ClientToServerMessage{
			Payload: &reventv1.ClientToServerMessage_QueryResponse{
				QueryResponse: &reventv1.QueryResponse{
					RequestId: msg.RequestID.String(),
					Result:    msg.Response,
				},
			},
		}, nil
	case *messages.QueryHandlingErrorMsg:
		var reason reventv1.QueryHandlingErrorReason
		switch msg.Reason {
		case string(messages.QueryRequestedErrorReasonErrorHandling):
			reason = reventv1.QueryHandlingErrorReason(1)
		default:
			reason = reventv1.QueryHandlingErrorReason(0)
		}
		return &reventv1.ClientToServerMessage{Payload: &reventv1.ClientToServerMessage_QueryHandlingError{
			QueryHandlingError: &reventv1.QueryHandlingError{
				RequestId: msg.RequestID.String(),
				Reason:    reason,
				Details:   msg.Details,
			},
		}}, nil
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
	case *reventv1.ServerToClientMessage_ClientRegistrationError:
		return &messages.ClientRegistrationErrorMsg{
			ClientID: revent.ClientID(casted.ClientRegistrationError.GetClientId()),
			Reason:   casted.ClientRegistrationError.GetReason(),
		}, nil
	case *reventv1.ServerToClientMessage_QueryRequested:
		return &messages.QueryRequestedMsg{
			RequestID:  revent.RequestID(uuid.MustParse(casted.QueryRequested.GetRequestId())),
			QueryID:    revent.QueryID(casted.QueryRequested.GetQueryId()),
			Parameters: casted.QueryRequested.GetParameters(),
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
