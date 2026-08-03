package revent_sdk_go

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	reventv1 "github.com/manuelarte/revent-sdk-go/internal/api/gRPC/revent/v1"
)

var _ TxRx = new(gRPCTxRx)

type (
	TxRxEvent interface {
		isTxRxEvent()
	}

	ClientRegisteredEvent struct {
		ClientID string
	}

	ClientRegistrationErrorEvent struct {
		ClientID string
		Reason   string
	}

	UnhandledServerMessageEvent struct {
		Message *reventv1.ServerToClientMessage
	}

	TxRx interface {
		Send(msg *reventv1.ClientToServerMessage) error
		NextEvent(ctx context.Context) (TxRxEvent, error)
		RegisterClient(clientID string, queryHandlers []string) error
	}

	gRPCTxRx struct {
		stream grpc.BidiStreamingClient[reventv1.ClientToServerMessage, reventv1.ServerToClientMessage]
	}
)

func (ClientRegisteredEvent) isTxRxEvent() {}

func (ClientRegistrationErrorEvent) isTxRxEvent() {}

func (UnhandledServerMessageEvent) isTxRxEvent() {}

func (g gRPCTxRx) Send(msg *reventv1.ClientToServerMessage) error {
	return g.stream.Send(msg)
}

func (g gRPCTxRx) NextEvent(_ context.Context) (TxRxEvent, error) {
	msg, err := g.stream.Recv()
	if err != nil {
		return nil, err
	}

	switch payload := msg.GetPayload().(type) {
	case *reventv1.ServerToClientMessage_ClientRegistered:
		return ClientRegisteredEvent{ClientID: payload.ClientRegistered.GetClientId()}, nil
	case *reventv1.ServerToClientMessage_ClientRegistrationError:
		return ClientRegistrationErrorEvent{
			ClientID: payload.ClientRegistrationError.GetClientId(),
			Reason:   payload.ClientRegistrationError.GetReason(),
		}, nil
	default:
		return UnhandledServerMessageEvent{Message: msg}, nil
	}
}

func (g gRPCTxRx) RegisterClient(clientID string, queryHandlers []string) error {
	return g.Send(&reventv1.ClientToServerMessage{
		Payload: &reventv1.ClientToServerMessage_RegisterClient{
			RegisterClient: &reventv1.RegisterClient{
				ClientId:      clientID,
				QueryHandlers: queryHandlers,
			},
		},
	})
}

func formatTxRxEvent(event TxRxEvent) string {
	if event == nil {
		return "<nil>"
	}

	switch e := event.(type) {
	case ClientRegisteredEvent:
		return fmt.Sprintf("ClientRegistered(clientID=%q)", e.ClientID)
	case ClientRegistrationErrorEvent:
		return fmt.Sprintf("ClientRegistrationError(clientID=%q)", e.ClientID)
	case UnhandledServerMessageEvent:
		if e.Message == nil {
			return "UnhandledServerMessage(<nil>)"
		}

		return fmt.Sprintf("UnhandledServerMessage(%T)", e.Message.GetPayload())
	default:
		return fmt.Sprintf("%T", event)
	}
}
