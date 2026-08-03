package revent_sdk_go

import (
	"context"

	"google.golang.org/grpc"

	reventv1 "github.com/manuelarte/revent-sdk-go/internal/api/gRPC/revent/v1"
)

var _ TxRx = new(gRPCTxRx)

type (
	TxRx interface {
		Recv(ctx context.Context) (*reventv1.ServerToClientMessage, error)
		RegisterClient(clientID string, queryHandlers []string) error
	}

	gRPCTxRx struct {
		stream grpc.BidiStreamingClient[reventv1.ClientToServerMessage, reventv1.ServerToClientMessage]
	}
)

func (g *gRPCTxRx) Recv(_ context.Context) (*reventv1.ServerToClientMessage, error) {
	return g.stream.Recv()
}

func (g *gRPCTxRx) RegisterClient(clientID string, queryHandlers []string) error {
	return g.stream.Send(&reventv1.ClientToServerMessage{
		Payload: &reventv1.ClientToServerMessage_RegisterClient{
			RegisterClient: &reventv1.RegisterClient{
				ClientId:      clientID,
				QueryHandlers: queryHandlers,
			},
		},
	})
}
