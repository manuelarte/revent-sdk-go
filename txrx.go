package revent_sdk_go

import (
	"google.golang.org/grpc"

	reventv1 "github.com/manuelarte/revent-sdk-go/internal/api/gRPC/revent/v1"
)

var _ TxRx = new(gRPCTxRx)

type (
	TxRx interface {
		Send(msg *reventv1.ClientToServerMessage) error
		Recv() (*reventv1.ServerToClientMessage, error)
	}

	gRPCTxRx struct {
		stream grpc.BidiStreamingClient[reventv1.ClientToServerMessage, reventv1.ServerToClientMessage]
	}
)

func (g gRPCTxRx) Send(msg *reventv1.ClientToServerMessage) error {
	return g.stream.Send(msg)
}

func (g gRPCTxRx) Recv() (*reventv1.ServerToClientMessage, error) {
	return g.stream.Recv()
}
