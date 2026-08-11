package revent_sdk_go

import (
	reventv1 "github.com/manuelarte/revent-sdk-go/internal/api/gRPC/revent/v1"
	"github.com/manuelarte/revent-sdk-go/internal/txrx"
)

var _ TxRx = new(txrx.GRPC)

type (
	TxRx interface {
		// Send sends a message to the server.
		Send(m *reventv1.ClientToServerMessage) error
	}
)
