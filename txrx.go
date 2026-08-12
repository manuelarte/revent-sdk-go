package revent_sdk_go

import (
	"github.com/manuelarte/revent-sdk-go/internal/txrx"
	"github.com/manuelarte/revent-sdk-go/revent"
)

var _ TxRx = new(txrx.GRPC)

type (
	TxRx interface {
		// Send sends a message to the server.
		Send(m revent.ClientMessage) error

		// Incoming emits server messages received by the transport.
		//
		// For the gRPC transport, queue size and overflow behavior are configured
		// through GrpcConfig.IncomingBufferSize and GrpcConfig.IncomingOverflowPolicy.
		// Supported overflow policies are: drop-new, block, and drop-oldest.
		Incoming() <-chan revent.ServerMessage
	}
)
