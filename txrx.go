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
	}
)
