package txrx

import (
	"github.com/manuelarte/revent-sdk-go/revent"
)

var _ TxRx = new(GRPC)

type (
	TxRx interface {
		// Send sends a message to the server.
		Send(m revent.ClientMsg) error

		// Incoming emits server messages received by the transport.
		//
		// The channel guarantees no message will be silently dropped. The stream
		// listener blocks when the queue is full, propagating backpressure to the
		// receiver goroutine. If the consumer cannot keep up with message rate,
		// the entire receiver will block; if it deadlocks, the app fails and the
		// server will resend the message on reconnect.
		//
		// For the gRPC transport, queue size is configured via
		// GrpcConfig.IncomingBufferSize (defaults to 64).
		Incoming() <-chan revent.ServerMsg
	}
)
