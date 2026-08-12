package internal

import (
	"context"

	"github.com/google/uuid"

	"github.com/manuelarte/revent-sdk-go/revent"
)

type ClientManager interface {
	// Subscribe subscribes to a stream of messages based on the given predicate.
	Subscribe(
		id uuid.UUID,
		pred func(msg revent.ServerMessage) bool,
		ch chan<- revent.ServerMessage,
	) error
	// Unsubscribe unsubscribes from a stream based on the given ID.
	Unsubscribe(id uuid.UUID) error
	Send(m revent.ClientMessage) error
	// Recv received a message from the server.
	Recv(msg revent.ServerMessage)
	RegisterClient(ctx context.Context) error
	QueryRequest(requestID revent.RequestID, queryID revent.QueryID) error
}
