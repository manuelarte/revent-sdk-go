package internal

import (
	"github.com/google/uuid"

	reventv1 "github.com/manuelarte/revent-sdk-go/internal/api/gRPC/revent/v1"
	"github.com/manuelarte/revent-sdk-go/revent"
)

type Manager interface {
	// Subscribe subscribes to a stream of messages based on the given predicate.
	Subscribe(
		id uuid.UUID,
		pred func(msg *reventv1.ServerToClientMessage) bool,
		ch chan<- *reventv1.ServerToClientMessage,
	) error
	// Unsubscribe unsubscribes from a stream based on the given ID.
	Unsubscribe(id uuid.UUID) error
	// Recv received a message from the server.
	Recv(msg *reventv1.ServerToClientMessage)
	RegisterClient() error
	QueryRequest(requestID revent.RequestID, queryID revent.QueryID) error
}
