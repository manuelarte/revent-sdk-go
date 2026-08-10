package internal

import (
	"github.com/google/uuid"

	reventv1 "github.com/manuelarte/revent-sdk-go/internal/api/gRPC/revent/v1"
	"github.com/manuelarte/revent-sdk-go/revent"
)

type Manager interface {
	Subscribe(
		id uuid.UUID,
		pred func(msg *reventv1.ServerToClientMessage) bool,
		ch chan<- *reventv1.ServerToClientMessage,
	) error
	Unsubscribe(id uuid.UUID) error
	RegisterClient(clientID string) error
	QueryRequest(requestID revent.RequestID, queryID revent.QueryID) error
}
