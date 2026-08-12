package internal

import (
	"context"

	"github.com/google/uuid"

	"github.com/manuelarte/revent-sdk-go/revent"
)

type (
	SubscriptionManager interface {
		Subscribe(
			id uuid.UUID,
			pred func(msg revent.ServerMessage) bool,
			ch chan<- revent.ServerMessage,
		) error
		// Unsubscribe unsubscribes from a stream based on the given ID.
		Unsubscribe(id uuid.UUID) error
	}

	Sender interface {
		Send(msg revent.ClientMessage) error
	}

	ClientManager interface {
		SubscriptionManager
		RegisterClient(ctx context.Context) error
		QueryRequest(requestID revent.RequestID, queryID revent.QueryID) error
	}
)
