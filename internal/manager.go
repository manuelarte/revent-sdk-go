package internal

import (
	"github.com/google/uuid"

	"github.com/manuelarte/revent-sdk-go/revent"
)

type (
	// SubscriptionManager manages the subscriptions of processes that listen to the different messages.
	// It is used to subscribe and unsubscribe from the different messages.
	SubscriptionManager interface {
		Subscribe(
			id uuid.UUID,
			pred func(msg revent.ServerMsg) bool,
			ch chan<- revent.ServerMsg,
		) error
		// Unsubscribe unsubscribes from a stream based on the given ID.
		Unsubscribe(id uuid.UUID) error
	}
)
