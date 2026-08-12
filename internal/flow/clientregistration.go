package flow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/manuelarte/revent-sdk-go/logger"
	"github.com/manuelarte/revent-sdk-go/revent"
)

type ClientRegistration struct {
	logger        logger.ILogger
	clientID      revent.ClientID
	queryHandlers []revent.QueryID
	timeout       time.Duration
}

func NewClientRegistration(
	logger logger.ILogger,
	clientID revent.ClientID,
	queryHandlers []revent.QueryID,
) *ClientRegistration {
	return &ClientRegistration{
		logger:        logger,
		clientID:      clientID,
		queryHandlers: queryHandlers,
		timeout:       2 * time.Second,
	}
}

func (c *ClientRegistration) Do(ctx context.Context, m SendAndSubscribe) error {
	subscriptionID := uuid.New()
	registrationEvents := make(chan revent.ServerMessage, 1)

	err := m.Subscribe(subscriptionID, func(msg revent.ServerMessage) bool {
		if msg == nil {
			return false
		}

		switch payload := msg.(type) {
		case *revent.ClientRegisteredMessage:
			return payload.ClientID == c.clientID
		default:
			return false
		}
	}, registrationEvents)
	if err != nil {
		return fmt.Errorf("error creating registration listener: %w", err)
	}

	defer func() {
		_ = m.Unsubscribe(subscriptionID)
	}()

	err = m.Send(&revent.ClientRegistrationMessage{
		ClientID:      c.clientID,
		QueryHandlers: c.queryHandlers,
	})
	if err != nil {
		return fmt.Errorf("error sending client registration: %w", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	select {
	case <-waitCtx.Done():
		if errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("client registration timeout after %s: %w", c.timeout, waitCtx.Err())
		}

		return fmt.Errorf("error waiting for client registration: %w", waitCtx.Err())
	case msg := <-registrationEvents:
		switch payload := msg.(type) {
		case *revent.ClientRegisteredMessage:
			return nil
		case *revent.ClientRegistrationError:
			return payload
		default:
			return UnexpectedMsgError{
				Flow: "ClientRegistration",
				Msg:  payload,
			}
		}
	}
}
