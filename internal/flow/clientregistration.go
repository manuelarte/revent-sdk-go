package flow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/manuelarte/revent-sdk-go/internal"
	reventv1 "github.com/manuelarte/revent-sdk-go/internal/api/gRPC/revent/v1"
	"github.com/manuelarte/revent-sdk-go/logger"
)

type ClientRegistration struct {
	logger   logger.ILogger
	clientID string
	timeout  time.Duration
}

func NewClientRegistration(logger logger.ILogger, clientID string) *ClientRegistration {
	return &ClientRegistration{
		logger:   logger,
		clientID: clientID,
		timeout:  2 * time.Second,
	}
}

type ClientRegistrationRejectedError struct {
	ClientID string
	Reason   string
}

func (e ClientRegistrationRejectedError) Error() string {
	if e.Reason == "" {
		return fmt.Sprintf("client %q registration rejected", e.ClientID)
	}

	return fmt.Sprintf("client %q registration rejected: %s", e.ClientID, e.Reason)
}

func (c *ClientRegistration) Do(ctx context.Context, m internal.Manager) error {
	subscriptionID := uuid.New()
	registrationEvents := make(chan *reventv1.ServerToClientMessage, 1)

	err := m.Subscribe(subscriptionID, func(msg *reventv1.ServerToClientMessage) bool {
		if msg == nil {
			return false
		}

		switch payload := msg.GetPayload().(type) {
		case *reventv1.ServerToClientMessage_ClientRegistered:
			return payload.ClientRegistered.GetClientId() == c.clientID
		case *reventv1.ServerToClientMessage_ClientRegistrationError:
			return payload.ClientRegistrationError.GetClientId() == c.clientID
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

	err = m.RegisterClient(c.clientID)
	if err != nil {
		return fmt.Errorf("error registering client: %w", err)
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
		switch payload := msg.GetPayload().(type) {
		case *reventv1.ServerToClientMessage_ClientRegistered:
			c.logger.Info("Client registered successfully", "clientID", c.clientID)

			return nil
		case *reventv1.ServerToClientMessage_ClientRegistrationError:
			return fmt.Errorf("error registering client: %w", ClientRegistrationRejectedError{
				ClientID: payload.ClientRegistrationError.GetClientId(),
				Reason:   payload.ClientRegistrationError.GetReason(),
			})
		default:
			return fmt.Errorf("unexpected registration message: %T", msg.GetPayload())
		}
	}
}
