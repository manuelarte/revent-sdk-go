package actions

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/manuelarte/revent-sdk-go/logger"
	"github.com/manuelarte/revent-sdk-go/revent"
)

type (
	ClientRegistration struct {
		logger logger.ILogger
		m      SendAndSubscribe
	}

	ClientRegistrationParams struct {
		ClientID      revent.ClientID
		QueryHandlers []revent.QueryID
	}

	ClientRegistrationResponse struct {
		Msg *revent.ClientRegisteredMsg
		Err error
	}
)

func NewClientRegistration(
	logger logger.ILogger,
	m SendAndSubscribe,
) *ClientRegistration {
	return &ClientRegistration{
		logger: logger,
		m:      m,
	}
}

func (c *ClientRegistration) Do(
	ctx context.Context,
	params ClientRegistrationParams,
) (*ClientRegistrationResponse, error) {
	subscriptionID := uuid.New()
	registrationEvents := make(chan revent.ServerMsg, 1)

	err := c.m.Subscribe(subscriptionID, func(msg revent.ServerMsg) bool {
		if msg == nil {
			return false
		}

		switch payload := msg.(type) {
		case *revent.ClientRegisteredMsg:
			return payload.ClientID == params.ClientID
		default:
			return false
		}
	}, registrationEvents)
	if err != nil {
		return nil, fmt.Errorf("error creating registration listener: %w", err)
	}

	defer func() {
		_ = c.m.Unsubscribe(subscriptionID)
	}()

	err = c.m.Send(&revent.ClientRegistrationMsg{
		ClientID:      params.ClientID,
		QueryHandlers: params.QueryHandlers,
	})
	if err != nil {
		return nil, fmt.Errorf("error sending client registration: %w", err)
	}

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("error waiting for client registration: %w", ctx.Err())
	case msg := <-registrationEvents:
		switch payload := msg.(type) {
		case *revent.ClientRegisteredMsg:
			return &ClientRegistrationResponse{
				Msg: payload,
				Err: nil,
			}, nil
		case *revent.ClientRegistrationErrorMsg:
			return &ClientRegistrationResponse{
				Msg: nil,
				Err: payload,
			}, nil
		default:
			return nil, UnexpectedMsgError{
				Flow: "ClientRegistration",
				Msg:  payload,
			}
		}
	}
}
