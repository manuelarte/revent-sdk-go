package actions

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/manuelarte/revent-sdk-go/logger"
	"github.com/manuelarte/revent-sdk-go/revent"
	"github.com/manuelarte/revent-sdk-go/revent/messages"
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
		Msg *messages.ClientRegisteredMsg
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

// Do send a ClientRegistrationMsg, waits for the ClientRegisteredMsg, and returns it.
// Output:
// It returns the output of the client registration response, that it could be:
// - revent.ClientRegisteredMsg
// - revent.ClientRegistrationErrorMsg
// Errors:
// - error coming from trying to send the ClientRegistrationMsg.
// - context error: if the context is canceled.
// - UnexpectedMsgError: if the received message is not expected.
func (c *ClientRegistration) Do(
	ctx context.Context,
	params ClientRegistrationParams,
) (*ClientRegistrationResponse, error) {
	subscriptionID := uuid.New()
	registrationEvents := make(chan messages.ServerMsg, 1)

	c.m.Subscribe(subscriptionID, func(msg messages.ServerMsg) bool {
		if msg == nil {
			return false
		}

		switch payload := msg.(type) {
		case *messages.ClientRegisteredMsg:
			return payload.ClientID == params.ClientID
		default:
			return false
		}
	}, registrationEvents)

	defer func() {
		_ = c.m.Unsubscribe(subscriptionID)
	}()

	err := c.m.Send(&messages.ClientRegistrationMsg{
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
		case *messages.ClientRegisteredMsg:
			return &ClientRegistrationResponse{
				Msg: payload,
				Err: nil,
			}, nil
		case *messages.ClientRegistrationErrorMsg:
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
