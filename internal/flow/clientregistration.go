package flow

import (
	"context"
	"fmt"
	"time"

	reventv1 "github.com/manuelarte/revent-sdk-go/internal/api/gRPC/revent/v1"
)

type Manager interface {
	Send(msg *reventv1.ClientToServerMessage) error
}

type ClientRegistration struct {
	clientID string
	timeout  time.Duration
}

func NewClientRegistration(clientID string) *ClientRegistration {
	return &ClientRegistration{
		clientID: clientID,
		timeout:  2 * time.Second,
	}
}

type ClientRegistrationResponse struct {
	Err error
}

func (c *ClientRegistration) Do(_ context.Context, m Manager) error {
	err := m.Send(&reventv1.ClientToServerMessage{
		Payload: &reventv1.ClientToServerMessage_RegisterClient{
			RegisterClient: &reventv1.RegisterClient{ClientId: c.clientID},
		},
	})
	if err != nil {
		return fmt.Errorf("error registering client: %w", err)
	}

	return nil
}
