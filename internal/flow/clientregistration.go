package flow

import (
	"context"
	"errors"
	"fmt"
	"time"

	reventv1 "github.com/manuelarte/revent-sdk-go/internal/api/gRPC/revent/v1"
	"github.com/manuelarte/revent-sdk-go/logger"
)

type Manager interface {
	Send(msg *reventv1.ClientToServerMessage) error
	WaitForClientRegistration(ctx context.Context) (ClientRegistrationResponse, error)
}

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

//go:structinit
type ClientRegistrationResponse struct {
	ClientID string
	Err      error
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

func (c *ClientRegistration) Do(ctx context.Context, m Manager) error {
	err := m.Send(&reventv1.ClientToServerMessage{
		Payload: &reventv1.ClientToServerMessage_RegisterClient{
			RegisterClient: &reventv1.RegisterClient{ClientId: c.clientID},
		},
	})
	if err != nil {
		return fmt.Errorf("error registering client: %w", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	response, err := m.WaitForClientRegistration(waitCtx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("client registration timeout after %s: %w", c.timeout, err)
		}

		return fmt.Errorf("error waiting for client registration: %w", err)
	}

	if response.Err != nil {
		return fmt.Errorf("error registering client: %w", response.Err)
	}

	if response.ClientID != "" && response.ClientID != c.clientID {
		return fmt.Errorf("unexpected client registration response for %q", response.ClientID)
	}

	c.logger.Info("Client registered successfully", "clientID", c.clientID)

	return nil
}
