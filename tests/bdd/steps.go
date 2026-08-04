//nolint:mnd // magic numbers related to timeouts.
package bdd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	reventsdkgo "github.com/manuelarte/revent-sdk-go"
	reventv1 "github.com/manuelarte/revent-sdk-go/internal/api/gRPC/revent/v1"
)

const (
	reventImage = "ghcr.io/manuelarte/revent:v0.0.1"
)

type scenarioState struct {
	serverInfo *serverInfo
	cfg        reventsdkgo.Config
	state      *reventsdkgo.State
	// we subscribe to every single message to do the checks later on.
	subID             uuid.UUID
	registrationCh    chan *reventv1.ServerToClientMessage
	openSessionErrCh  chan error
	openSessionCancel context.CancelFunc
}

func (s *scenarioState) theServerIsRunning(ctx context.Context) (context.Context, error) {
	si, err := startServer(ctx)
	if err != nil {
		return ctx, fmt.Errorf("error starting server: %w", err)
	}

	s.serverInfo = si
	s.serverInfo.update(&s.cfg)

	return ctx, nil
}

func (s *scenarioState) iOpenTheSDKSession(ctx context.Context) (context.Context, error) {
	state, err := reventsdkgo.NewState(s.cfg)
	if err != nil {
		return ctx, fmt.Errorf("failed to create state: %w", err)
	}

	s.state = state
	if errSubscribing := state.Subscribe(s.subID, func(msg *reventv1.ServerToClientMessage) bool {
		return true
	}, s.registrationCh); errSubscribing != nil {
		return ctx, fmt.Errorf("failed to subscribe: %w", errSubscribing)
	}

	newCancelCtx, cancel := context.WithCancel(ctx)
	s.openSessionCancel = cancel

	go func() {
		s.openSessionErrCh <- reventsdkgo.OpenSession(newCancelCtx, s.state)
	}()

	return ctx, nil
}

func (s *scenarioState) openSessionShouldFinishWithContextCanceled(ctx context.Context) (context.Context, error) {
	select {
	case err := <-s.openSessionErrCh:
		s.openSessionCancel = nil

		if !errors.Is(err, context.Canceled) {
			return ctx, fmt.Errorf("expected OpenSession to finish with context canceled, got: %w", err)
		}

		return ctx, nil
	case <-time.After(2 * time.Second):
		return ctx, errors.New("timeout waiting for OpenSession to finish")
	}
}

func (s *scenarioState) sessionShouldFailWithCantConnectToServerError(ctx context.Context) (context.Context, error) {
	select {
	case err := <-s.openSessionErrCh:
		s.openSessionCancel = nil

		if err == nil {
			return ctx, errors.New("expected OpenSession to fail, got nil")
		}

		var connectErr reventsdkgo.CantConnectToServerError
		if !errors.As(err, &connectErr) {
			return ctx, fmt.Errorf("expected CantConnectToServerError, got: %w", err)
		}

		return ctx, nil
	case <-time.After(2 * time.Second):
		return ctx, errors.New("timeout waiting for OpenSession failure")
	}
}

func (s *scenarioState) iCancelTheSDKSessionContext(ctx context.Context) (context.Context, error) {
	if s.openSessionCancel != nil {
		s.openSessionCancel()
		s.openSessionCancel = nil
	}

	return ctx, nil
}

func (s *scenarioState) theClientShouldBeRegisteredByTheServer(ctx context.Context) (context.Context, error) {
	select {
	case msg := <-s.registrationCh:
		registered := msg.GetClientRegistered()
		if registered == nil {
			return ctx, fmt.Errorf("expected client_registered message, got %T", msg.GetPayload())
		}

		if registered.GetClientId() != s.cfg.ClientID.String() {
			return ctx, fmt.Errorf("unexpected client id: got %q want %q", registered.GetClientId(), s.cfg.ClientID.String())
		}

		return ctx, nil
	case <-time.After(8 * time.Second):
		return ctx, errors.New("timeout waiting for registration confirmation")
	}
}
