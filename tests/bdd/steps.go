//nolint:mnd // magic numbers related to timeouts.
package bdd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	reventsdkgo "github.com/manuelarte/revent-sdk-go"
	"github.com/manuelarte/revent-sdk-go/internal/txrx"
	"github.com/manuelarte/revent-sdk-go/revent"
)

const (
	reventImage = "ghcr.io/manuelarte/revent:v0.0.1"
)

type scenarioState struct {
	serverInfo *serverInfo
	cfg        reventsdkgo.Config
	state      *reventsdkgo.ServerManager
	// we subscribe to every single message to do the checks later on.
	subID             uuid.UUID
	registrationCh    chan revent.ServerMsg
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

func (s *scenarioState) theServerRestarts(ctx context.Context) (context.Context, error) {
	err := s.serverInfo.restartServer(ctx)
	if err != nil {
		return ctx, fmt.Errorf("error restarting server: %w", err)
	}

	return ctx, nil
}

func (s *scenarioState) iOpenTheSDKSession(ctx context.Context) (context.Context, error) {
	state, err := reventsdkgo.NewState(s.cfg)
	if err != nil {
		return ctx, fmt.Errorf("failed to create state: %w", err)
	}

	s.state = state
	if errSubscribing := state.Subscribe(s.subID, func(msg revent.ServerMsg) bool {
		return true
	}, s.registrationCh); errSubscribing != nil {
		return ctx, fmt.Errorf("failed to subscribe: %w", errSubscribing)
	}

	newCancelCtx, cancel := context.WithCancel(ctx)
	s.openSessionCancel = cancel

	go func() {
		s.openSessionErrCh <- reventsdkgo.OpenSession(newCancelCtx, s.state, s.cfg.GRPCCfg)
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

		var connectErr txrx.CantConnectToServerError
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
		registered, ok := msg.(*revent.ClientRegisteredMsg)
		if !ok {
			return ctx, fmt.Errorf("expected ClientRegisteredMessage, got %T", msg)
		}

		if registered.ClientID.String() != s.cfg.ClientID.String() {
			return ctx, fmt.Errorf("unexpected client id: got %q want %q", registered.ClientID.String(), s.cfg.ClientID.String())
		}

		return ctx, nil
	case <-time.After(8 * time.Second):
		return ctx, errors.New("timeout waiting for registration confirmation")
	}
}
