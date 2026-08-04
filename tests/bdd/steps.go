//nolint:mnd // magic numbers related to timeouts.
package bdd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	grpcbackoff "google.golang.org/grpc/backoff"

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

	return ctx, nil
}

func (s *scenarioState) aSDKState(ctx context.Context) (context.Context, error) {
	cfg := reventsdkgo.DefaultConfig()
	cfg.ClientID = reventsdkgo.ClientID("bdd-" + uuid.NewString())
	cfg.NumberOfRetries = 3
	cfg.BackoffCfg = grpcbackoff.Config{
		BaseDelay:  10 * time.Millisecond,
		Multiplier: 1,
		Jitter:     0,
		MaxDelay:   25 * time.Millisecond,
	}

	if s.serverInfo != nil {
		cfg.ServerURL = s.serverInfo.host
		cfg.ServerGRPCPort = s.serverInfo.grpcPort
		cfg.ServerRestPort = s.serverInfo.restPort
	} else {
		cfg.ServerURL = "127.0.0.1"
		cfg.ServerGRPCPort = 65535
		cfg.NumberOfRetries = 1
	}

	state, err := reventsdkgo.NewState(cfg)
	if err != nil {
		return ctx, fmt.Errorf("failed to create state: %w", err)
	}

	s.cfg = cfg

	s.state = state
	if errSubscribing := state.Subscribe(s.subID, func(msg *reventv1.ServerToClientMessage) bool {
		return true
	}, s.registrationCh); errSubscribing != nil {
		return ctx, fmt.Errorf("failed to subscribe: %w", errSubscribing)
	}

	return ctx, nil
}

func (s *scenarioState) iOpenTheSDKSession(ctx context.Context) (context.Context, error) {
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

func (s *scenarioState) theServerRestarts(ctx context.Context) (context.Context, error) {
	if s.serverInfo == nil {
		return ctx, errors.New("server did not start")
	}

	if err := s.serverInfo.restart(ctx); err != nil {
		return ctx, fmt.Errorf("error restarting server: %w", err)
	}

	// CRITICAL: Update the SDK config with new server address
	// This is necessary because the port mappings changed
	newCfg := reventsdkgo.DefaultConfig()
	newCfg.ClientID = s.cfg.ClientID
	newCfg.ServerURL = s.serverInfo.host
	newCfg.ServerGRPCPort = s.serverInfo.grpcPort
	newCfg.ServerRestPort = s.serverInfo.restPort
	newCfg.NumberOfRetries = s.cfg.NumberOfRetries
	newCfg.BackoffCfg = s.cfg.BackoffCfg

	// Create a new state with updated config and re-subscribe
	newState, err := reventsdkgo.NewState(newCfg)
	if err != nil {
		_ = s.serverInfo.container.Terminate(ctx)
		return ctx, fmt.Errorf("error creating new state with updated config: %w", err)
	}

	if err := newState.Subscribe(s.subID, func(msg *reventv1.ServerToClientMessage) bool {
		return true
	}, s.registrationCh); err != nil {
		_ = s.serverInfo.container.Terminate(ctx)
		return ctx, fmt.Errorf("error subscribing to new state: %w", err)
	}

	// Update scenario state with the new state
	s.cfg = newCfg
	s.state = newState

	// Cancel the old session and start a new one with the new state
	if s.openSessionCancel != nil {
		s.openSessionCancel()
		s.openSessionCancel = nil
	}

	// Open a new session with the updated state
	newSessionCtx, newSessionCancel := context.WithCancel(ctx)
	s.openSessionCancel = newSessionCancel

	go func() {
		// Don't send error to channel, as we want to wait for re-registration
		_ = reventsdkgo.OpenSession(newSessionCtx, s.state)
	}()

	return ctx, nil
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
