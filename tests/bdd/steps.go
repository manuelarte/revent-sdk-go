package bdd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	grpcbackoff "google.golang.org/grpc/backoff"

	reventsdkgo "github.com/manuelarte/revent-sdk-go"
	reventv1 "github.com/manuelarte/revent-sdk-go/internal/api/gRPC/revent/v1"
)

const (
	reventImage = "ghcr.io/manuelarte/revent:v0.0.1"
)

type (
	serverInfo struct {
		container testcontainers.Container
		host      string
		grpcPort  int
		restPort  int
	}
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
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        reventImage,
			ExposedPorts: []string{"10000/tcp", "10001/tcp"},
			WaitingFor: wait.ForAll(
				wait.ForListeningPort("10000/tcp"),
				wait.ForListeningPort("10001/tcp"),
			).WithDeadline(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		return ctx, fmt.Errorf("error starting container: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(ctx)

		return ctx, fmt.Errorf("error hosting container: %w", err)
	}

	grpcPort, err := container.MappedPort(ctx, "10000/tcp")
	if err != nil {
		_ = container.Terminate(ctx)

		return ctx, fmt.Errorf("error mapping gRPC port: %w", err)
	}

	restPort, err := container.MappedPort(ctx, "10001/tcp")
	if err != nil {
		_ = container.Terminate(ctx)

		return ctx, fmt.Errorf("error mapping REST port: %w", err)
	}

	s.serverInfo = &serverInfo{
		container: container,
		host:      host,
		grpcPort:  int(grpcPort.Num()),
		restPort:  int(restPort.Num()),
	}

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
			return ctx, fmt.Errorf("expected OpenSession to finish with context canceled, got: %v", err)
		}

		return ctx, nil
	case <-time.After(10 * time.Second):
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
			return ctx, fmt.Errorf("expected CantConnectToServerError, got: %v", err)
		}

		return ctx, nil
	case <-time.After(5 * time.Second):
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
	case <-time.After(10 * time.Second):
		return ctx, errors.New("timeout waiting for registration confirmation")
	}
}
