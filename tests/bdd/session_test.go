package bdd

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cucumber/godog"
	"github.com/google/uuid"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	reventsdkgo "github.com/manuelarte/revent-sdk-go"
	reventv1 "github.com/manuelarte/revent-sdk-go/internal/api/gRPC/revent/v1"
)

const (
	reventImage = "ghcr.io/manuelarte/revent:v0.0.1"
)

type reventServer struct {
	container testcontainers.Container
	host      string
	grpcPort  int
	restPort  int
}

//nolint:gochecknoglobals // to be refactor later
var sharedServer *reventServer

type scenarioState struct {
	server *reventServer

	cfg      reventsdkgo.Config
	state    *reventsdkgo.State
	clientID string
	subID    uuid.UUID

	registrationCh chan *reventv1.ServerToClientMessage
	sessionErrCh   chan error
	lastSessionErr error
	//nolint:containedctx // to be refactor later
	sessionCtx    context.Context
	sessionCancel context.CancelFunc
}

func TestFeatures(t *testing.T) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping BDD integration tests in short mode")
	}

	testSuite := godog.TestSuite{
		Name:                 "revent-sdk-go-bdd",
		ScenarioInitializer:  InitializeScenario,
		TestSuiteInitializer: InitializeTestSuite,
		Options: &godog.Options{
			Format:   "pretty",
			Paths:    []string{"."},
			Strict:   true,
			TestingT: t,
		},
	}

	if testSuite.Run() != 0 {
		t.Fatal("godog test suite failed")
	}
}

func InitializeTestSuite(ctx *godog.TestSuiteContext) {
	ctx.BeforeSuite(func() {
		server, err := startReventServer(context.Background())
		if err != nil {
			panic(fmt.Errorf("failed to start revent test container: %w", err))
		}

		sharedServer = server
	})

	ctx.AfterSuite(func() {
		if sharedServer == nil || sharedServer.container == nil {
			return
		}

		_ = sharedServer.container.Terminate(context.Background())
		sharedServer = nil
	})
}

func InitializeScenario(ctx *godog.ScenarioContext) {
	s := &scenarioState{}

	ctx.Before(func(context.Context, *godog.Scenario) (context.Context, error) {
		s.server = sharedServer
		s.sessionErrCh = make(chan error, 1)
		s.lastSessionErr = nil

		return context.Background(), nil
	})

	ctx.After(func(context.Context, *godog.Scenario, error) (context.Context, error) {
		if s.sessionCancel != nil {
			s.sessionCancel()
		}

		if s.sessionErrCh != nil {
			select {
			case <-s.sessionErrCh:
			default:
			}
		}

		if s.state != nil {
			_ = s.state.Unsubscribe(s.subID)
		}

		return context.Background(), nil
	})

	ctx.Step(
		`^the R-Event gRPC server is running in testcontainers$`,
		s.theREventServerIsRunning,
	)
	ctx.Step(
		`^a configured SDK state$`,
		s.aConfiguredSDKState,
	)
	ctx.Step(
		`^a configured SDK state with an unavailable gRPC endpoint$`,
		s.aConfiguredSDKStateWithUnavailableGRPCEndpoint,
	)
	ctx.Step(`^I open the SDK session$`, s.iOpenTheSDKSession)
	ctx.Step(`^I open the SDK session and wait for completion$`, s.iOpenTheSDKSessionAndWaitForCompletion)
	ctx.Step(`^the client should be registered by the server$`, s.theClientShouldBeRegisteredByTheServer)
	ctx.Step(`^I cancel the SDK session context$`, s.iCancelTheSDKSessionContext)
	ctx.Step(`^OpenSession should finish with context canceled$`, s.openSessionShouldFinishWithContextCanceled)
	ctx.Step(
		`^OpenSession should fail with CantConnectToServerError$`,
		s.openSessionShouldFailWithCantConnectToServerError,
	)
}

func startReventServer(ctx context.Context) (*reventServer, error) {
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
		return nil, err
	}

	host, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(ctx)

		return nil, err
	}

	grpcPort, err := container.MappedPort(ctx, "10000/tcp")
	if err != nil {
		_ = container.Terminate(ctx)

		return nil, err
	}

	restPort, err := container.MappedPort(ctx, "10001/tcp")
	if err != nil {
		_ = container.Terminate(ctx)

		return nil, err
	}

	return &reventServer{
		container: container,
		host:      host,
		grpcPort:  int(grpcPort.Num()),
		restPort:  int(restPort.Num()),
	}, nil
}

func (s *scenarioState) theREventServerIsRunning() error {
	if s.server == nil {
		return errors.New("revent test container is not running")
	}

	return nil
}

func (s *scenarioState) aConfiguredSDKState() error {
	cfg := reventsdkgo.DefaultConfig()
	cfg.ClientID = reventsdkgo.ClientID("bdd-" + uuid.NewString())
	cfg.ServerURL = s.server.host
	cfg.ServerGRPCPort = s.server.grpcPort
	cfg.ServerRestPort = s.server.restPort
	cfg.NumberOfRetries = 3

	return s.configureState(cfg, true)
}

func (s *scenarioState) aConfiguredSDKStateWithUnavailableGRPCEndpoint() error {
	cfg := reventsdkgo.DefaultConfig()
	cfg.ClientID = reventsdkgo.ClientID("bdd-unavailable-" + uuid.NewString())
	cfg.ServerURL = "127.0.0.1"
	cfg.ServerGRPCPort = 65535
	cfg.ServerRestPort = 10001
	cfg.NumberOfRetries = 1

	return s.configureState(cfg, false)
}

func (s *scenarioState) configureState(cfg reventsdkgo.Config, subscribeRegistration bool) error {
	state, err := reventsdkgo.NewState(cfg)
	if err != nil {
		return fmt.Errorf("failed to create state: %w", err)
	}

	s.cfg = cfg
	s.state = state
	s.clientID = cfg.ClientID.String()
	s.subID = uuid.New()
	s.registrationCh = make(chan *reventv1.ServerToClientMessage, 1)

	if !subscribeRegistration {
		return nil
	}

	return s.state.Subscribe(s.subID, func(msg *reventv1.ServerToClientMessage) bool {
		if msg == nil {
			return false
		}

		registered, ok := msg.GetPayload().(*reventv1.ServerToClientMessage_ClientRegistered)
		if !ok {
			return false
		}

		return registered.ClientRegistered.GetClientId() == s.clientID
	}, s.registrationCh)
}

func (s *scenarioState) iOpenTheSDKSession() error {
	s.sessionCtx, s.sessionCancel = context.WithCancel(context.Background())

	go func() {
		s.sessionErrCh <- reventsdkgo.OpenSession(s.sessionCtx, s.state)
	}()

	select {
	case err := <-s.sessionErrCh:
		if err == nil {
			return errors.New("session finished unexpectedly")
		}

		s.sessionErrCh <- err

		return fmt.Errorf("OpenSession returned early: %w", err)
	case <-time.After(300 * time.Millisecond):
		return nil
	}
}

func (s *scenarioState) iOpenTheSDKSessionAndWaitForCompletion() error {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	s.lastSessionErr = reventsdkgo.OpenSession(ctx, s.state)

	if s.lastSessionErr == nil {
		return errors.New("expected OpenSession to fail but it returned nil")
	}

	return nil
}

func (s *scenarioState) theClientShouldBeRegisteredByTheServer() error {
	select {
	case msg := <-s.registrationCh:
		registered := msg.GetClientRegistered()
		if registered == nil {
			return fmt.Errorf("expected client_registered message, got %T", msg.GetPayload())
		}

		if registered.GetClientId() != s.clientID {
			return fmt.Errorf("unexpected client id: got %q want %q", registered.GetClientId(), s.clientID)
		}

		return nil
	case err := <-s.sessionErrCh:
		if err == nil {
			return errors.New("OpenSession finished before registration")
		}

		return fmt.Errorf("OpenSession failed before registration: %w", err)
	case <-time.After(20 * time.Second):
		return errors.New("timeout waiting for registration confirmation")
	}
}

func (s *scenarioState) iCancelTheSDKSessionContext() error {
	if s.sessionCancel == nil {
		return errors.New("session was not started")
	}

	s.sessionCancel()

	return nil
}

func (s *scenarioState) openSessionShouldFinishWithContextCanceled() error {
	select {
	case err := <-s.sessionErrCh:
		if !errors.Is(err, context.Canceled) {
			return fmt.Errorf("unexpected OpenSession error: got %w, want %w", err, context.Canceled)
		}

		return nil
	case <-time.After(10 * time.Second):
		return errors.New("timeout waiting for OpenSession to stop after cancel")
	}
}

func (s *scenarioState) openSessionShouldFailWithCantConnectToServerError() error {
	if s.lastSessionErr == nil {
		return errors.New("OpenSession error was not captured")
	}

	var connectErr reventsdkgo.CantConnectToServerError
	if !errors.As(s.lastSessionErr, &connectErr) {
		return fmt.Errorf("expected CantConnectToServerError, got: %w", s.lastSessionErr)
	}

	if connectErr.NumAttempts != int(s.cfg.NumberOfRetries) {
		return fmt.Errorf(
			"unexpected retries in error: got %d want %d",
			connectErr.NumAttempts,
			int(s.cfg.NumberOfRetries),
		)
	}

	return nil
}
