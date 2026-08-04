package bdd

import (
	"context"
	"testing"

	"github.com/cucumber/godog"
	"github.com/google/uuid"

	reventsdkgo "github.com/manuelarte/revent-sdk-go"
	reventv1 "github.com/manuelarte/revent-sdk-go/internal/api/gRPC/revent/v1"
)

func TestFeatures(t *testing.T) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping BDD integration tests in short mode")
	}

	testSuite := godog.TestSuite{
		Name:                "revent-sdk-go-bdd",
		ScenarioInitializer: initializeScenario,
		Options: &godog.Options{
			Format:   "pretty",
			Paths:    []string{"features"},
			Strict:   true,
			TestingT: t,
		},
	}

	if testSuite.Run() != 0 {
		t.Fatal("godog test suite failed")
	}
}

func initializeScenario(ctx *godog.ScenarioContext) {
	s := &scenarioState{}

	ctx.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		s.serverInfo = nil
		s.cfg = reventsdkgo.Config{}
		s.state = nil
		s.openSessionCancel = nil
		s.subID = uuid.New()
		s.registrationCh = make(chan *reventv1.ServerToClientMessage, 1)
		s.openSessionErrCh = make(chan error, 1)

		return ctx, nil
	})

	ctx.After(func(ctx context.Context, scenario *godog.Scenario, err error) (context.Context, error) {
		if s.openSessionCancel != nil {
			s.openSessionCancel()
			s.openSessionCancel = nil
		}

		if s.state != nil {
			_ = s.state.Unsubscribe(s.subID)
		}

		if s.serverInfo != nil {
			_ = s.serverInfo.container.Terminate(ctx)
		}

		s.serverInfo = nil

		return ctx, err
	})

	ctx.Step(`^the server is running$`, s.theServerIsRunning)
	ctx.Step(`^a SDK state$`, s.aSDKState)
	ctx.Step(`^I open the SDK session$`, s.iOpenTheSDKSession)
	ctx.Step(`^the client should be registered by the server$`, s.theClientShouldBeRegisteredByTheServer)
	ctx.Step(`^I cancel the SDK session context$`, s.iCancelTheSDKSessionContext)
	ctx.Step(`^the session should finish with context canceled$`, s.openSessionShouldFinishWithContextCanceled)
	ctx.Step(`^the server restarts$`, s.theServerRestarts)
	ctx.Step(`^session should fail with CantConnectToServerError$`, s.sessionShouldFailWithCantConnectToServerError)
}
