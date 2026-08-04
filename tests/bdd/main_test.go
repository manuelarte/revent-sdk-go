package bdd

import (
	"context"
	"testing"

	"github.com/cucumber/godog"
	"github.com/google/uuid"

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

	ctx.Before(func(context.Context, *godog.Scenario) (context.Context, error) {
		s.subID = uuid.New()
		s.registrationCh = make(chan *reventv1.ServerToClientMessage, 1)

		return context.Background(), nil
	})

	ctx.After(func(ctx context.Context, scenario *godog.Scenario, err error) (context.Context, error) {
		if s.state != nil {
			_ = s.state.Unsubscribe(s.subID)
		}

		return ctx, err
	})

	ctx.Step(
		`^the server is running$`,
		s.theServerIsRunning,
	)
	ctx.Step(
		`^a configured SDK state$`,
		s.aConfiguredSDKState,
	)
	ctx.Step(`^I open the SDK session$`, s.iOpenTheSDKSession)
	ctx.Step(`^the client should be registered by the server$`, s.theClientShouldBeRegisteredByTheServer)
	ctx.Step(`^I cancel the SDK session context$`, s.iCancelTheSDKSessionContext)
}
