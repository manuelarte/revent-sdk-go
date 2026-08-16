package bdd

import (
	"context"
	"testing"
	"time"

	"github.com/cucumber/godog"
	"github.com/google/uuid"
	grpcbackoff "google.golang.org/grpc/backoff"

	reventsdkgo "github.com/manuelarte/revent-sdk-go"
	"github.com/manuelarte/revent-sdk-go/revent"
	"github.com/manuelarte/revent-sdk-go/revent/messages"
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
		cfg := reventsdkgo.DefaultConfig()
		cfg.ClientID = revent.ClientID("bdd-" + uuid.NewString())
		cfg.GRPCCfg.NumberOfRetries = 3
		cfg.GRPCCfg.BackoffCfg = grpcbackoff.Config{
			BaseDelay:  10 * time.Millisecond,
			Multiplier: 1,
			Jitter:     0,
			MaxDelay:   25 * time.Millisecond,
		}
		cfg.GRPCCfg.GRPCAddress = "127.0.0.1:65535"

		s.serverInfo = nil
		s.cfg = cfg
		s.state = nil
		s.openSessionCancel = nil
		s.subID = uuid.New()
		s.registrationCh = make(chan messages.ServerMsg, 1)
		s.openSessionErrCh = make(chan error, 1)
		s.queryErrByReqID = make(map[revent.RequestID]error)

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

	ctx.Step(`^I open the SDK session$`, s.iOpenTheSDKSession)
	ctx.Step(`^I cancel the SDK session context$`, s.iCancelTheSDKSessionContext)
	ctx.Step(`^I send a query request$`, s.iSendAQueryRequest)
	ctx.Step(`^the client should be registered by the server$`, s.theClientShouldBeRegisteredByTheServer)
	ctx.Step(`^the query "([^"]*)" should fail with QueryHandlerNotFound$`, s.theQueryShouldFailWithQueryHandlerNotFound)
	ctx.Step(`^the server is running$`, s.theServerIsRunning)
	ctx.Step(`^the server restarts$`, s.theServerRestarts)
	ctx.Step(`^the session should finish with context canceled$`, s.openSessionShouldFinishWithContextCanceled)
	ctx.Step(`^the session should fail with CantConnectToServerError$`, s.sessionShouldFailWithCantConnectToServerError)
}
