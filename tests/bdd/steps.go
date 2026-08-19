//nolint:mnd // magic numbers related to timeouts.
package bdd

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cucumber/godog"
	"github.com/google/uuid"

	reventsdkgo "github.com/manuelarte/revent-sdk-go"
	"github.com/manuelarte/revent-sdk-go/internal/revent/actions"
	"github.com/manuelarte/revent-sdk-go/internal/revent/messages"
	"github.com/manuelarte/revent-sdk-go/internal/txrx"
	"github.com/manuelarte/revent-sdk-go/revent"
)

type (
	scenarioState struct {
		serverInfo *serverInfo
		cfg        reventsdkgo.Config
		state      *reventsdkgo.ServerManager
		// we subscribe to every single message to do the checks later on.
		subID              uuid.UUID
		registrationCh     chan messages.ServerMsg
		openSessionErrCh   chan error
		openSessionCancel  context.CancelFunc
		queryErrByReqID    map[revent.RequestID]error
		queryResultByReqID map[revent.RequestID]*bddQueryOutput
	}
)

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

func (s *scenarioState) iRegisterAHandlerForQuery(ctx context.Context, queryIDRaw string) (context.Context, error) {
	if s.state == nil {
		state, err := reventsdkgo.NewState(s.cfg)
		if err != nil {
			return ctx, fmt.Errorf("failed to create state: %w", err)
		}

		s.state = state
	}

	switch queryIDRaw {
	case string(testQuery):
		err := reventsdkgo.RegisterQueryHandler(
			s.state,
			testQuery,
			func(ctx context.Context, params *bddQueryInput) *bddQueryOutput {
				val := ""
				if params != nil {
					val = params.Value
				}

				return &bddQueryOutput{Result: "handled-" + val}
			},
		)
		if err != nil {
			return ctx, fmt.Errorf("failed to register query handler: %w", err)
		}
	case string(testInvalidResponseQueryServer):
		// This handler returns bddQueryOutput which marshals to {"Result":"..."}
		// but the client will try to unmarshal it as bddInvalidQueryOutput
		// which will fail with UnmarshalError
		err := reventsdkgo.RegisterQueryHandler(
			s.state,
			testInvalidResponseQueryServer,
			func(ctx context.Context, params *bddQueryInput) *bddQueryOutput {
				return &bddQueryOutput{Result: "this-will-fail-to-unmarshal"}
			},
		)
		if err != nil {
			return ctx, fmt.Errorf("failed to register query handler: %w", err)
		}
	default:
		return ctx, fmt.Errorf("unknown query id %q", queryIDRaw)
	}

	return ctx, nil
}

func (s *scenarioState) iRegisterAnErrorReturningHandlerForQuery(
	ctx context.Context,
	queryIDRaw string,
) (context.Context, error) {
	if s.state == nil {
		state, err := reventsdkgo.NewState(s.cfg)
		if err != nil {
			return ctx, fmt.Errorf("failed to create state: %w", err)
		}

		s.state = state
	}

	switch queryIDRaw {
	case string(testErrorQuery):
		err := reventsdkgo.RegisterQueryHandler(
			s.state,
			testErrorQuery,
			func(ctx context.Context, params *bddQueryInput) *bddErrorQueryOutput {
				// Return a response that will fail during marshaling
				return &bddErrorQueryOutput{shouldFail: true}
			},
		)
		if err != nil {
			return ctx, fmt.Errorf("failed to register query handler: %w", err)
		}
	default:
		return ctx, fmt.Errorf("unknown query id %q", queryIDRaw)
	}

	return ctx, nil
}

func (s *scenarioState) iOpenTheSDKSession(ctx context.Context) (context.Context, error) {
	if s.state == nil {
		state, err := reventsdkgo.NewState(s.cfg)
		if err != nil {
			return ctx, fmt.Errorf("failed to create state: %w", err)
		}

		s.state = state
	}

	s.state.Subscribe(s.subID, func(msg messages.ServerMsg) bool {
		return true
	}, s.registrationCh)

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
	timer := time.NewTimer(8 * time.Second)
	defer timer.Stop()

	for {
		select {
		case msg := <-s.registrationCh:
			registered, ok := msg.(*messages.ClientRegisteredMsg)
			if !ok {
				continue
			}

			if registered.ClientID.String() != s.cfg.ClientID.String() {
				return ctx, fmt.Errorf(
					"unexpected client id: got %q want %q",
					registered.ClientID.String(),
					s.cfg.ClientID.String(),
				)
			}

			return ctx, nil
		case <-timer.C:
			return ctx, errors.New("timeout waiting for registration confirmation")
		}
	}
}

func (s *scenarioState) iSendAQueryRequest(ctx context.Context, table *godog.Table) (context.Context, error) {
	if s.state == nil {
		return ctx, errors.New("state is nil")
	}

	if table == nil {
		return ctx, errors.New("query request table is nil")
	}

	var requestIDRaw, queryIDRaw string

	for _, row := range table.Rows {
		if len(row.Cells) < 2 {
			continue
		}

		switch strings.TrimSpace(row.Cells[0].Value) {
		case "RequestId":
			requestIDRaw = strings.TrimSpace(row.Cells[1].Value)
		case "Query":
			queryIDRaw = strings.TrimSpace(row.Cells[1].Value)
		}
	}

	if requestIDRaw == "" {
		return ctx, errors.New("field RequestId is required")
	}

	if queryIDRaw == "" {
		return ctx, errors.New("field Query is required")
	}

	requestUUID, err := uuid.Parse(requestIDRaw)
	if err != nil {
		return ctx, fmt.Errorf("invalid RequestId %q: %w", requestIDRaw, err)
	}

	requestID := revent.RequestID(requestUUID)

	requestCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	// Handle different query types based on queryIDRaw
	switch queryIDRaw {
	case string(testInvalidResponseQueryClient):
		// For invalid response query, we expect unmarshal error on client
		queryID := testInvalidResponseQueryClient
		output, err := reventsdkgo.QueryRequest(requestCtx, s.state, requestID, queryID, &bddQueryInput{Value: "any"})

		s.queryErrByReqID[requestID] = err
		if output != nil {
			// Convert to bddQueryOutput for storage (won't happen in error case)
			s.queryResultByReqID[requestID] = &bddQueryOutput{}
		}
	case string(testErrorQuery):
		// For error query, handler returns output that fails marshaling
		queryID := testErrorQuery
		output, errQueryRequest := reventsdkgo.QueryRequest(requestCtx, s.state, requestID, queryID, &bddQueryInput{Value: "any"})

		s.queryErrByReqID[requestID] = errQueryRequest
		if output != nil {
			s.queryResultByReqID[requestID] = &bddQueryOutput{}
		}
	default:
		// For standard test query
		queryID := revent.Query[*bddQueryInput, *bddQueryOutput](queryIDRaw)
		output, errQueryRequest := reventsdkgo.QueryRequest(requestCtx, s.state, requestID, queryID, &bddQueryInput{Value: "any"})

		s.queryErrByReqID[requestID] = errQueryRequest
		if output != nil {
			s.queryResultByReqID[requestID] = output
		}
	}

	return ctx, nil
}

func (s *scenarioState) iSendAQueryRequestWithTheSameRequestID(
	ctx context.Context,
	table *godog.Table,
) (context.Context, error) {
	if s.state == nil {
		return ctx, errors.New("state is nil")
	}

	if table == nil {
		return ctx, errors.New("query request table is nil")
	}

	var requestIDRaw, queryIDRaw string

	for _, row := range table.Rows {
		if len(row.Cells) < 2 {
			continue
		}

		switch strings.TrimSpace(row.Cells[0].Value) {
		case "RequestId":
			requestIDRaw = strings.TrimSpace(row.Cells[1].Value)
		case "Query":
			queryIDRaw = strings.TrimSpace(row.Cells[1].Value)
		}
	}

	if requestIDRaw == "" {
		return ctx, errors.New("field RequestId is required")
	}

	if queryIDRaw == "" {
		return ctx, errors.New("field Query is required")
	}

	requestUUID, err := uuid.Parse(requestIDRaw)
	if err != nil {
		return ctx, fmt.Errorf("invalid RequestId %q: %w", requestIDRaw, err)
	}

	requestID := revent.RequestID(requestUUID)

	requestCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	// Handle different query types
	switch queryIDRaw {
	case string(testInvalidResponseQueryClient):
		queryID := testInvalidResponseQueryClient
		output, errQueryRequest := reventsdkgo.QueryRequest(requestCtx, s.state, requestID, queryID, &bddQueryInput{Value: "any"})

		s.queryErrByReqID[requestID] = errQueryRequest
		if output != nil {
			s.queryResultByReqID[requestID] = &bddQueryOutput{}
		}
	case string(testErrorQuery):
		queryID := testErrorQuery
		output, errQueryRequest := reventsdkgo.QueryRequest(requestCtx, s.state, requestID, queryID, &bddQueryInput{Value: "any"})

		s.queryErrByReqID[requestID] = errQueryRequest
		if output != nil {
			s.queryResultByReqID[requestID] = &bddQueryOutput{}
		}
	default:
		queryID := revent.Query[*bddQueryInput, *bddQueryOutput](queryIDRaw)
		output, errQueryRequest := reventsdkgo.QueryRequest(requestCtx, s.state, requestID, queryID, &bddQueryInput{Value: "any"})

		s.queryErrByReqID[requestID] = errQueryRequest
		if output != nil {
			s.queryResultByReqID[requestID] = output
		}
	}

	return ctx, nil
}

func (s *scenarioState) theQueryShouldSucceedWithResult(
	ctx context.Context,
	requestIDRaw, expectedResult string,
) (context.Context, error) {
	requestUUID, err := uuid.Parse(requestIDRaw)
	if err != nil {
		return ctx, fmt.Errorf("invalid RequestId %q: %w", requestIDRaw, err)
	}

	requestID := revent.RequestID(requestUUID)

	if queryErr := s.queryErrByReqID[requestID]; queryErr != nil {
		return ctx, fmt.Errorf("expected query to succeed, got error: %w", queryErr)
	}

	res, ok := s.queryResultByReqID[requestID]
	if !ok || res == nil {
		return ctx, fmt.Errorf("query result for request %q was not captured", requestIDRaw)
	}

	if res.Result != expectedResult {
		return ctx, fmt.Errorf("expected query result %q, got %q", expectedResult, res.Result)
	}

	return ctx, nil
}

func (s *scenarioState) theQueryShouldFailWithQueryHandlerNotFound(
	ctx context.Context,
	requestIDRaw string,
) (context.Context, error) {
	requestUUID, err := uuid.Parse(requestIDRaw)
	if err != nil {
		return ctx, fmt.Errorf("invalid RequestId %q: %w", requestIDRaw, err)
	}

	requestID := revent.RequestID(requestUUID)

	queryErr, ok := s.queryErrByReqID[requestID]
	if !ok {
		return ctx, fmt.Errorf("query result for request %q was not captured", requestIDRaw)
	}

	if queryErr == nil {
		return ctx, errors.New("expected query to fail, got nil")
	}

	var requestedErr *actions.QueryRequestedError
	if !errors.As(queryErr, &requestedErr) {
		return ctx, fmt.Errorf("expected QueryRequestedError, got: %w", queryErr)
	}

	if requestedErr.Reason != messages.QueryRequestedErrorReasonQueryHandlerNotFound {
		return ctx, fmt.Errorf(
			"expected query error reason %q, got %q",
			messages.QueryRequestedErrorReasonQueryHandlerNotFound,
			requestedErr.Reason,
		)
	}

	return ctx, nil
}

func (s *scenarioState) theQueryShouldFailWithUnmarshalError(
	ctx context.Context,
	requestIDRaw string,
) (context.Context, error) {
	requestUUID, err := uuid.Parse(requestIDRaw)
	if err != nil {
		return ctx, fmt.Errorf("invalid RequestId %q: %w", requestIDRaw, err)
	}

	requestID := revent.RequestID(requestUUID)

	queryErr, ok := s.queryErrByReqID[requestID]
	if !ok {
		return ctx, fmt.Errorf("query result for request %q was not captured", requestIDRaw)
	}

	if queryErr == nil {
		return ctx, errors.New("expected query to fail, got nil")
	}

	var requestedErr *actions.QueryRequestedError
	if !errors.As(queryErr, &requestedErr) {
		return ctx, fmt.Errorf("expected QueryRequestedError, got: %w", queryErr)
	}

	if requestedErr.Reason != messages.QueryRequestedErrorReasonErrorHandling {
		return ctx, fmt.Errorf(
			"expected query error reason %q, got %q",
			messages.QueryRequestedErrorReasonErrorHandling,
			requestedErr.Reason,
		)
	}

	if requestedErr.Details == "" {
		return ctx, errors.New("expected unmarshal error to have details")
	}

	return ctx, nil
}

func (s *scenarioState) theQueryShouldFailWithRequestIDDuplicated(
	ctx context.Context,
	requestIDRaw string,
) (context.Context, error) {
	requestUUID, err := uuid.Parse(requestIDRaw)
	if err != nil {
		return ctx, fmt.Errorf("invalid RequestId %q: %w", requestIDRaw, err)
	}

	requestID := revent.RequestID(requestUUID)

	queryErr, ok := s.queryErrByReqID[requestID]
	if !ok {
		return ctx, fmt.Errorf("query result for request %q was not captured", requestIDRaw)
	}

	if queryErr == nil {
		return ctx, errors.New("expected query to fail, got nil")
	}

	var requestedErr *actions.QueryRequestedError
	if !errors.As(queryErr, &requestedErr) {
		return ctx, fmt.Errorf("expected QueryRequestedError, got: %w", queryErr)
	}

	if requestedErr.Reason != messages.QueryRequestedErrorReasonRequestIDDuplicated {
		return ctx, fmt.Errorf(
			"expected query error reason %q, got %q",
			messages.QueryRequestedErrorReasonRequestIDDuplicated,
			requestedErr.Reason,
		)
	}

	return ctx, nil
}

func (s *scenarioState) theQueryShouldFailWithQueryHandlingError(
	ctx context.Context,
	requestIDRaw string,
) (context.Context, error) {
	requestUUID, err := uuid.Parse(requestIDRaw)
	if err != nil {
		return ctx, fmt.Errorf("invalid RequestId %q: %w", requestIDRaw, err)
	}

	requestID := revent.RequestID(requestUUID)

	queryErr, ok := s.queryErrByReqID[requestID]
	if !ok {
		return ctx, fmt.Errorf("query result for request %q was not captured", requestIDRaw)
	}

	if queryErr == nil {
		return ctx, errors.New("expected query to fail, got nil")
	}

	// QueryHandlingError can come as either a QueryRequestedError with a specific reason
	// or as a generic error if the server returns it in a different way
	var requestedErr *actions.QueryRequestedError
	if errors.As(queryErr, &requestedErr) {
		// If it's a QueryRequestedError, check for appropriate reasons
		if requestedErr.Reason != messages.QueryRequestedErrorReasonErrorHandling &&
			requestedErr.Reason != messages.QueryRequestedErrorReasonQueryTimedOut {
			return ctx, fmt.Errorf(
				"expected query error reason to indicate handling failure or timeout, got %q",
				requestedErr.Reason,
			)
		}

		return ctx, nil
	}

	// Otherwise, just verify there's an error
	if queryErr.Error() == "" {
		return ctx, errors.New("expected query to have an error message")
	}

	return ctx, nil
}
