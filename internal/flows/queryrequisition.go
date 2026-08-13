package flows

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/manuelarte/revent-sdk-go/logger"
	"github.com/manuelarte/revent-sdk-go/revent"
)

type (
	// QueryRequisition is the flow to send a QueryRequest, wait, and forward the QueryResponse.
	QueryRequisition[I revent.QueryRequestParameters, O revent.QueryResponse] struct {
		logger  logger.ILogger
		queryID revent.Query[I, O]
		timeout time.Duration
	}

	QueryRequestError struct {
		RequestID string
		Reason    string
	}
)

func NewQueryRequisition[I revent.QueryRequestParameters, O revent.QueryResponse](
	logger logger.ILogger,
	query revent.Query[I, O],
) *QueryRequisition[I, O] {
	return &QueryRequisition[I, O]{
		logger:  logger,
		queryID: query,
		timeout: 2 * time.Second,
	}
}

func (e QueryRequestError) Error() string {
	return fmt.Sprintf("query request %q rejected: %q", e.RequestID, e.Reason)
}

func (c *QueryRequisition[I, O]) Do(ctx context.Context, m SendAndSubscribe) error {
	subscriptionID := uuid.New()
	queryRequestEvents := make(chan revent.ServerMsg, 1)

	err := m.Subscribe(subscriptionID, func(msg revent.ServerMsg) bool {
		if msg == nil {
			return false
		}

		switch payload := msg.(type) {
		case *revent.QueryResponseMsg:
			return payload.RequestID.String() == subscriptionID.String()
		default:
			return false
		}
	}, queryRequestEvents)
	if err != nil {
		return fmt.Errorf("error creating query responsed listener: %w", err)
	}

	defer func() {
		_ = m.Unsubscribe(subscriptionID)
	}()

	err = m.Send(&revent.QueryRequestMsg{
		RequestID:  revent.RequestID(subscriptionID),
		QueryID:    "",
		Parameters: nil,
	})
	if err != nil {
		return fmt.Errorf("error sending query request: %w", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	select {
	case <-waitCtx.Done():
		if errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("query request timeout after %s: %w", c.timeout, waitCtx.Err())
		}

		return fmt.Errorf("error waiting for query response: %w", waitCtx.Err())
	case msg := <-queryRequestEvents:
		switch payload := msg.(type) {
		case *revent.QueryResponseMsg:
			c.logger.Info("Query responded successfully", "requestID", payload.RequestID)
			// TODO: rethink flow api
			return nil
		case *revent.QueryResponseErrorMsg:
			return payload
		default:
			return UnexpectedMsgError{
				Flow: "ClientRegistration",
				Msg:  payload,
			}
		}
	}
}
