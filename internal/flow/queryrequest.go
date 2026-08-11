package flow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/manuelarte/revent-sdk-go/internal"
	reventv1 "github.com/manuelarte/revent-sdk-go/internal/api/gRPC/revent/v1"
	"github.com/manuelarte/revent-sdk-go/logger"
	"github.com/manuelarte/revent-sdk-go/revent"
)

// QueryRequest is the flow to send a QueryRequest and wait for the QueryResponse.
type QueryRequest[I revent.QueryRequestParameters, O revent.QueryResponse] struct {
	logger  logger.ILogger
	queryID revent.Query[I, O]
	timeout time.Duration
}

func NewQueryRequest[I revent.QueryRequestParameters, O revent.QueryResponse](
	logger logger.ILogger,
	query revent.Query[I, O],
) *QueryRequest[I, O] {
	return &QueryRequest[I, O]{
		logger:  logger,
		queryID: query,
		timeout: 2 * time.Second,
	}
}

type QueryRequestError struct {
	RequestID string
	Reason    string
}

func (e QueryRequestError) Error() string {
	return fmt.Sprintf("query request %q rejected: %q", e.RequestID, e.Reason)
}

func (c *QueryRequest[I, O]) Do(ctx context.Context, m internal.Manager) error {
	subscriptionID := uuid.New()
	queryRequestEvents := make(chan *reventv1.ServerToClientMessage, 1)

	err := m.Subscribe(subscriptionID, func(msg *reventv1.ServerToClientMessage) bool {
		if msg == nil {
			return false
		}

		switch payload := msg.GetPayload().(type) {
		case *reventv1.ServerToClientMessage_QueryResponded:
			return payload.QueryResponded.RequestId == subscriptionID.String()
		case *reventv1.ServerToClientMessage_QueryRequestedError:
			return payload.QueryRequestedError.RequestId == subscriptionID.String()
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

	err = m.QueryRequest(revent.RequestID(subscriptionID), revent.QueryID(c.queryID))
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
		switch payload := msg.GetPayload().(type) {
		case *reventv1.ServerToClientMessage_QueryResponded:
			c.logger.Info("Query responded successfully", "requestID", payload.QueryResponded.GetRequestId())

			return nil
		case *reventv1.ServerToClientMessage_QueryRequestedError:
			return fmt.Errorf("error processing query request: %w", QueryRequestError{
				RequestID: payload.QueryRequestedError.GetRequestId(),
				Reason:    payload.QueryRequestedError.GetReason(),
			})
		default:
			return fmt.Errorf("unexpected query response message: %T", msg.GetPayload())
		}
	}
}
