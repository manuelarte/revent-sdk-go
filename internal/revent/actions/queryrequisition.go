package actions

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/manuelarte/revent-sdk-go/logger"
	"github.com/manuelarte/revent-sdk-go/revent"
)

type (
	// QueryRequisition is the flow to send a QueryRequest, wait, and forward the QueryResponse.
	QueryRequisition[I revent.QueryRequestParameters, O revent.QueryResponse] struct {
		logger logger.ILogger
		m      SendAndSubscribe
	}

	QueryRequisitionParams[I revent.QueryRequestParameters, O revent.QueryResponse] struct {
		RequestID revent.RequestID
		QueryID   revent.Query[I, O]
		Params    I
	}

	QueryRequestError struct {
		RequestID string
		Reason    string
	}
)

func NewQueryRequisition[I revent.QueryRequestParameters, O revent.QueryResponse](
	logger logger.ILogger,
	m SendAndSubscribe,
) *QueryRequisition[I, O] {
	return &QueryRequisition[I, O]{
		logger: logger,
		m:      m,
	}
}

func (e QueryRequestError) Error() string {
	return fmt.Sprintf("query request %q rejected: %q", e.RequestID, e.Reason)
}

func (c *QueryRequisition[I, O]) Do(ctx context.Context, params QueryRequisitionParams[I, O]) error {
	queryRequestEvents := make(chan revent.ServerMsg, 1)

	err := c.m.Subscribe(uuid.UUID(params.RequestID), func(msg revent.ServerMsg) bool {
		if msg == nil {
			return false
		}

		switch payload := msg.(type) {
		case *revent.QueryResponseRawMsg:
			return payload.RequestID.String() == params.RequestID.String()
		default:
			return false
		}
	}, queryRequestEvents)
	if err != nil {
		return fmt.Errorf("error creating query responsed listener: %w", err)
	}

	defer func() {
		_ = c.m.Unsubscribe(uuid.UUID(params.RequestID))
	}()

	err = c.m.Send(&revent.QueryRequestMsg{
		RequestID:  params.RequestID,
		QueryID:    revent.QueryID(params.QueryID),
		Parameters: nil,
	})
	if err != nil {
		return fmt.Errorf("error sending query request: %w", err)
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("error waiting for query response: %w", ctx.Err())
	case msg := <-queryRequestEvents:
		switch payload := msg.(type) {
		case *revent.QueryResponseRawMsg:
			c.logger.Info("Query responded successfully", "requestID", payload.RequestID)
			// TODO: rethink flow api
			return nil
		case *revent.QueryResponseErrorMsg:
			return payload
		default:
			return UnexpectedMsgError{
				Flow: "QueryRequisition",
				Msg:  payload,
			}
		}
	}
}
