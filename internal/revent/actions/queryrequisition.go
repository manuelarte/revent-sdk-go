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

	QueryRequisitionResponse[O revent.QueryResponse] struct {
		Response O
		Err      error
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

func (c *QueryRequisition[I, O]) Do(ctx context.Context, params QueryRequisitionParams[I, O]) (*QueryRequisitionResponse[O], error) {
	queryRequestEvents := make(chan revent.ServerMsg, 1)

	err := c.m.Subscribe(uuid.UUID(params.RequestID), func(msg revent.ServerMsg) bool {
		if msg == nil {
			return false
		}

		if identificable, ok := msg.(revent.IdempotentMsg); ok {
			return identificable.GetRequestID().String() == params.RequestID.String()
		}
		return false
	}, queryRequestEvents)
	if err != nil {
		return nil, fmt.Errorf("error creating query responsed listener: %w", err)
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
		return nil, fmt.Errorf("error sending query request: %w", err)
	}

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("error waiting for query response: %w", ctx.Err())
	case msg := <-queryRequestEvents:
		switch payload := msg.(type) {
		case *revent.QueryResponseRawMsg:
			return &QueryRequisitionResponse[O]{
				Response: payload.Response,
			}, nil
		case *revent.QueryResponseErrorRawMsg:
			return &QueryRequisitionResponse[O]{
				Err: &revent.QueryResponseErrorMsg{
					RequestID: payload.RequestID,
					QueryID:   revent.QueryID(params.QueryID),
					Reason:    payload.Reason,
				},
			}, nil
		default:
			return nil, UnexpectedMsgError{
				Flow: "QueryRequisition",
				Msg:  payload,
			}
		}
	}
}
