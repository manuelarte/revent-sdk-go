package actions

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"github.com/manuelarte/revent-sdk-go/internal/revent/messages"
	"github.com/manuelarte/revent-sdk-go/revent"
)

type (
	// QueryRequisition is the flow to send a QueryRequest, wait, and forward the QueryResponse.
	QueryRequisition[I revent.QueryRequestParameters, O revent.QueryResponse] struct {
		m SendAndSubscribe
	}

	QueryRequisitionParams[I revent.QueryRequestParameters, O revent.QueryResponse] struct {
		RequestID revent.RequestID
		QueryID   revent.Query[I, O]
		Params    I
	}

	// QueryResponse is the response to a QueryRequest, built based on messages.QueryResponseRawMsg.
	QueryResponse[O revent.QueryResponse] struct {
		RequestID revent.RequestID
		Response  O
	}

	// QueryRequestedError is the error response to a QueryRequest,
	// built based on messages.QueryRequestedErrorRawMsg.
	QueryRequestedError struct {
		RequestID revent.RequestID
		QueryID   revent.QueryID
		Reason    messages.QueryRequestedErrorReason
		Details   string
	}

	// QueryRequestResponse is the response to a QueryRequest.
	// It can be either a QueryResponse or a QueryRequestedErrorMsg.
	// Where Msg is the response to the QueryRequest, and Err is the message
	// containing the error why the query could not be processed.
	QueryRequestResponse[O revent.QueryResponse] struct {
		Msg *QueryResponse[O]
		Err *QueryRequestedError
	}
)

func NewQueryRequisition[I revent.QueryRequestParameters, O revent.QueryResponse](
	m SendAndSubscribe,
) *QueryRequisition[I, O] {
	return &QueryRequisition[I, O]{
		m: m,
	}
}

// Do send a QueryRequest, waits for the QueryResponse, and returns it.
// Output:
// It returns the output of the query response, that it could be:
// - revent.QueryResponse
// - revent.QueryRequestedErrorMsg
// Errors:
// - error coming from trying to send the QueryRequest.
// - context error: if the context is canceled.
// - UnexpectedMsgError: if the received message is not expected.
func (c *QueryRequisition[I, O]) Do(
	ctx context.Context,
	params QueryRequisitionParams[I, O],
) (*QueryRequestResponse[O], error) {
	queryRequestEvents := make(chan messages.ServerMsg, 1)

	c.m.Subscribe(uuid.UUID(params.RequestID), func(msg messages.ServerMsg) bool {
		if msg == nil {
			return false
		}

		switch msg.(type) {
		case *messages.QueryResponseRawMsg, *messages.QueryRequestedErrorRawMsg:
			if identifiable, ok := msg.(messages.IdempotentMsg); ok {
				return identifiable.GetRequestID().String() == params.RequestID.String()
			}
		}

		return false
	}, queryRequestEvents)

	defer func() {
		_ = c.m.Unsubscribe(uuid.UUID(params.RequestID))
	}()

	err := c.m.Send(&messages.QueryRequestMsg{
		RequestID:  params.RequestID,
		QueryID:    revent.QueryID(params.QueryID),
		Parameters: params.Params,
	})
	if err != nil {
		return nil, fmt.Errorf("error sending query request: %w", err)
	}

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("error waiting for query response: %w", ctx.Err())
	case msg := <-queryRequestEvents:
		switch payload := msg.(type) {
		case *messages.QueryResponseRawMsg:
			var zero O

			errUnmarshal := json.Unmarshal(payload.Response, &zero)
			if errUnmarshal != nil {
				return &QueryRequestResponse[O]{
					Err: &QueryRequestedError{
						RequestID: payload.RequestID,
						QueryID:   revent.QueryID(params.QueryID),
						Reason:    messages.QueryRequestedErrorReasonErrorHandling,
						Details:   errUnmarshal.Error(),
					},
				}, nil
			}

			return &QueryRequestResponse[O]{
				Msg: &QueryResponse[O]{
					RequestID: payload.RequestID,
					Response:  zero,
				},
			}, nil
		case *messages.QueryRequestedErrorRawMsg:
			return &QueryRequestResponse[O]{
				Err: &QueryRequestedError{
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

func (q QueryRequestResponse[O]) GetRequestID() revent.RequestID {
	if q.Msg != nil {
		return q.Msg.RequestID
	}

	if q.Err != nil {
		return q.Err.RequestID
	}

	return revent.RequestID(uuid.Nil)
}

func (e QueryRequestedError) Error() string {
	return fmt.Sprintf("query %q response error: %q", e.QueryID, e.Reason)
}
