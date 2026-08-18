package actions

import (
	"context"
	"fmt"

	"github.com/manuelarte/revent-sdk-go/internal/revent/messages"
	"github.com/manuelarte/revent-sdk-go/logger"
	"github.com/manuelarte/revent-sdk-go/revent"
)

type (
	UnexpectedError struct {
		Reason string
	}

	QueryHandler func(ctx context.Context, params map[string]string) ([]byte, error)

	QueryHandling struct {
		logger     logger.ILogger
		sender     Sender
		getHandler func(queryID revent.QueryID) (QueryHandler, bool)
	}

	QueryHandlingParams struct {
		Msg *messages.QueryRequestedMsg
	}
)

func (e UnexpectedError) Error() string {
	return fmt.Sprintf("unexpected error: %s", e.Reason)
}

func NewQueryHandling(
	logger logger.ILogger,
	sender Sender,
	getHandler func(queryID revent.QueryID) (QueryHandler, bool),
) *QueryHandling {
	return &QueryHandling{
		logger:     logger,
		sender:     sender,
		getHandler: getHandler,
	}
}

func (qh *QueryHandling) Do(ctx context.Context, qhp QueryHandlingParams) error {
	if qhp.Msg == nil || qh.getHandler == nil || qh.sender == nil {
		return UnexpectedError{
			Reason: "nil message, handler, or sender",
		}
	}

	handler, ok := qh.getHandler(qhp.Msg.QueryID)
	if !ok {
		// TODO: inform the client
		return UnexpectedError{
			Reason: fmt.Sprintf("no handler registered for query: queryID=%s, requestID=%s", qhp.Msg.QueryID, qhp.Msg.RequestID),
		}
	}

	responseBytes, err := handler(ctx, qhp.Msg.Parameters)
	if err != nil {
		qh.logger.Error("failed to handle query", "queryID", qhp.Msg.QueryID, "requestID", qhp.Msg.RequestID, "error", err)

		// TODO: inform the client. This is missing in R-Event, it's not able to handle Client sending
		// an error response.
		// qh.sender.Send(&messages.QueryRequestedErrorRawMsg{
		//	RequestID: qhp.Msg.RequestID,
		//	Reason:    ,
		// })
		return UnexpectedError{
			Reason: fmt.Sprintf(
				"failed to handle query: queryID=%s, requestID=%s, error=%v",
				qhp.Msg.QueryID,
				qhp.Msg.RequestID,
				err,
			),
		}
	}

	if errSend := qh.sender.Send(&messages.QueryResponseRawMsg{
		RequestID: qhp.Msg.RequestID,
		Response:  responseBytes,
	}); errSend != nil {
		return fmt.Errorf("failed to send query response: %w", errSend)
	}

	return nil
}
