package actions

import (
	"context"

	"github.com/manuelarte/revent-sdk-go/internal/revent/messages"
	"github.com/manuelarte/revent-sdk-go/logger"
	"github.com/manuelarte/revent-sdk-go/revent"
)

type (
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

func (qh *QueryHandling) Do(ctx context.Context, qhp QueryHandlingParams) {
	// TODO: handle errors
	if qhp.Msg == nil || qh.getHandler == nil || qh.sender == nil {
		return
	}

	handler, ok := qh.getHandler(qhp.Msg.QueryID)
	if !ok {
		qh.logger.Error("no handler registered for query", "queryID", qhp.Msg.QueryID, "requestID", qhp.Msg.RequestID)

		return
	}

	responseBytes, err := handler(ctx, qhp.Msg.Parameters)
	if err != nil {
		qh.logger.Error("failed to handle query", "queryID", qhp.Msg.QueryID, "requestID", qhp.Msg.RequestID, "error", err)

		return
	}

	if errSend := qh.sender.Send(&messages.QueryResponseRawMsg{
		RequestID: qhp.Msg.RequestID,
		Response:  responseBytes,
	}); errSend != nil {
		qh.logger.Error("failed to send query response", "queryID", qhp.Msg.QueryID, "requestID", qhp.Msg.RequestID, "error", errSend)
	}
}
