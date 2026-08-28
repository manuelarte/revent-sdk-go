package actions

import (
	"context"
	"fmt"

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

	QueryHandlingResponse struct {
		Msg *messages.QueryResponseRawMsg
		Err *messages.QueryHandlingFailedMsg
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

func (qh *QueryHandling) Do(ctx context.Context, qhp QueryHandlingParams) (*QueryHandlingResponse, error) {
	if qhp.Msg == nil || qh.getHandler == nil || qh.sender == nil {
		msg := &messages.QueryHandlingFailedMsg{
			RequestID: qhp.Msg.RequestID,
			Reason:    "Unknown",
			Details:   "nil message, handler, or sender",
		}

		err := qh.sender.Send(msg)
		if err != nil {
			return nil, fmt.Errorf("failed to send query handling error: %w", err)
		}

		return &QueryHandlingResponse{
			Err: msg,
		}, nil
	}

	handler, ok := qh.getHandler(qhp.Msg.QueryID)
	if !ok {
		msg := &messages.QueryHandlingFailedMsg{
			RequestID: qhp.Msg.RequestID,
			Reason:    "Unknown",
			Details:   "handler not found",
		}

		err := qh.sender.Send(msg)
		if err != nil {
			return nil, fmt.Errorf("failed to send query handling error: %w", err)
		}

		return &QueryHandlingResponse{
			Err: msg,
		}, nil
	}

	responseBytes, err := handler(ctx, qhp.Msg.Parameters)
	if err != nil {
		msg := &messages.QueryHandlingFailedMsg{
			RequestID: qhp.Msg.RequestID,
			Reason:    string(messages.QueryRequestedFailedReasonErrorHandling),
			Details:   err.Error(),
		}

		errSending := qh.sender.Send(msg)
		if errSending != nil {
			return nil, fmt.Errorf("failed to send query handling error: %w", errSending)
		}

		return &QueryHandlingResponse{
			Err: msg,
		}, nil
	}

	msg := &messages.QueryResponseRawMsg{
		RequestID: qhp.Msg.RequestID,
		Response:  responseBytes,
	}
	if errSend := qh.sender.Send(msg); errSend != nil {
		return nil, fmt.Errorf("failed to send query response: %w", errSend)
	}

	return &QueryHandlingResponse{
		Msg: msg,
	}, nil
}
