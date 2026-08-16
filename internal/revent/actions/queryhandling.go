package actions

import (
	"context"

	"github.com/manuelarte/revent-sdk-go/logger"
	"github.com/manuelarte/revent-sdk-go/revent"
	"github.com/manuelarte/revent-sdk-go/revent/messages"
)

type (
	QueryHandling[I revent.QueryRequestParameters, O revent.QueryResponse] struct {
		logger logger.ILogger
		m      SendAndSubscribe
	}

	QueryHandlingParams struct {
		Msg messages.QueryRequestMsg
	}
)

// TODO: doubt if here I should just handle the query, or also launch the goroutine

func (q QueryHandling[I, O]) Do(ctx context.Context, params QueryHandlingParams) error {
	// TODO: get the handled from the manager, and call the function,
	// then send the output to the server.
	return nil
}
