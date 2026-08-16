package actions

import (
	"context"

	"github.com/manuelarte/revent-sdk-go/logger"
	"github.com/manuelarte/revent-sdk-go/revent"
)

type (
	QueryHandling[I revent.QueryRequestParameters, O revent.QueryResponse] struct {
		logger logger.ILogger
		m      SendAndSubscribe
	}

	QueryHandlingParams struct {
		ClientID      revent.ClientID
		QueryHandlers []revent.QueryID
	}
)

// TODO: doubt if here I should just handle the query, or also launch the goroutine

func (q QueryHandling[I, O]) Do(ctx context.Context, params QueryHandlingParams) (O, error) {
	var zero O
	return zero, nil
}
