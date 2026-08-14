package revent_sdk_go

import (
	"context"
	"fmt"

	"github.com/manuelarte/revent-sdk-go/internal/flows"
	"github.com/manuelarte/revent-sdk-go/internal/txrx"
	"github.com/manuelarte/revent-sdk-go/revent"
)

// OpenSession starts a session and can be called only once per State instance.
func OpenSession(ctx context.Context, s *State, cfg txrx.GrpcConfig) error {
	var err error

	s.once.Do(func() {
		createTxRxFn := func() (TxRx, <-chan error, error) {
			return txrx.NewGRPCTxRx(ctx, s.logger, cfg, s)
		}
		err = s.start(ctx, createTxRxFn)
	})

	if err != nil {
		return err
	}

	return nil
}

// RegisterQueryHandler registers a query handler for a specific query ID.
// It ensures that only one handler is registered for each query ID and returns an error
// if a handler already exists for the given query ID.
func RegisterQueryHandler[
	I revent.QueryRequestParameters,
	O revent.QueryResponse,
](
	s *State,
	query revent.Query[I, O],
	qh revent.QueryHandlerFunc[I, O],
) error {
	queryID := revent.QueryID(query)

	s.muQueryHandlers.Lock()
	defer s.muQueryHandlers.Unlock()

	if _, ok := s.queryHandlers[queryID]; ok {
		return revent.QueryHandlerAlreadyRegisteredError{
			QueryID: queryID,
		}
	}

	s.queryHandlers[queryID] = qh

	return nil
}

func RegisterSourceEventHandler[E revent.Event, S revent.SourceEvent[E]](eh revent.SourceEventHandler[E, S]) error {
	// TODO: here we need the Client struct and add the event handler for that event id.
	return nil
}

func QueryRequest[I revent.QueryRequestParameters, O revent.QueryResponse](
	ctx context.Context,
	s *State,
	requestID revent.RequestID,
	query revent.Query[I, O],
	params I,
) (O, error) {
	queryRequisition := flows.NewQueryRequisition(s.logger, requestID, query)

	type sendAndSubscribe struct {
		*State
		TxRx
	}

	x := sendAndSubscribe{s, s.txRx}

	err := queryRequisition.Do(ctx, x)
	if err != nil {
		var zero O

		return zero, fmt.Errorf("error sending query request: %w", err)
	}

	var zero O

	return zero, nil
}
