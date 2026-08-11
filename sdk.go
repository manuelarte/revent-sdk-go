package revent_sdk_go

import (
	"context"
	"fmt"

	"github.com/manuelarte/revent-sdk-go/internal/flow"
	"github.com/manuelarte/revent-sdk-go/internal/txrx"
	"github.com/manuelarte/revent-sdk-go/revent"
)

// OpenSession starts a session and can be called only once per State instance.
func OpenSession(ctx context.Context, s *State) error {
	var err error

	s.once.Do(func() {
		createTxRxFn := func() (TxRx, <-chan error, error) {
			onConnected := func() {
				errReg := flow.NewClientRegistration(s.logger, s.cfg.ClientID.String()).Do(ctx, s)
				if errReg != nil {
					s.logger.Error("Failed to register client", "error", errReg)
				}
			}
			return txrx.NewGRPCTxRx(ctx, s.logger, s.cfg.GRPCCfg, onConnected, s)
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

func QueryRequest[I revent.QueryRequestParameters, O revent.QueryResponse](ctx context.Context, s *State, query revent.Query[I, O], params I) (O, error) {
	queryRequestFlow := flow.NewQueryRequest(s.logger, query)
	err := queryRequestFlow.Do(ctx, s)
	if err != nil {
		var zero O
		return zero, fmt.Errorf("error sending query request: %w", err)
	}

	var zero O
	return zero, nil
}
