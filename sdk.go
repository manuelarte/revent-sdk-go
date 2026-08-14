package revent_sdk_go

import (
	"context"
	"fmt"
	"time"

	"github.com/manuelarte/revent-sdk-go/internal/revent/actions"
	"github.com/manuelarte/revent-sdk-go/internal/txrx"
	"github.com/manuelarte/revent-sdk-go/revent"
)

// OpenSession starts a session and can be called only once per ServerManager instance.
func OpenSession(ctx context.Context, s *ServerManager, cfg txrx.GrpcConfig) error {
	var err error

	s.once.Do(func() {
		createTxRxFn := func() (txrx.TxRx, error) {
			return txrx.NewGRPCTxRx(ctx, s.logger, cfg, registerClient(s))
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
	s *ServerManager,
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
	s *ServerManager,
	requestID revent.RequestID,
	query revent.Query[I, O],
	params I,
) (O, error) {
	x := struct {
		txrx.TxRx
		*ServerManager
	}{
		s.txRx,
		s,
	}
	qr := actions.NewQueryRequisition[I, O](s.logger, x)

	output, err := qr.Do(ctx, actions.QueryRequisitionParams[I, O]{
		RequestID: requestID,
		QueryID:   query,
		Params:    params,
	})
	if err != nil {
		var zero O

		return zero, fmt.Errorf("error sending query request: %w", err)
	}

	if output.Err != nil {
		var zero O

		return zero, output.Err
	}

	return output.Msg.Response, nil
}

func registerClient(s *ServerManager) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		x := struct {
			txrx.TxRx
			*ServerManager
		}{
			s.txRx,
			s,
		}
		cr := actions.NewClientRegistration(s.logger, x)

		waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		output, errReg := cr.Do(waitCtx, actions.ClientRegistrationParams{
			ClientID:      s.clientID,
			QueryHandlers: s.getQueryHandlerIDs(),
		})
		if errReg != nil {
			return fmt.Errorf("failed to register client: %w", errReg)
		}

		if output.Err != nil {
			return fmt.Errorf("failed to register client: %w", output.Err)
		}

		s.logger.Info("Client registered successfully", "clientId", output.Msg.ClientID)

		return nil
	}
}
